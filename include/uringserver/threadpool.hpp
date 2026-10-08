#pragma once
#include "log.hpp"
#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <format>
#include <functional>
#include <future>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

//业务线程池默认参数
constexpr size_t THREADPOOL_DEFAULT_MAX_THREADS=100;                //弹性模式下允许的最大线程数
constexpr size_t THREADPOOL_DEFAULT_MAX_QUEUE=1024;                 //任务队列长度上限
//时间用std::chrono表达:调用点写60s/1s一眼可见,不会把"秒"当成"毫秒"传进来
constexpr std::chrono::seconds THREADPOOL_DEFAULT_IDLE_TIMEOUT{60}; //弹性模式下线程空闲多久被回收
constexpr std::chrono::seconds THREADPOOL_DEFAULT_SUBMIT_WAIT{1};   //队列满时提交任务的最长等待

//业务线程池
//定位:把"解析请求/查库/拼装响应"这类阻塞或耗时的逻辑搬出io线程,worker只做计算,不碰io;算完之后用Connection::send()(线程安全)或send_on_ring()回到io线程发送。
//三条使用约定:
//  1. 任务参数按值拷贝或移动进任务(与std::async一致);要传引用请用std::ref
//  2. 不要在线程池自己的任务里销毁线程池对象
//  3. 不要在线程池的任务里"阻塞等待另一个任务的future"(例如submit_task(...).get())。
//     固定(Fixed)模式下所有worker都被阻塞时,那个future永远不会被执行 -> 整个池子连同析构一起永久卡死。
//     需要嵌套就改用Elastic(会扩容)或者用try_submit_task()+超时轮询,或者把嵌套逻辑拆成两个独立任务
//  4. 本池不保证任务之间的执行顺序,也不保证"同一个对象上的任务串行执行"(没有Asio strand那样的机制):
//     N个worker会并发取任务,先提交的任务完全可能后执行、甚至与后一个任务同时在跑。
//     需要串行就自己加锁/加队列,或者在网络库里用Connection::post()/send_on_ring()把逻辑投回该连接所属的io线程
class ThreadPool
{
public:
    //线程数量策略
    enum class Sizing
    {
        Fixed,      //固定:start()之后线程数不再变化
        Elastic,    //弹性:待处理任务多于空闲线程就补线程,空闲超过上限就回收额外线程(保底线程不回收)
    };
    //运行状态
    enum class State
    {
        NotStarted, //还没启动
        Running,    //正在运行
        Stopping,   //优雅停止中:队列里剩下的任务执行完再退出
        Stopped,    //已完全停止
    };
private:
    //packaged_task是move-only,所以包一层shared_ptr再存进std::function;
    //捕获单个shared_ptr的lambda正好16字节,能放进std::function的小对象缓冲,不会产生第二次堆分配:每个任务仍然只有一次分配
    using Task=std::function<void()>;
    //一个worker线程
    //线程句柄必须由线程池持有:否则线程池可能在worker还在访问自己成员时就被析构
    struct Worker
    {
        std::thread      _handle;
        std::atomic_bool _exited{false};//线程函数已经跑完(句柄可以join并回收)
        std::atomic_bool _abandoned{false};//池子已经不再等待这个线程(在worker里调stop()的场景),它退出时不能再访问池子成员
        bool             _extra{false};//弹性线程:空闲超时后允许退出。保底线程永远是false
        size_t           _id{0};
    };
    //同步原语与任务队列
    mutable std::mutex      _mutex;//保护下面全部状态
    std::condition_variable _cond_work;//队列非空/状态变化
    std::condition_variable _cond_task;//队列不满(可以继续提交)
    std::condition_variable _cond_quit;//worker全部退出
    std::queue<Task>        _tasks;
    //worker句柄:必须持有,否则池子可能在worker还活着的时候被析构;用shared_ptr是因为worker线程自己也持有一份,被detach之后对象仍然有效
    std::vector<std::shared_ptr<Worker>> _workers;
    //线程计数
    size_t           _live_threads{0};//当前存活的worker数
    size_t           _idle_threads{0};//当前空闲的worker数
    size_t           _exited_unreaped{0};//已经退出但句柄还没回收的worker数
    size_t           _self_stop_waiters{0};//当前正在shutdown()里等待的worker数:它们不能join自己,停机时要允许它们残留
    size_t           _next_worker_id{0};
    //运行状态与配置
    State                _state{State::NotStarted};
    std::atomic<Sizing>  _sizing{Sizing::Fixed};
    size_t               _min_threads{0};//保底线程数:弹性模式下不被回收
    size_t               _max_threads{THREADPOOL_DEFAULT_MAX_THREADS};//弹性模式下的线程数上限
    size_t               _max_queue{THREADPOOL_DEFAULT_MAX_QUEUE};//任务队列长度上限,0表示不限制
    std::chrono::seconds _idle_timeout{THREADPOOL_DEFAULT_IDLE_TIMEOUT};
    std::chrono::seconds _submit_wait{THREADPOOL_DEFAULT_SUBMIT_WAIT};
    //统计
    std::atomic_uint64_t _submitted{0};
    std::atomic_uint64_t _finished{0};
    std::atomic_uint64_t _rejected{0};
    std::atomic_uint64_t _dropped{0};
    std::atomic_int64_t  _last_warn_ns{0};//限速告警上次打日志的时间(ns)
public:
    ThreadPool()=default;
    ~ThreadPool(){shutdown(true,true);}
    ThreadPool(const ThreadPool&)=delete;
    ThreadPool& operator=(const ThreadPool&)=delete;
    ThreadPool(ThreadPool&&)=delete;
    ThreadPool& operator=(ThreadPool&&)=delete;
public:
    //控制类:启动/停止/提交任务
    //启动线程池:num为0时取CPU核心数,最终会被钳制到[1,最大线程数]
    [[nodiscard]] bool start(size_t num=0)
    {
        if(num==0){num=std::thread::hardware_concurrency();}
        if(num==0){num=1;}//hardware_concurrency()允许返回0,不兜住的话池子会"运行中但零线程"
        std::unique_lock<std::mutex> lock(_mutex);
        if(_state==State::Running)
        {
            WARN("thread pool start ignored: it is already running with {} thread(s)",_live_threads);
            return false;
        }
        if(_state==State::Stopping)
        {
            ERROR("thread pool start rejected: it is still stopping, wait for stop() to return");
            return false;
        }
        reap_exited_locked();
        //上一代可能还有worker在退出的路上(典型情况:某个任务里调过stop(),它的句柄留在_workers里还没人join):
        //它们马上就要结束,这里先join干净再开新一代,否则老worker的退出路径会去改新一代的计数
        if(!_workers.empty())
        {
            //绝不能持着_mutex去join:老worker的退出路径(改计数/登记退出)也要拿_mutex,持锁join就是死锁
            //(实测:worker在任务里自stop()之后任务继续跑,另一个线程start()永久卡死)。
            //join期间把状态伪装成Stopping:并发的start()会被拒、提交会被拒、shutdown()会等老worker退完,都不会以为池子可用
            const State gate=_state;
            _state=State::Stopping;
            std::vector<std::shared_ptr<Worker>> leftover;
            leftover.swap(_workers);
            const std::thread::id self_id=std::this_thread::get_id();
            lock.unlock();
            for(auto& worker:leftover)
            {
                if(!worker->_handle.joinable()){continue;}
                if(worker->_handle.get_id()==self_id)
                {
                    //极端情况:在上一代worker自己的任务里又调start():只能把自己摘出去(先置旗子再detach,顺序不能反)
                    worker->_abandoned.store(true,std::memory_order_release);
                    worker->_handle.detach();
                }
                else
                {
                    worker->_handle.join();
                }
            }
            lock.lock();
            _state=gate;
            _live_threads=0;
            _idle_threads=0;
            _exited_unreaped=0;
        }
        if(num>_max_threads){num=_max_threads;}
        _min_threads=num;
        const State prev_state=_state;//启动彻底失败时要原样还回去,让调用方修完问题还能再start()
        _state=State::Running;
        for(size_t i=0;i<num;i++)
        {
            //spawn除了system_error还可能抛bad_alloc(make_shared/push_back):只接system_error的话bad_alloc会逃逸,状态卡在Running
            try{
                spawn_worker_locked();
            }catch(const std::exception& e){
                ERROR("cannot create a worker thread: {} (created {} of {})",e.what(),_live_threads,num);//已经建出来的线程照常工作:少几个线程总比完全不干活好
                break;
            }
        }
        if(_live_threads==0)
        {
            //一个worker都没建出来:返回false却把状态留在Running的话,池子会永久卡在"运行中但零线程",start()再也进不来
            _state=prev_state;
            ERROR("cannot start the thread pool: all {} worker thread creation(s) failed",num);
            return false;
        }
        //部分成功时把保底线程数压到实际建出来的数:否则之后弹性扩出的线程全被当成保底线程,永远不会被空闲回收
        if(_live_threads<_min_threads){_min_threads=_live_threads;}
        INFO("thread pool started: sizing={}, threads={}, max_threads={}, max_queue={}",_sizing.load(std::memory_order_relaxed)==Sizing::Elastic?"elastic":"fixed",_live_threads,_max_threads,_max_queue);
        return true;
    }
    //优雅停止:队列里还没开始执行的任务会被执行完,然后回收所有worker
    //可以在任意线程调用(内部会join),但不要在线程池自己的任务里销毁线程池对象
    void stop(){shutdown(true,false);}
    //立即停止:队列里还没开始执行的任务被丢弃并计入dropped_tasks(),正在执行的任务不受影响
    void stop_now(){shutdown(false,false);}
    //提交任务,返回future;无法被接受时抛std::runtime_error
    //"无法被接受"的两种情况:池子不在运行(未启动/正在停止)、队列满且等满了提交等待时间
    //返回类型可以是void/值/引用;参数按值拷贝或移动进任务,要传引用请用std::ref
    template<typename Func,typename... Args>
        requires std::invocable<std::decay_t<Func>&,std::decay_t<Args>...>
    [[nodiscard]] auto submit_task(Func&& func,Args&&... args)->std::future<std::invoke_result_t<std::decay_t<Func>,std::decay_t<Args>...>>
    {
        auto accepted=try_submit_task(std::forward<Func>(func),std::forward<Args>(args)...);
        if(!accepted)
        {
            throw std::runtime_error("thread pool rejected the task: the pool is not running, or its queue stayed full");
        }
        return std::move(*accepted);
    }
    //提交任务,不抛异常:没有被接受时返回std::nullopt(同时计入rejected_tasks())
    //"队列满就丢、绝不阻塞业务线程"的场景用这个
    template<typename Func,typename...Args>
        requires std::invocable<std::decay_t<Func>&,std::decay_t<Args>...>
    [[nodiscard]] auto try_submit_task(Func&& func,Args&&... args)->std::optional<std::future<std::invoke_result_t<std::decay_t<Func>,std::decay_t<Args>...>>>
    {
        using ReturnType=std::invoke_result_t<std::decay_t<Func>,std::decay_t<Args>...>;
        //用包初始化捕获的lambda而不是std::bind:std::bind要求可调用对象可拷贝(捕获了unique_ptr的lambda直接编译不过),还会把参数再拷一层
        //整个构造+入队都在try里:本函数承诺"不抛异常",而构造packaged_task(拷贝参数)与入队都可能抛bad_alloc/length_error
        try{
            auto task=std::make_shared<std::packaged_task<ReturnType()>>([func=std::decay_t<Func>(std::forward<Func>(func)),...captured=std::decay_t<Args>(std::forward<Args>(args))]() mutable -> ReturnType{return std::invoke(std::move(func),std::move(captured)...);});
            std::future<ReturnType> result=task->get_future();
            Task queued=[task](){(*task)();};
            if(!enqueue_task(std::move(queued))){return std::nullopt;}
            //注意:这里之后绝不能再访问池子的任何成员:任务一旦入队,worker就可能立刻取走并执行它,如果任务里销毁了池子(违反使用约定,但必须不崩),下面任何一次成员访问都是use-after-free
            //弹性扩容因此必须留在enqueue_task的锁内,不能挪到锁外"为了缩短临界区"
            return std::optional<std::future<ReturnType>>(std::move(result));
        }catch(const std::exception& e){
            //入队前的分配失败(参数太大/内存不足):按"没有被接受"处理,而不是把异常抛给调用方
            _rejected.fetch_add(1,std::memory_order_relaxed);
            warn_limited("thread pool rejected a task: cannot allocate the task object ({})",e.what());
            return std::nullopt;
        }
    }
    //批量提交(不抛异常):一次加锁把整批任务入队、出锁后只唤醒一次worker,把单任务级提交的锁与唤醒开销按批摊薄。
    //这是P1(提交速率随worker数崩塌)的修复:要提交亚微秒级的小任务,请用这个,不要循环try_submit_task。
    //count是任务个数;func是无参可调用对象或"接受size_t下标"的可调用对象(泛型可调用对象按"带下标"调用)。
    //返回真正被接受的个数:0表示池子没运行或始终没有空位;**部分接受是允许的**(没被接受的按个数计入rejected_tasks()),调用方应检查返回值并自行重试剩下部分。
    template<typename Func>
        requires std::invocable<std::decay_t<Func>&>||std::invocable<std::decay_t<Func>&,size_t>
    [[nodiscard]] size_t post_batch(size_t count,Func&& func)
    {
        if(count==0){return 0;}
        //整批只构造一份可调用对象,每个任务捕获它和自己的下标(捕获shared_ptr+size_t会超出std::function的小对象缓冲,每任务一次分配,与单任务路径的packaged_task同量级)
        const auto shared=std::make_shared<std::decay_t<Func>>(std::forward<Func>(func));
        return post_batch_impl(count,[shared](size_t index)
        {
            return [shared,index]() mutable
            {
                if constexpr(std::invocable<std::decay_t<Func>&,size_t>){std::invoke(*shared,index);}
                else{std::invoke(*shared);}
            };
        });
    }
    //批量提交一个区间的每个元素:每个任务处理一个元素(元素按值拷进任务,要移动就传std::make_move_iterator)。
    //要求随机访问迭代器:入队前要先知道能接受多少个,所以"第i个任务"必须能O(1)取到
    template<std::random_access_iterator It,typename Func>
        requires std::invocable<std::decay_t<Func>&,std::iter_value_t<It>>
    [[nodiscard]] size_t post_batch(It first,It last,Func&& func)
    {
        const size_t count=static_cast<size_t>(last-first);
        if(count==0){return 0;}
        using Value=std::iter_value_t<It>;
        const auto shared=std::make_shared<std::decay_t<Func>>(std::forward<Func>(func));
        return post_batch_impl(count,[shared,first](size_t index) mutable
        {
            //元素可能是move-only(unique_ptr之类),而std::function要求目标可拷贝,所以和单任务路径一样用shared_ptr把元素包起来
            auto value=std::make_shared<Value>(std::forward<decltype(first[index])>(first[index]));
            return [shared,value]() mutable{std::invoke(*shared,std::move(*value));};
        });
    }
public:
    //参数设置类
    //线程数量策略(必须在start()之前设置)
    void set_sizing(Sizing value)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if(_state!=State::NotStarted)
        {
            WARN("thread pool set_sizing ignored: it can only be changed before start()");
            return;
        }
        _sizing.store(value,std::memory_order_release);
    }
    //任务队列长度上限(必须在start()之前设置),0表示不限制
    void set_max_queued_tasks(size_t count)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if(_state!=State::NotStarted)
        {
            WARN("thread pool set_max_queued_tasks ignored: it can only be changed before start()");
            return;
        }
        _max_queue=count;
    }
    //弹性模式下的线程数上限(必须在start()之前设置),0会被抬到1
    void set_max_threads(size_t count)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if(_state!=State::NotStarted)
        {
            WARN("thread pool set_max_threads ignored: it can only be changed before start()");
            return;
        }
        _max_threads=count==0?1:count;
    }
    //弹性模式下线程空闲多久被回收(秒);0表示不修改
    void set_idle_timeout(std::chrono::seconds timeout)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        if(timeout.count()>0){_idle_timeout=timeout;}
    }
    //队列满时提交任务的最长等待(秒);0表示不等待
    void set_submit_wait(std::chrono::seconds timeout)
    {
        std::lock_guard<std::mutex> lock(_mutex);
        _submit_wait=timeout;
    }
public:
    //观测类
    //当前状态
    State state()const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return _state;
    }
    //是否处于运行中
    bool running()const{return state()==State::Running;}
    Sizing sizing()const{return _sizing.load(std::memory_order_acquire);}
    //当前存活的worker数
    size_t thread_count()const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return _live_threads;
    }
    //当前空闲(正在等任务)的worker数
    size_t idle_thread_count()const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return _idle_threads;
    }
    //当前正在执行任务的worker数(夹取到0:并发下_idle_threads可能短暂大于_live_threads,无符号相减会得到SIZE_MAX)
    size_t busy_thread_count()const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return _live_threads>_idle_threads?_live_threads-_idle_threads:0;
    }
    //同一时刻的一致快照:分别调用上面几个getter拿到的是"不同瞬间"的数字,算出来的busy可能无意义
    struct Stats
    {
        size_t live{0};     //存活worker数
        size_t idle{0};     //空闲worker数
        size_t busy{0};     //正在执行任务的worker数
        size_t pending{0};  //队列里没开始的任务数
    };
    Stats stats()const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        Stats out;
        out.live=_live_threads;
        out.idle=_idle_threads;
        out.busy=_live_threads>_idle_threads?_live_threads-_idle_threads:0;
        out.pending=_tasks.size();
        return out;
    }
    //队列里还没开始执行的任务数
    size_t pending_tasks()const
    {
        std::lock_guard<std::mutex> lock(_mutex);
        return _tasks.size();
    }
    //累计提交的任务数
    uint64_t submitted_tasks()const{return _submitted.load(std::memory_order_relaxed);}
    //累计执行完的任务数
    uint64_t finished_tasks()const{return _finished.load(std::memory_order_relaxed);}
    //被拒绝的任务数:池子未运行,或者队列满且等满了提交等待时间
    uint64_t rejected_tasks()const{return _rejected.load(std::memory_order_relaxed);}
    //被stop_now()丢弃的任务数(队列里还没开始执行的)
    uint64_t dropped_tasks()const{return _dropped.load(std::memory_order_relaxed);}
private:
    //内部实现:worker生命周期
    //创建一个worker(调用方必须持有_mutex)
    void spawn_worker_locked()
    {
        auto worker=std::make_shared<Worker>();
        worker->_id=_next_worker_id++;
        worker->_extra=_live_threads>=_min_threads;//"是不是额外线程"在创建时就定下来:保底线程永远不会被空闲回收
        std::shared_ptr<Worker> keep=worker;//线程自己持有一份,保证被detach之后对象仍然有效
        //先push_back再建线程:反过来的话push_back抛bad_alloc时,joinable的std::thread随Worker一起析构,直接std::terminate
        _workers.push_back(worker);
        try{
            worker->_handle=std::thread([this,keep](){worker_loop(keep.get(),keep);});
        }catch(...){
            _workers.pop_back();//刚放进去的就是末尾这个
            throw;
        }
        _live_threads++;
        _idle_threads++;
    }
    //worker主循环
    void worker_loop(Worker* self,std::shared_ptr<Worker> keep_alive)
    {
        (void)keep_alive;
        for(;;)
        {
            //池子已经放弃这个线程(worker里调了stop()):立刻退出,不再访问池子的任何成员
            if(self->_abandoned.load(std::memory_order_acquire)){break;}
            Task task;
            {
                std::unique_lock<std::mutex> lock(_mutex);
                if(_state==State::NotStarted||_state==State::Stopped){break;}
                if(_state==State::Running&&_sizing.load(std::memory_order_relaxed)==Sizing::Elastic&&self->_extra)
                {
                    //弹性线程:空闲超过上限就退出。wait_for返回时仍然持有_mutex,所以"判定空闲超时"与"新任务入队"之间没有丢任务的窗口
                    bool signaled=_cond_work.wait_for(lock,_idle_timeout,[this]()
                    {
                        return !_tasks.empty()||_state!=State::Running;
                    });
                    if(!signaled){break;}//空闲超时,回收这个额外线程(保底线程走不到这里)
                }
                else
                {
                    _cond_work.wait(lock,[this]()
                    {
                        return !_tasks.empty()||_state!=State::Running;
                    });
                }
                if(_state==State::Stopped){break;}
                if(_state==State::Stopping&&_tasks.empty()){break;}
                if(_tasks.empty()){continue;}
                task=std::move(_tasks.front());
                _tasks.pop();
                if(_idle_threads>0){_idle_threads--;}
                if(!_tasks.empty()){_cond_work.notify_one();}//还有剩余任务就再唤醒一个
                if(_max_queue>0){_cond_task.notify_one();}//队列腾出了位置,唤醒可能在等空位的提交者;队列没上限时那个等待者不可能存在(等待在enqueue_task的if(_max_queue>0)里),_max_queue只允许在start()前改,这里读它是安全的
            }
            if(task)
            {
                //任务由packaged_task包着,用户异常会被存进future;这里再兜一层,防止packaged_task自身抛出的异常把整个进程带走
                try{
                    task();
                }catch(const std::exception& e){
                    ERROR("a thread pool task threw an exception: {}",e.what());
                }catch(...){
                    ERROR("a thread pool task threw an unexpected exception");
                }
                if(self->_abandoned.load(std::memory_order_acquire))
                {
                    //池子已经被放弃(典型场景:任务里销毁了池子):立刻退出,一个池子成员都不要碰
                    //这个检查必须在下面_finished/_mutex之前,否则就是析构之后的use after free
                    self->_exited.store(true,std::memory_order_release);
                    return;
                }
                _finished.fetch_add(1,std::memory_order_relaxed);
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    _idle_threads++;
                }
            }
        }
        if(self->_abandoned.load(std::memory_order_acquire))
        {
            //池子已经不再等待本线程,退出时不能再碰它的成员(它可能已经被析构)
            self->_exited.store(true,std::memory_order_release);
            return;
        }
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(_live_threads>0){_live_threads--;}
            if(_idle_threads>0){_idle_threads--;}
            self->_exited.store(true,std::memory_order_release);
            _exited_unreaped++;
            //每退一个都要通知:等待方可能只等"降到1"(自己就是worker的场景)
            _cond_quit.notify_all();
            DEBUG("thread pool worker {} exited: threads={}, queued={}",self->_id,_live_threads,_tasks.size());
        }
    }
private:
    //批量入队的非模板实现:与"怎么造第index个任务"无关的锁与队列逻辑只保留一份(与模板参数无关的代码不要内联进模板)
    //prepared是已经造好的任务;返回真正入队的个数(允许部分接受,调用方自己重试剩下的)
    size_t enqueue_prepared(std::vector<Task>& prepared)
    {
        if(prepared.empty()){return 0;}
        const size_t count=prepared.size();
        size_t accepted=0;
        {
            std::unique_lock<std::mutex> lock(_mutex);
            if(_exited_unreaped>0){reap_exited_locked();}
            if(_state!=State::Running)
            {
                _rejected.fetch_add(count,std::memory_order_relaxed);
                warn_limited("thread pool rejected {} batched task(s): the pool is not running (state={})",count,static_cast<int>(_state));
                return 0;
            }
            size_t room=count;
            if(_max_queue>0)
            {
                if(_tasks.size()>=_max_queue)
                {
                    //整批只等一次(和单任务一样最多_submit_wait秒),不等到凑够count个空位:剩下的按"部分接受"返回
                    const bool freed=_cond_task.wait_for(lock,_submit_wait,[this]()
                    {
                        return _state!=State::Running||_tasks.size()<_max_queue;
                    });
                    if(!freed||_state!=State::Running)
                    {
                        _rejected.fetch_add(count,std::memory_order_relaxed);
                        warn_limited("thread pool rejected {} batched task(s): the queue is full or the pool stopped (queued={}, limit={})",count,_tasks.size(),_max_queue);
                        return 0;
                    }
                }
                const size_t free_slots=_max_queue-_tasks.size();
                if(room>free_slots){room=free_slots;}
            }
            //临界区里只做移动:std::function的移动不分配内存,不会把worker的pop堵在任务构造上
            bool queue_failed=false;
            try{
                for(size_t i=0;i<room;i++){_tasks.push(std::move(prepared[i]));accepted++;}
            }catch(const std::exception& e){
                queue_failed=true;
                warn_limited("thread pool rejected {} batched task(s): cannot queue them ({})",count-accepted,e.what());
            }
            //没进去的必须如实计入rejected:否则submitted+rejected+finished这套账就对不上提交总数了(部分接受时"没进去的"同样是"没有被接受")
            if(accepted<count)
            {
                _rejected.fetch_add(count-accepted,std::memory_order_relaxed);
                if(!queue_failed){warn_limited("thread pool rejected {} batched task(s): no room in the queue (accepted={}, requested={}, limit={})",count-accepted,accepted,count,_max_queue);}
            }
            if(accepted>0)
            {
                _submitted.fetch_add(accepted,std::memory_order_relaxed);
                if(_sizing.load(std::memory_order_relaxed)==Sizing::Elastic&&_live_threads<_max_threads&&_tasks.size()>_idle_threads)
                {
                    try{
                        spawn_worker_locked();
                        DEBUG("thread pool grew to {} thread(s): queued={}, idle={}, floor={}",_live_threads,_tasks.size(),_idle_threads,_min_threads);
                    }catch(const std::exception& e){
                        ERROR("cannot grow the thread pool: {}",e.what());
                    }
                }
            }
        }//唤醒放在解锁之后:被唤醒的worker不必再和"还持着锁的提交者"抢同一把锁(单任务路径的notify在锁内,那里不改动,以免影响已测过的t1/t4)
        if(accepted>0){_cond_work.notify_one();}
        return accepted;
    }
    //把count个任务造出来再交给enqueue_prepared:maker(index)返回第index个任务,只按真正要入队的个数造,避免被拒绝时白分配
    template<typename Maker>
    size_t post_batch_impl(size_t count,Maker&& maker)
    {
        std::vector<Task> prepared;
        size_t built=0;
        try{
            prepared.reserve(count);
            for(;built<count;built++){prepared.push_back(Task(maker(built)));}
        }catch(const std::exception& e){
            //造任务时内存不够:已经造好的照常提交,没造出来的如实计入rejected
            _rejected.fetch_add(count-built,std::memory_order_relaxed);
            warn_limited("thread pool cannot build {} batched task(s) ({})",count-built,e.what());
        }
        return enqueue_prepared(prepared);
    }
    //内部实现:任务入队
    //把任务交给worker:返回false表示没有被接受(池子没运行,或者队列满且等满了提交等待时间)
    bool enqueue_task(Task task)
    {
        std::unique_lock<std::mutex> lock(_mutex);
        //弹性线程退出后句柄还留在_workers里,线程本身也一直是joinable(zombie)状态;回收不能只放在start()/shutdown(),长跑的服务会一轮轮扩缩,句柄与线程资源会一直累积。
        //这里用计数器保证"没有线程退出"时零开销,有退出时顺手join掉(立即返回)
        if(_exited_unreaped>0){reap_exited_locked();}
        if(_state!=State::Running)
        {
            _rejected.fetch_add(1,std::memory_order_relaxed);
            warn_limited("thread pool rejected a task: the pool is not running (state={})",static_cast<int>(_state));
            return false;
        }
        if(_max_queue>0&&_tasks.size()>=_max_queue)
        {
            //队列满:最多等_submit_wait秒,期间有worker取走任务就会被唤醒
            bool room=_cond_task.wait_for(lock,_submit_wait,[this]()
            {
                return _state!=State::Running||_tasks.size()<_max_queue;
            });
            if(!room||_state!=State::Running)
            {
                _rejected.fetch_add(1,std::memory_order_relaxed);
                warn_limited("thread pool rejected a task: the queue is full or the pool stopped (queued={}, limit={})",_tasks.size(),_max_queue);
                return false;
            }
        }
        _tasks.push(std::move(task));
        _submitted.fetch_add(1,std::memory_order_relaxed);
        //只需要唤醒一个空闲worker
        _cond_work.notify_one();
        //弹性模式:待处理任务多于空闲线程就补一个线程。
        //必须在锁内做:任务已经入队,worker随时可能取走它,出了锁再访问池子就可能UAF;创建线程要几十微秒,但只有"需要扩容"的那几次提交会走到这里,不是热路径
        if(_sizing.load(std::memory_order_relaxed)==Sizing::Elastic&&_live_threads<_max_threads&&_tasks.size()>_idle_threads)
        {
            //扩容失败只影响"跑得快一点",绝不能让异常逃出enqueue_task:任务已经入队且已计数,逃出去会被调用方当成"被拒绝",但任务照样会被执行(契约违反+双计数)
            try{
                spawn_worker_locked();
                DEBUG("thread pool grew to {} thread(s): queued={}, idle={}, floor={}",_live_threads,_tasks.size(),_idle_threads,_min_threads);
            }catch(const std::exception& e){
                ERROR("cannot grow the thread pool: {}",e.what());
            }
        }
        return true;
    }
    //内部实现:停机
    //停止的公共实现:drain=true表示先把队列里的任务跑完
    //destroying:本次停机是不是来自析构函数。worker线程里析构线程池是违反使用约定的用法,那时只能把自己摘出去(见下面分支)
    void shutdown(bool drain,bool destroying)
    {
        std::vector<std::shared_ptr<Worker>> workers;
        bool was_active=false;
        bool self_worker=false;
        bool collected=false;
        {
            std::unique_lock<std::mutex> lock(_mutex);
            //"已经停干净"必须同时满足:状态是NotStarted/Stopped且 没有活着的worker。
            //只看状态是不够的:stop_now()期间状态在worker退出之前就已经是Stopped,此时若放行,句柄会被偷走,
            //正在自己shutdown里的worker永远拿不到_abandoned标记,之后照常改_idle_threads -> 计数崩坏+停机线程永久卡死。
            //(两个线程并发停机时第二个会走正常路径:它换到的是空句柄表,内容无害)
            if((_state==State::NotStarted||_state==State::Stopped)&&_live_threads==0)
            {
                //没启动过或者已经停干净:只把残留句柄收干净
                reap_exited_locked();
                workers.swap(_workers);
                _idle_threads=0;
                _exited_unreaped=0;
                //没启动过就保持NotStarted:置成Stopped会让之后的set_sizing/set_max_threads被静默忽略,而start()仍然可用
                collected=true;//句柄已经收进workers:下面的正常停机分支绝不能再swap一次,否则句柄被换回_workers、锁外join落空,还活着的worker没人等(ASAN实测:worker退出路径在池子析构后UAF)
            }
            else if(!destroying)
            {
                //先判断自己是不是worker
                const std::thread::id self_id=std::this_thread::get_id();
                for(const auto& worker:_workers)
                {
                    if(worker->_handle.joinable()&&worker->_handle.get_id()==self_id){self_worker=true;break;}
                }
            }
            if(self_worker&&!destroying)
            {
                //worker自己的任务里调stop()(池子不析构):只请求停机并等其它worker退完,自己这个线程绝不能join自己,
                //句柄也留在_workers里——交给之后某个非worker线程的stop()/析构去join,否则句柄会随着局部变量一起消失
                was_active=true;
                _state=drain?State::Stopping:State::Stopped;
                if(!drain&&!_tasks.empty())
                {
                    uint64_t dropped=_tasks.size();
                    std::queue<Task> empty;
                    _tasks.swap(empty);
                    _dropped.fetch_add(dropped,std::memory_order_relaxed);
                    WARN("thread pool stop_now() dropped {} queued task(s) that had not started yet",static_cast<unsigned long long>(dropped));
                }
                _cond_work.notify_all();
                _cond_task.notify_all();
                //把自己算进"允许残留的活线程数":可能有多个worker同时这么做,必须是计数而不是布尔
                _self_stop_waiters++;
                _cond_quit.wait(lock,[this]()
                {
                    return _live_threads<=_self_stop_waiters;
                });
                if(_self_stop_waiters>0){_self_stop_waiters--;}
                //除自己以外都退完了(自己的退出由工作线程自己的退出路径负责,那里有下限保护)
                _live_threads=0;
                _idle_threads=0;
                _exited_unreaped=0;
                //状态直接收敛:本线程的句柄还留在_workers里,start()会先把上一代join干净再开新一代
                _state=State::Stopped;
                _cond_quit.notify_all();
                WARN("thread pool stop() was called from one of its own worker threads: that thread cannot be joined, its handle is kept for a later stop()/destructor");
                return;
            }
            else if(!collected)
            {
                was_active=true;
                //两种停机都用Stopping,Stopped只在所有句柄join完之后才置:
                //这样停机期间start()一定被拒绝,不会出现"旧worker还没退、新一代已经起来"的两代并存
                _state=State::Stopping;
                if(!drain&&!_tasks.empty())
                {
                    //不排空:队列里没开始的任务全部丢弃,并如实计数
                    uint64_t dropped=_tasks.size();
                    std::queue<Task> empty;
                    _tasks.swap(empty);
                    _dropped.fetch_add(dropped,std::memory_order_relaxed);
                    WARN("thread pool stop_now() dropped {} queued task(s) that had not started yet",static_cast<unsigned long long>(dropped));
                }
                _cond_work.notify_all();
                _cond_task.notify_all();
                std::thread::id self_id=std::this_thread::get_id();
                for(const auto& worker:_workers)
                {
                    if(worker->_handle.joinable()&&worker->_handle.get_id()==self_id){self_worker=true;break;}
                }
                if(self_worker)
                {
                    //在worker自己的任务里调stop():不能等自己退出,把自己算进"允许残留的活线程数"。
                    //必须是计数而不是布尔:多个worker同时这么做时,只允许一个会让它们互相等死
                    _self_stop_waiters++;
                }
                _cond_quit.wait(lock,[this]()
                {
                    return _live_threads<=_self_stop_waiters;
                });
                //注意:self_stop_waiters在这里绝不动,必须等下面锁外join全部回来再减(见join后的收尾块)。
                //提前减的话:别的worker还在等_live_threads<=_self_stop_waiters,本线程是活线程不会去减_live_threads,
                //它们的谓词永远等不到 -> 双向死锁(实测:一个worker任务里stop()、另一个worker任务里析构池子)
                workers.swap(_workers);
                if(!self_worker)
                {
                    _live_threads=0;
                    _idle_threads=0;
                    _exited_unreaped=0;
                }
                //worker内调stop()时故意先不清_live_threads:一旦清零,外面(例如析构函数)会立刻开始销毁对象,
                //而本线程下面还要join其它worker,它们的退出路径还会碰池子。留到join全部返回之后再清。
                _cond_quit.notify_all();
            }
            if(self_worker)
            {
                //所有会碰池子的收尾都必须在还持锁时做完(下面清零_live_threads之后就可能有人销毁对象了)
                WARN("thread pool stop() was called from one of its own worker threads: that thread cannot be joined and is abandoned after its task returns");
            }
        }
        //join必须在锁外:worker退出路径上也要拿_mutex,持锁join会死锁
        for(auto& worker:workers)
        {
            if(!worker->_handle.joinable()){continue;}
            if(worker->_handle.get_id()==std::this_thread::get_id())
            {
                //把自己摘出去:先置_abandoned,再detach,顺序不能反,否则本线程可能在跑到退出路径时仍然去访问马上就要析构的池子
                worker->_abandoned.store(true,std::memory_order_release);
                worker->_handle.detach();
            }
            else
            {
                worker->_handle.join();
            }
        }
        if(self_worker)
        {
            //join全部返回:所有worker的退出路径都已经结束,现在才能清零计数并放行等待者(可能是正在析构本对象的线程)。
            //这个块之后本线程绝不能再碰池子的任何成员(那面_abandoned旗子会让worker循环直接退出)
            std::lock_guard<std::mutex> lock(_mutex);
            if(_self_stop_waiters>0){_self_stop_waiters--;}//把自己从自停等待者里减掉:此刻减是安全的,其他自停worker看到的谓词只会更松
            _live_threads=0;
            _idle_threads=0;
            _exited_unreaped=0;
            _state=State::Stopped;
            _cond_quit.notify_all();
            return;
        }
        {
            std::lock_guard<std::mutex> lock(_mutex);
            if(was_active){_state=State::Stopped;}
            _cond_quit.notify_all();
        }
        if(was_active)
        {
            INFO("thread pool stopped: submitted={} finished={} rejected={} dropped={}",static_cast<unsigned long long>(submitted_tasks()),static_cast<unsigned long long>(finished_tasks()),static_cast<unsigned long long>(rejected_tasks()),static_cast<unsigned long long>(dropped_tasks()));
        }
    }
    //回收已经退出的worker句柄(调用方必须持有_mutex);join一个已经结束的线程会立刻返回
    void reap_exited_locked()
    {
        _exited_unreaped=0;
        for(auto it=_workers.begin();it!=_workers.end();)
        {
            Worker* worker=it->get();
            if(!worker->_exited.load(std::memory_order_acquire)){++it;continue;}
            if(worker->_handle.joinable())
            {
                if(worker->_handle.get_id()==std::this_thread::get_id())
                {
                    //自己join自己会抛异常;这是"worker里调stop()"的场景,由shutdown负责detach
                    ++it;
                    continue;
                }
                worker->_handle.join();
            }
            it=_workers.erase(it);
        }
    }
private:
    //内部实现:限速告警
    //同一类失败最多每秒一条,避免提交失败时把日志刷爆;模板化之后格式串与实参由编译器按std::format规则校验,不必先vsnprintf进中间缓冲再当成字符串打出来
    template<typename... Args>
    void warn_limited(std::format_string<Args...> fmt,Args&&... args)
    {
        const int64_t now_ns=std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        int64_t last=_last_warn_ns.load(std::memory_order_relaxed);//compare_exchange_strong需要非const左值
        if(now_ns-last<1000000000LL){return;}
        if(!_last_warn_ns.compare_exchange_strong(last,now_ns,std::memory_order_relaxed)){return;}
        WARN(fmt,std::forward<Args>(args)...);
    }
};
