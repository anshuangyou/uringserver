#pragma once
#include "log.hpp"
#include "buffer.hpp"
#include "iouring.hpp"
#include "socket.hpp"
#include <condition_variable>
#include <shared_mutex>
#include <mutex>
#include <unordered_map>
#include <netinet/tcp.h>
#include <any>
#include <thread>
class Connection:public std::enable_shared_from_this<Connection>
{
private:
    enum class State
    {
        Disconnected=0,
        Connectioning=1,
        Connected=2,
        DisConnecting=3,
    };
    using ConnPtr=std::shared_ptr<Connection>;
    using ConnCallback=std::function<void(const ConnPtr&)>;
    using RecvCallback=std::function<void(const ConnPtr&,std::shared_ptr<Buffer>)>;
private:
    //连接标识与对端地址
    IoUring*                      _uring;
    std::atomic<int>              _fd{-1};
    uint64_t                      _id{0};
    uint16_t                      _port{0};
    std::string                   _ip{""};
    bool                          _ipv6{false};
    //收发缓冲:接收缓冲由内核异步填写;发送进行中新数据只能进_pending_buffer,绝不能动内核正在使用的_out_buffer
    std::shared_ptr<BufferPool>   _buffer_pool;
    std::shared_ptr<Buffer>       _in_buffer;
    std::shared_ptr<Buffer>       _out_buffer;
    std::shared_ptr<Buffer>       _pending_buffer;
    std::mutex                    _send_mtx;
    std::atomic_bool              _sending{false};
    std::atomic<size_t>           _max_pending_send{BUFFER_MAX_CAPACITY};//setter不拿_send_mtx,读它的地方在锁里:必须原子,否则读写构成数据竞争
    std::atomic_uint64_t          _bytes_queued{0};//累计交给发送子系统的字节数(供访问日志/指标统计)
    std::function<void()>         _send_drained_callback;//发送缓冲区彻底排空时的回调(只在_send_mtx之外调用),流式发送用它做背压
    //生命周期状态:_closing是close_now()/force_close()/discard()共用的门闩,保证fd只会被真正关闭一次
    std::atomic<State>            _state{State::Connectioning};
    std::atomic_bool              _closing{false};
    std::atomic_bool              _consume_thread_warned{false};//consume()被跨线程调用时只告警一次
    //空闲超时:判定是惰性的,只更新时间戳,真正关闭由on_timeout_check负责
    std::mutex                    _timer_mtx;
    std::atomic_bool              _timed_release{false};
    std::atomic_uint64_t          _timer_id{0};
    uint64_t                      _timer_gen{0};//定时器链代次(只在_timer_mtx内访问):换链/拆链都+1,回调按代次认链,被换掉/拆掉的链就地终止——否则on_timeout_check的"锁外挂表+锁内覆盖_timer_id"会与set_timeout/shutdown互相覆盖,产生永不被取消的孤儿定时器链
    std::atomic<int64_t>          _last_active{0};
    std::atomic_uint32_t          _timeout{static_cast<uint32_t>(TIMEOUT_SECONDS.count())};
    std::function<void()>         _timeout_callback;//定时器到期回调(默认关闭连接),HTTP用它区分"请求超时"与"连接空闲"
    //业务上下文:建立连接时写一次、之后每个请求读多次,用读写锁让多个worker的读并发
    std::any                      _context;
    mutable std::shared_mutex     _context_mtx;
    //生命周期回调:establish()之后统一冻结,之后再改就是数据竞争,只告警不生效
    ConnCallback                  _connected_callback;
    RecvCallback                  _recv_callback;
    ConnCallback                  _close_callback;
    ConnCallback                  _clear_callback;
    std::atomic_bool              _callbacks_frozen{false};
public:
    Connection(const Connection&)=delete;
    Connection& operator=(const Connection&)=delete;
    Connection(Connection&&)=delete;
    Connection& operator=(Connection&&)=delete;
    Connection(IoUring* uring,int fd,uint64_t id,std::shared_ptr<BufferPool> buffer_pool=nullptr)
    :_uring(uring)
    ,_fd(fd)
    ,_id(id)
    ,_buffer_pool(buffer_pool)
    {
        //接收缓冲从池里借(池为空则新建),连接销毁时自动归还
        _in_buffer=buffer_pool?buffer_pool->acquire(BUFFER_DEFAULT_CAPACITY,MAX_RECV_BUFFER_SIZE):std::make_shared<Buffer>(BUFFER_DEFAULT_CAPACITY,MAX_RECV_BUFFER_SIZE);
        if(!_in_buffer){_in_buffer=std::make_shared<Buffer>(BUFFER_DEFAULT_CAPACITY,MAX_RECV_BUFFER_SIZE);}
        //F_GETFL失败时flags是-1:直接拿去F_SETFL会把O_ASYNC等所有可写位全置上,之后每有数据到达就收到SIGIO(默认动作是终止进程)
        const int fl_flags=fcntl(fd,F_GETFL);
        if(fl_flags>=0){fcntl(_fd,F_SETFL,fl_flags|O_NONBLOCK);}
        //关闭Nagle算法。本库收到数据就立刻异步发送,一次响应会被拆成多个send提交:
        //Nagle打开后一个分片要等前一个分片的ACK,而ACK又被延迟确认拖住,大于一个接收缓冲区的响应会出现约40ms的固定停顿
        int nodelay=1;
        if(setsockopt(_fd,IPPROTO_TCP,TCP_NODELAY,&nodelay,sizeof(nodelay))<0)
        {
            WARN("cannot disable Nagle on conn {}: fd={}, error={}",_id,fd,errno_text(errno).c_str());
        }
        //用sockaddr_storage统一接收v4/v6地址
        sockaddr_storage addr{};
        socklen_t len=sizeof(addr);
        if(getpeername(_fd,reinterpret_cast<struct sockaddr*>(&addr),&len)==0)
        {
            _port=storage_to_port(addr);
            _ip=storage_to_ip_string(addr);
            _ipv6=(addr.ss_family==AF_INET6);
            if(_ip.empty()){_ip="127.0.0.1";}
        }
        else
        {
            //RST比accept先到的连接本来就拿不到对端地址,这是常态而不是故障:按DEBUG记,免得被RST风暴刷屏
            DEBUG("cannot read the peer address of conn {}: fd={}, error={}",_id,fd,errno_text(errno).c_str());
            _port=0;
            _ip="127.0.0.1";
        }
    }
    ~Connection(){disable_timed_release();}
public:
    //生命周期控制
    //建立连接:四个回调必须在调用它之前一次性注册完毕
    void establish()
    {
        State expected=State::Connectioning;
        if(!_state.compare_exchange_strong(expected,State::Connected,std::memory_order_acq_rel,std::memory_order_acquire))
        {
            return;//连接在建立之前已经被关闭
        }
        //Connection没有为回调指针加锁,建立之后再改就是数据竞争,所以通知完on_connected就把生命周期回调冻结,之后再改只告警不生效
        if(!_recv_callback)
        {
            WARN("conn {} was established without a recv callback: incoming data will be discarded",_id);
        }
        if(_connected_callback){_connected_callback(shared_from_this());}
        _callbacks_frozen.store(true,std::memory_order_release);
        if(_state.load(std::memory_order_acquire)!=State::Connected){return;}//回调里可能已经关闭了连接
        auto self=shared_from_this();
        bool submitted=false;
        int recv_fd=-1;
        {
            //提交recv必须与force_close() 的"摘fd+close"互斥:锁外读到fd号之后,fd可能已经被关闭并被新连接复用,
            //这次recv就提交到别人的socket上,把别人的数据读进本连接并回调出去(发送路径已经这么修,这里对齐)
            std::lock_guard<std::mutex> lock(_send_mtx);
            recv_fd=_fd.load(std::memory_order_acquire);
            if(recv_fd>=0){submitted=_uring->async_recv(recv_fd,_in_buffer,[self](IoTask& task,ssize_t size){self->on_recv_complete(task,size);});}
        }
        if(!submitted)
        {
            ERROR("cannot submit the first recv for conn {}: fd={}, closing the connection",_id,recv_fd);
            shutdown();
        }
    }
    //发送数据,返回false表示连接已关闭或者待发数据超过上限
    bool send(const char* data,size_t len)
    {
        if(len==0){return true;}//长度为0的数据认定为发送成功
        if(_state.load(std::memory_order_acquire)!=State::Connected){return false;}
        bool failed=false;
        {
            std::lock_guard<std::mutex> lock(_send_mtx);
            //必须在锁内复查状态:上面检查与这里加锁之间,shutdown()可能已经完成了begin_close()甚至close_now()
            if(_state.load(std::memory_order_acquire)!=State::Connected)
            {
                failed=true;
            }
            else
            {
                size_t pending=(_out_buffer?_out_buffer->readable_size():0)+(_pending_buffer?_pending_buffer->readable_size():0);
                //背压保护:业务不消费pending_send_size()时,待发数据会无限堆积直到把内存吃光
                if(pending+len>_max_pending_send.load(std::memory_order_acquire))
                {
                    ERROR("send rejected on conn {}: queued={} bytes, this call={} bytes, limit={} bytes, closing the connection",_id,pending,len,_max_pending_send.load(std::memory_order_acquire));
                    failed=true;
                }
                else
                {
                    try{
                        //发送进行中时只能追加到待发缓冲区,不能动正在被内核使用的_out_buffer
                        if(_sending.load(std::memory_order_acquire))
                        {
                            if(!_pending_buffer){_pending_buffer=acquire_send_buffer();}
                            _pending_buffer->append(data,len);
                        }
                        else
                        {
                            if(!_out_buffer){_out_buffer=acquire_send_buffer();}
                            _out_buffer->append(data,len);
                        }
                    }catch(const std::bad_alloc& e){
                        ERROR("cannot buffer {} bytes for conn {}: {}, closing the connection",len,_id,e.what());
                        failed=true;
                    }catch(...){
                        ERROR("unexpected error while buffering {} bytes for conn {}, closing the connection",len,_id);
                        failed=true;
                    }
                }
                if(!failed&&!_sending.load(std::memory_order_acquire)&&_out_buffer&&_out_buffer->readable_size()>0)
                {
                    _sending.store(true,std::memory_order_release);
                    //提交必须在_send_mtx内做:submit_send要读_fd,而force_close()在同一把锁里摘fd并同步close。
                    //锁外提交的话,可能出现"读了fd号还没提交、fd已被关闭并被复用",数据发到陌生人的socket上
                    if(!submit_send()){_sending.store(false,std::memory_order_release);failed=true;}
                }
            }
        }
        if(failed){shutdown();return false;}
        //累计交给发送子系统的字节数(含头部与正文,含零拷贝切出去的部分),可用差分统计每个响应发送多少个字节
        _bytes_queued.fetch_add(len,std::memory_order_relaxed);
        return true;
    }
    [[nodiscard]] bool send(std::span<const char> data){return send(data.data(),data.size());}
    [[nodiscard]] bool send(const Buffer& buf){return send(buf.readable());}
    [[nodiscard]] bool send(const Buffer* buf){return buf?send(buf->readable()):false;}
    [[nodiscard]] bool send(const std::string& str){return send(str.data(),str.size());}
    //线程安全的发送:先把数据拷进一个共享缓冲,再投递回ring线程调用send(),适用于业务线程池不允许直接碰连接对象的场景
    [[nodiscard]] bool send_on_ring(const void* data,size_t len)
    {
        if(len==0){return true;}
        if(!data){return false;}
        if(_state.load(std::memory_order_acquire)!=State::Connected){return false;}
        auto payload=std::make_shared<std::string>(static_cast<const char*>(data),len);
        auto self=shared_from_this();
        return _uring->post([self,payload](){self->send(payload->data(),payload->size());});
    }
    //把一段逻辑投递回拥有该连接的IoUring线程执行。业务线程池可直接调用:连接在任务执行完之前不会被释放,任务里再调send()/shutdown()就是本线程操作
    [[nodiscard]] bool post(std::function<void()> fn)
    {
        if(!fn){return false;}
        if(_state.load(std::memory_order_acquire)!=State::Connected){return false;}
        auto self=shared_from_this();
        return _uring->post([self,fn=std::move(fn)]()mutable{fn();});
    }
    //优雅关闭:先把待发数据发完,再关闭
    void shutdown()
    {
        if(!begin_close()){return;}
        bool deferred=false;
        {
            std::lock_guard<std::mutex> lock(_send_mtx);
            //发送进行中时绝对不要动_out_buffer:内核此刻正持着它上一次提交时记下的指针与长度,
            //此时往_out_buffer里追加数据,reserve_for可能把未发数据搬到缓冲区头部、甚至resize触发vector重新分配,
            //正在飞的那个send就会从错误的位置(或已释放的内存)取数据发给对端,表现为响应末尾一段莫名其妙的字节
            //待发数据先留在_pending_buffer里,交给on_send_complete在发送完成后再合并提交
            if(_sending.load(std::memory_order_acquire))
            {
                deferred=true;
            }
            else
            {
                //把待发缓冲区的数据并入发送缓冲区
                if(_pending_buffer&&_pending_buffer->readable_size()>0)
                {
                    try{
                        if(!_out_buffer){_out_buffer=acquire_send_buffer();}
                        _out_buffer->append(_pending_buffer->readable());
                        _pending_buffer->reset();
                    }catch(const std::exception& e){
                        ERROR("cannot merge the pending send buffer while closing conn {}: {}",_id,e.what());
                    }
                }
                //还有数据没发完,等发送完成后由on_send_complete收尾
                if(_out_buffer&&_out_buffer->readable_size()>0)
                {
                    _sending.store(true,std::memory_order_release);
                    if(submit_send()){deferred=true;}
                    else{_sending.store(false,std::memory_order_release);}
                }
            }
        }
        if(deferred)
        {
            //收尾已经托付给on_send_complete,但"发送还在途中"有可能永远结束不了(对端零窗口、一直不读、或者干脆掉线不回应)
            //原来这里是在进函数时就disable_timed_release(),把空闲定时器一并取消了:于是这条连接既不会被超时回收,又等不到发送完成,fd与缓冲永久泄漏
            //现在改成挂一个"关闭兜底期限",到点还没发完就由on_timeout_check强制关闭
            bool failed=false;
            {
                std::lock_guard<std::mutex> lock(_timer_mtx);
                _last_active.store(now_ns(),std::memory_order_release);//兜底时间从"进入关闭"这一刻重新起算
                ++_timer_gen;//换链:空闲超时链上还没跑的回调按代次自检退出
                const uint64_t gen=_timer_gen;
                uint64_t old=_timer_id.load(std::memory_order_acquire);
                if(old!=0){_uring->cancel_timer(old);}
                const uint32_t time=_timeout.load(std::memory_order_acquire);
                auto self=shared_from_this();
                uint64_t id=_uring->add_timer(std::chrono::seconds(time?time:1),[self,gen](){self->on_timeout_check(gen);});
                if(id==0){failed=true;}
                else
                {
                    _timer_id.store(id,std::memory_order_release);
                    _timed_release.store(true,std::memory_order_release);
                }
            }
            //挂不上表就退回到"立刻强制关闭":绝不能留下一条永远关不掉的连接
            if(failed)
            {
                ERROR("cannot arm the close deadline of conn {}, closing it immediately",_id);
                abort_close();
            }
            return;
        }
        disable_timed_release();
        close_now();
    }
    //立即同步关闭连接,会丢弃待发数据,并且不依赖异步close的完成回调,保证fd一定被释放
    void force_close()
    {
        //与close_now()共用同一把门闩。原来这里直接置位_closing、不看别人是否已经在关闭:
        //close_now()可能已经把fd交给IORING_OP_CLOSE(甚至已经完成、fd号被系统复用),此时force_close()再::close一次,关掉的就是别人的fd
        if(_closing.exchange(true,std::memory_order_acq_rel)){return;}
        _state.store(State::DisConnecting,std::memory_order_release);
        disable_timed_release();
        _sending.store(false,std::memory_order_release);
        //摘fd与同步close必须在_send_mtx内:submit_send在同一把锁里读fd并提交,这扇窗口不关上,
        //就会出现"发送线程读到旧fd号还没提交、这里已经close且fd号被复用",数据发到陌生人的socket上
        int fd=-1;
        {
            std::lock_guard<std::mutex> lock(_send_mtx);
            fd=_fd.exchange(-1,std::memory_order_acq_rel);
            if(fd>=0)
            {
                ::shutdown(fd,SHUT_RDWR);
                ::close(fd);
            }
        }
        if(fd<0){return;}//已经关闭过,回调也已经触发过
        //与on_close_complete一致:关闭时必须唤醒还在等"发送排空"的一方,
        //否则应用被arm_send_drained_callback()挂起后,stop()的兜底force_close只会关掉fd,等待方永远等不到回调(实测挂死)
        std::function<void()> drained;
        {
            std::lock_guard<std::mutex> lock(_send_mtx);
            drained=std::move(_send_drained_callback);
            _send_drained_callback=nullptr;
        }
        //生命周期回调是用户代码,绝不能把异常放出force_close:stop()尾段会批量调它,异常逃逸到~TcpServer就是terminate
        try{
            if(_close_callback){_close_callback(shared_from_this());}
        }catch(const std::exception& e){ERROR("close callback of conn {} threw an exception: {}",_id,e.what());}
        catch(...){ERROR("close callback of conn {} threw an unexpected exception",_id);}
        try{
            if(_clear_callback){_clear_callback(shared_from_this());}
        }catch(const std::exception& e){ERROR("clear callback of conn {} threw an exception: {}",_id,e.what());}
        catch(...){ERROR("clear callback of conn {} threw an unexpected exception",_id);}
        try{
            if(drained){drained();}
        }catch(const std::exception& e){ERROR("send-drained callback of conn {} threw an exception: {}",_id,e.what());}
        catch(...){ERROR("send-drained callback of conn {} threw an unexpected exception",_id);}
    }
    //放弃一条"还没有establish()"的连接:只释放fd,不触发任何生命周期回调
    //accept之后、establish()之前被丢弃的连接,上层从没在on_connected里见过它,补一条on_close会让"按on_connected/on_close配对计数"的应用逻辑错位
    //已经建立过的连接必须走force_close():它可能还在连接表里,只有clear回调能把它移除
    void discard()
    {
        if(_state.load(std::memory_order_acquire)!=State::Connectioning)
        {
            force_close();
            return;
        }
        //与close_now()/force_close()共用同一把门闩,保证fd只会被关闭一次
        if(_closing.exchange(true,std::memory_order_acq_rel)){return;}
        _state.store(State::DisConnecting,std::memory_order_release);
        //预建立连接也可能被上层挂过空闲定时器(establish之前enable_timed_release是合法的):
        //不拆的话定时器靠时间轮持有shared_ptr苟活,ring销毁后~Connection再去cancel_timer就是UAF
        disable_timed_release();
        int fd=_fd.exchange(-1,std::memory_order_acq_rel);
        if(fd<0){return;}
        ::shutdown(fd,SHUT_RDWR);
        ::close(fd);
    }
    //消费掉已经处理完的字节,只能在该连接的ring线程上调用(也就是recv回调内部)
    //跨线程调用会和内核/回调线程同时改读写下标,属于数据竞争
    void consume(size_t size)
    {
        if(IoUring::current()!=_uring&&!_consume_thread_warned.exchange(true,std::memory_order_acq_rel))
        {
            WARN("Connection::consume of conn {} was called from a foreign thread: it must run on the IoUring worker that owns the connection (that is, inside the recv callback)",_id);
        }
        size=size>_in_buffer->readable_size()?_in_buffer->readable_size():size;
        (void)_in_buffer->consume(size);//上面已经按readable_size()钳过,不会失败
    }
    void enable_timed_release(std::chrono::seconds timeout=TIMEOUT_SECONDS)
    {
        const uint32_t time=static_cast<uint32_t>(timeout.count());
        if(time==0){return;}
        //连接已进入关闭流程就绝不能再碰_uring:此时服务器可能已经停机、IoUring对象已经被析构,
        //而用户可能还留着自己的shared_ptr<Connection>,再挂定时器就是对已析构对象的use after free
        if(_closing.load(std::memory_order_acquire)){return;}
        std::lock_guard<std::mutex> lock(_timer_mtx);
        if(_timed_release.load(std::memory_order_acquire)){return;}
        _timeout.store(time,std::memory_order_release);
        _last_active.store(now_ns(),std::memory_order_release);
        auto self=shared_from_this();
        const uint64_t gen=++_timer_gen;
        //时间轮只有TIMING_WHEEL_SLOTS个槽:add_timer会把"这一次挂多久"统一钳进合法范围,而超时值本身不钳制
        //到期后on_timeout_check会按剩余时间继续重挂,所以120秒超时会分成几段59秒来等,语义不变
        uint64_t id=_uring->add_timer(timeout,[self,gen](){self->on_timeout_check(gen);});
        if(id==0)
        {
            ERROR("cannot arm the idle timer of conn {} with timeout={}s: the timer wheel rejected it",_id,time);
            return;
        }
        _timer_id.store(id,std::memory_order_release);
        _timed_release.store(true,std::memory_order_release);
    }
    void disable_timed_release()
    {
        std::lock_guard<std::mutex> lock(_timer_mtx);
        if(!_timed_release.load(std::memory_order_acquire)){return;}
        ++_timer_gen;//摘掉这条链:已经从轮上取下但还没跑的回调会按代次自检退出
        _uring->cancel_timer(_timer_id.load(std::memory_order_acquire));
        _timed_release.store(false,std::memory_order_release);
        _timer_id.store(0,std::memory_order_release);
    }
    //刷新空闲计时:只更新一个时间戳
    //原实现是"取消旧定时器+重新挂一个",每次收发都要加两次锁、改一次哈希表和时间轮链表,高QPS下这部分开销会和业务逻辑一样大;改成惰性判定后只剩一次原子写
    void refresh_timeout()
    {
        if(!_timed_release.load(std::memory_order_acquire)){return;}
        //进入关闭流程之后定时器的语义已经从"空闲超时"变成"关闭兜底期限",此时不能再因为"又完成了几个字节"就把它往后推:
        //否则对端只要每隔_timeout读走一点数据,一条已经在关闭的连接就能永远赖着不走,fd与缓冲始终不释放
        if(_state.load(std::memory_order_acquire)==State::DisConnecting){return;}
        _last_active.store(now_ns(),std::memory_order_release);
    }
public:
    //参数设置
    //设置待发送数据的上限(默认10MB),超过后本次send会被拒绝并关闭连接
    void set_max_pending_send_size(size_t size)
    {
        if(size<4096){size=4096;}
        _max_pending_send.store(size,std::memory_order_release);
    }
    //修改超时时间并按新值立即重新挂表
    //refresh_timeout()只更新时间戳(惰性判定),不会改变已经挂好的到期时间,所以这里必须显式取消旧表项再重新挂一次
    void set_timeout(std::chrono::seconds timeout)
    {
        const uint32_t seconds=static_cast<uint32_t>(timeout.count());
        if(seconds==0){return;}
        //同enable_timed_release:关闭中的连接不能再往(可能已析构的)IoUring上挂定时器
        if(_closing.load(std::memory_order_acquire)){return;}
        //原实现在没有挂表时直接return,于是set_timeout()看起来配了超时其实完全没生效;改成没挂表就按新值挂上,已挂表才走"取消旧的再重挂"
        if(!_timed_release.load(std::memory_order_acquire))
        {
            enable_timed_release(timeout);
            return;
        }
        std::lock_guard<std::mutex> lock(_timer_mtx);
        _timeout.store(seconds,std::memory_order_release);
        _last_active.store(now_ns(),std::memory_order_release);
        ++_timer_gen;//换链:旧链上还没跑的回调按代次自检退出
        const uint64_t gen=_timer_gen;
        _uring->cancel_timer(_timer_id.load(std::memory_order_acquire));
        auto self=shared_from_this();
        //时间轮槽位有限,挂表周期由add_timer统一钳制(见IoUring::add_timer):否则大于等于60秒的超时会挂不上并彻底关掉该连接的超时
        uint64_t id=_uring->add_timer(timeout,[self,gen](){self->on_timeout_check(gen);});
        if(id==0)
        {
            ERROR("cannot re-arm the idle timer of conn {} with timeout={}s: the timer wheel rejected it",_id,seconds);
            _timed_release.store(false,std::memory_order_release);
            _timer_id.store(0,std::memory_order_release);
            return;
        }
        _timer_id.store(id,std::memory_order_release);
    }
    //注册空闲超时回调
    void set_timeout_callback(std::function<void()> cb)
    {
        std::lock_guard<std::mutex> lock(_timer_mtx);
        _timeout_callback=std::move(cb);
    }
    //注册/清除"发送缓冲区已排空"回调:流式发送大文件时读盘远快于网卡发送,必须在待发数据堆积过多时停下来等排空
    void set_send_drained_callback(std::function<void()> cb)
    {
        std::lock_guard<std::mutex> lock(_send_mtx);
        _send_drained_callback=std::move(cb);
    }
    //同一次加锁里完成"判断待发量是否超过高水位+注册排空回调"
    //分开写的话(先判断完先释放锁,再调set_send_drained_callback())两步之间如果本次发送正好完成、缓冲区排空,on_send_complete拿到的还是旧回调,新注册的回调永远不会被触发,流式发送会卡死在中途直到客户端超时
    //返回true表示已经挂好回调,调用方应当停止推进等待回调;false表示还没到高水位可以继续
    bool arm_send_drained_callback(size_t high_water,std::function<void()> cb)
    {
        std::lock_guard<std::mutex> lock(_send_mtx);
        size_t pending=(_out_buffer?_out_buffer->readable_size():0)+(_pending_buffer?_pending_buffer->readable_size():0);
        if(pending<=high_water){return false;}
        _send_drained_callback=std::move(cb);
        return true;
    }
    void set_connected_callback(const ConnCallback& cb){assign_callback("set_connected_callback",_connected_callback,cb);}
    void set_recv_callback(const RecvCallback& cb){assign_callback("set_recv_callback",_recv_callback,cb);}
    void set_close_callback(const ConnCallback& cb){assign_callback("set_close_callback",_close_callback,cb);}
    void set_clear_callback(const ConnCallback& cb){assign_callback("set_clear_callback",_clear_callback,cb);}
    //任意类型上下文:由业务代码自己存取。这里加锁只是为了让"读/写std::any本身"不是数据竞争,get_context_ptr()返回的指针仍然只在下一次set_context之前有效
    void set_context(std::any context)
    {
        std::unique_lock<std::shared_mutex> lock(_context_mtx);
        _context=std::move(context);
    }
    template<typename T>
    void set_context_value(T&& value)
    {
        std::unique_lock<std::shared_mutex> lock(_context_mtx);
        _context=std::forward<T>(value);
    }
public:
    //观测
    int get_fd()const{return _fd.load(std::memory_order_acquire);}
    bool is_connected()const{return _state.load(std::memory_order_acquire)==State::Connected;}
    uint64_t get_id()const{return _id;}
    const std::string& get_ip()const{return _ip;}
    bool is_ipv6()const{return _ipv6;}//对端是否是IPv6连接
    uint16_t get_port()const{return _port;}
    //接收缓冲的只读视图。不能在recv回调之外把它当普通缓冲用:
    //这块内存正被内核异步填写,业务线程一旦对它Write/reserve,内核可能同时往同一块内存写,vector重分配之后内核甚至会写进已经释放的内存
    std::shared_ptr<const Buffer> get_in_buffer()const{return _in_buffer;}
    //待发送的数据总量,应用层可以用它做背压控制
    size_t pending_send_size()
    {
        std::lock_guard<std::mutex> lock(_send_mtx);
        return (_out_buffer?_out_buffer->readable_size():0)+(_pending_buffer?_pending_buffer->readable_size():0);
    }
    IoUring* get_uring(){return _uring;}
    //累计已经交给发送子系统的字节数
    uint64_t bytes_queued()const{return _bytes_queued.load(std::memory_order_relaxed);}
    //零拷贝直接写socket的部分不经过send(),由调用方补记
    void add_bytes_queued(uint64_t len){_bytes_queued.fetch_add(len,std::memory_order_relaxed);}
    size_t get_max_pending_send_size()const{return _max_pending_send.load(std::memory_order_acquire);}
    //生命周期回调一旦冻结(establish之后)就不再接受修改,避免数据竞争
    bool callbacks_frozen()const{return _callbacks_frozen.load(std::memory_order_acquire);}
    std::any get_context()const
    {
        std::shared_lock<std::shared_mutex> lock(_context_mtx);
        return _context;
    }
    template<typename T>
    T* get_context_ptr()
    {
        std::shared_lock<std::shared_mutex> lock(_context_mtx);
        return std::any_cast<T>(&_context);
    }
    template<typename T>
    const T* get_context_ptr()const
    {
        std::shared_lock<std::shared_mutex> lock(_context_mtx);
        return std::any_cast<T>(&_context);
    }
private:
    //io_uring完成回调(只允许在拥有该连接的ring线程上执行)
    void on_recv_complete(IoTask&,ssize_t size)
    {
        if(size<=0){shutdown();return;}
        refresh_timeout();
        if(_in_buffer->readable_size()>0&&_recv_callback)
        {
            try{
                _recv_callback(shared_from_this(),_in_buffer);
            }catch(const std::exception& e){
                ERROR("recv callback of conn {} threw an exception: {}, closing the connection",_id,e.what());
                shutdown();
                return;
            }catch(...){
                ERROR("recv callback of conn {} threw an unexpected exception, closing the connection",_id);
                shutdown();
                return;
            }
        }
        //再次设置读事件,保证所有数据接收完成
        if(_state.load(std::memory_order_acquire)==State::Connected)
        {
            //重新提交读之前准备缓冲:1.上次把缓冲读满说明对端一次发的数据更多,按需扩容以减少读回调次数;2.必须保证有可写空间,否则recv长度为0,内核会立刻返回0并被误判成对端关闭
            if(static_cast<size_t>(size)>=_in_buffer->capacity()&&_in_buffer->capacity()<MAX_RECV_BUFFER_SIZE)
            {
                size_t want=_in_buffer->capacity()*2;
                if(want>MAX_RECV_BUFFER_SIZE){want=MAX_RECV_BUFFER_SIZE;}
                //扩容抛bad_alloc绝不能让它穿出本函数:下面的读不会再重提交,这条连接没有任何在途recv,
                //若又没启用空闲超时,fd/缓冲/连接表项就永久残留(僵尸连接)
                try{
                    _in_buffer->reserve(want);//回调已经读完数据,扩容不会影响内核手里的指针
                }catch(const std::exception& e){
                    ERROR("cannot grow the input buffer of conn {} to {} bytes: {}, closing the connection",_id,want,e.what());
                    shutdown();
                    return;
                }
            }
            //有可写空间时直接提交读;空间用完时先尝试按当前容量重排(把已消费的部分挪到头部),失败说明这条连接的接收缓冲到了上限
            if(_in_buffer->writable_size()==0)
            {
                try{
                    _in_buffer->reserve_for(_in_buffer->capacity());
                }catch(const std::exception& e){
                    ERROR("input buffer of conn {} reached its limit of {} bytes: {}, closing the connection",_id,MAX_RECV_BUFFER_SIZE,e.what());
                    shutdown();
                    return;
                }
                if(_in_buffer->writable_size()==0)
                {
                    ERROR("input buffer of conn {} is still full after growing to {} bytes, closing the connection",_id,_in_buffer->capacity());
                    shutdown();
                    return;
                }
            }
            //扩容可能把容量翻到最大64KB且从不回收,大量长连接会一直占着大缓冲,所以这里在回调返回后缩容
            //必须放在扩容之后、且只在"最近一次读取远小于容量"时做,否则刚缩容就会被下一次大读取按同样条件重新扩回去(等于白做);此时没有在途recv,搬移/缩容不会影响内核手里的指针
            size_t cap=_in_buffer->capacity();
            if(cap>BUFFER_DEFAULT_CAPACITY*4&&static_cast<size_t>(size)*4<cap&&_in_buffer->readable_size()*4<cap){_in_buffer->shrink();}
            auto self=shared_from_this();
            bool submitted=false;
            int recv_fd=-1;
            {
                //同上:下一次recv也要在_send_mtx内读fd并提交,否则与并发的force_close()之间仍有fd复用窗口
                std::lock_guard<std::mutex> lock(_send_mtx);
                recv_fd=_fd.load(std::memory_order_acquire);
                if(recv_fd>=0){submitted=_uring->async_recv(recv_fd,_in_buffer,[self](IoTask& task,ssize_t size){self->on_recv_complete(task,size);});}
            }
            if(!submitted)
            {
                ERROR("cannot submit the next recv for conn {}: fd={}, closing the connection",_id,recv_fd);
                shutdown();
            }
        }
    }
    void on_send_complete(IoTask&,ssize_t size)
    {
        if(size<=0)
        {
            //发送失败或者对端已经关闭,剩余数据已经没有意义,强制关闭,否则_sending会一直为真,连接永远不会被关闭
            abort_close();
            return;
        }
        refresh_timeout();
        bool failed=false;
        bool need_close=false;
        size_t queued_left=0;
        std::function<void()> drained;
        {
            std::lock_guard<std::mutex> lock(_send_mtx);
            //把发送期间新产生的数据并入发送缓冲区
            if(_pending_buffer&&_pending_buffer->readable_size()>0)
            {
                try{
                    if(!_out_buffer){_out_buffer=acquire_send_buffer();}
                    _out_buffer->append(_pending_buffer->read_ptr(),_pending_buffer->readable_size());
                    _pending_buffer->reset();
                }catch(const std::exception& e){
                    ERROR("cannot merge the pending send buffer of conn {}: {}, closing the connection",_id,e.what());
                    failed=true;
                }
            }
            if(!failed&&_out_buffer&&_out_buffer->readable_size()>0)
            {
                if(submit_send()){return;}
                failed=true;
            }
            _sending.store(false,std::memory_order_release);
            queued_left=(_out_buffer?_out_buffer->readable_size():0)+(_pending_buffer?_pending_buffer->readable_size():0);//锁内取快照:锁外读_out_buffer会和send()替换它构成shared_ptr数据竞争
            need_close=(_state.load(std::memory_order_acquire)==State::DisConnecting);
            //缓冲区彻底空了,通知等待背压的一方
            if(!need_close&&(!_out_buffer||_out_buffer->readable_size()==0)&&(_pending_buffer==nullptr||_pending_buffer->readable_size()==0))
            {
                drained=std::move(_send_drained_callback);
                _send_drained_callback=nullptr;
            }
        }
        if(failed)
        {
            ERROR("cannot submit a send for conn {}: fd={}, queued={} bytes, closing the connection",_id,_fd.load(std::memory_order_acquire),queued_left);
            if(_state.load(std::memory_order_acquire)==State::DisConnecting){close_now();}
            else{shutdown();}
            return;
        }
        if(drained){drained();}
        if(need_close){close_now();}
    }
    void on_close_complete(IoTask&,ssize_t){on_close_complete();}
    void on_close_complete()
    {
        int fd=_fd.exchange(-1,std::memory_order_acq_rel);
        if(fd>=0)
        {
            disable_timed_release();
            //连接关闭时也要唤醒还在等"发送排空"的一方,否则流式发送的业务逻辑会永远挂在这个回调上,直到客户端超时
            std::function<void()> drained;
            {
                std::lock_guard<std::mutex> lock(_send_mtx);
                drained=std::move(_send_drained_callback);
                _send_drained_callback=nullptr;
            }
            //用户回调绝不能把异常放出去:clear回调负责把连接从服务器表里移除,它抛了连接就残留在表里( drained 也一并丢失)
            try{
                if(_close_callback){_close_callback(shared_from_this());}
            }catch(const std::exception& e){ERROR("close callback of conn {} threw an exception: {}",_id,e.what());}
            catch(...){ERROR("close callback of conn {} threw an unexpected exception",_id);}
            try{
                if(_clear_callback){_clear_callback(shared_from_this());}
            }catch(const std::exception& e){ERROR("clear callback of conn {} threw an exception: {}",_id,e.what());}
            catch(...){ERROR("clear callback of conn {} threw an unexpected exception",_id);}
            try{
                if(drained){drained();}
            }catch(const std::exception& e){ERROR("send-drained callback of conn {} threw an exception: {}",_id,e.what());}
            catch(...){ERROR("send-drained callback of conn {} threw an unexpected exception",_id);}
        }
    }
private:
    //定时器与关闭
    //单调时钟的纳秒值,用于空闲超时判定
    static int64_t now_ns(){return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
    //空闲定时器到期:真正空闲超过_timeout才关闭,否则按剩余时间重新挂表
    //gen是这条定时器链的代次:定时器从时间轮摘下到回调真正执行之间有一个窗口,这期间disable_timed_release的cancel_timer
    //已经找不到它(不在轮上了),回调照跑不误——不校验代次的话,刚显式关掉超时的活连接会被误shutdown
    void on_timeout_check(uint64_t gen)
    {
        {
            std::lock_guard<std::mutex> lock(_timer_mtx);
            if(gen!=_timer_gen||!_timed_release.load(std::memory_order_acquire)){return;}//这条链已被换掉/拆掉,就地终止
        }
        uint32_t time=_timeout.load(std::memory_order_acquire);
        int64_t limit_ns=static_cast<int64_t>(time)*1000000000LL;
        int64_t idle_ns=now_ns()-_last_active.load(std::memory_order_acquire);
        if(idle_ns>=limit_ns)
        {
            //已经进入关闭流程却还停在这里,说明在途发送不可能再完成了
            //这时必须强制关闭(而不是再去调一次shutdown()),否则fd、发送/接收缓冲与连接表项会一直留着
            if(_state.load(std::memory_order_acquire)==State::DisConnecting)
            {
                WARN("conn {} has been closing for {} seconds with a send still in flight, forcing close",_id,time);
                abort_close();
                return;
            }
            std::function<void()> cb;
            {
                std::lock_guard<std::mutex> lock(_timer_mtx);
                if(gen!=_timer_gen||!_timed_release.load(std::memory_order_acquire)){return;}//等锁期间链被换掉:让新链自己管
                cb=_timeout_callback;
            }
            if(cb)
            {
                //用户接管了超时语义(例如HTTP回408):回调返回后若连接还活着就继续按剩余时间盯,
                //否则这条连接从此再也没有超时检查,fd与缓冲会一直留着
                try{cb();}
                catch(const std::exception& e){ERROR("timeout callback of conn {} threw an exception: {}",_id,e.what());}
                catch(...){ERROR("timeout callback of conn {} threw an unexpected exception",_id);}
                if(_state.load(std::memory_order_acquire)!=State::Connected){return;}//回调里关了,不用再挂
                idle_ns=now_ns()-_last_active.load(std::memory_order_acquire);//回调可能干过活,重新算剩余
            }
            else{shutdown();return;}
        }
        //idle_ns可能已经超过limit_ns(回调里耗了时间):负数直接转成uint32_t会回绕成天文数字,先按有符号算再钳到1秒
        const int64_t remain_s=(limit_ns-idle_ns+999999999LL)/1000000000LL;
        uint32_t remain=remain_s<1?1:static_cast<uint32_t>(remain_s);
        //挂表+登记必须在同一次_timer_mtx临界区里,先按代次确认这条链还活着:
        //锁外挂表的话,set_timeout可能抢先把_timer_id换成新链,这里再覆盖回去,新链就成了没人能取消的孤儿
        uint64_t id=0;
        {
            std::lock_guard<std::mutex> lock(_timer_mtx);
            if(gen!=_timer_gen||!_timed_release.load(std::memory_order_acquire)){return;}
            auto self=shared_from_this();
            id=_uring->add_timer(std::chrono::seconds(remain),[self,gen](){self->on_timeout_check(gen);});
            if(id!=0){_timer_id.store(id,std::memory_order_release);}
        }
        if(id==0)
        {
            ERROR("cannot re-arm the idle timer of conn {}: remaining={}s was rejected, closing the connection",_id,remain);
            shutdown();
            return;
        }
    }
    //把连接状态推进到DisConnecting,返回本次调用是否真正发起了关闭
    bool begin_close()
    {
        State expected=State::Connected;
        if(_state.compare_exchange_strong(expected,State::DisConnecting,std::memory_order_acq_rel,std::memory_order_acquire)){return true;}
        expected=State::Connectioning;
        return _state.compare_exchange_strong(expected,State::DisConnecting,std::memory_order_acq_rel,std::memory_order_acquire);
    }
    //强制关闭:丢弃待发数据直接关闭
    void abort_close()
    {
        //注意:这里不能只用begin_close()当门槛
        //shutdown()会先把状态置成DisConnecting,再因为_sending==true提前返回、把收尾托付给on_send_complete
        //如果这次send以错误完成(on_send_complete收到size<=0),就必须由这里接手关闭
        //否则状态已经是DisConnecting会让begin_close()失败,连接永远不会被关闭(fd/内存/连接表全部泄漏)
        if(!begin_close()&&_state.load(std::memory_order_acquire)!=State::DisConnecting){return;}
        disable_timed_release();
        _sending.store(false,std::memory_order_release);
        close_now();
    }
    //真正释放连接
    //关键点:只提交IORING_OP_CLOSE是不够的,挂起的recv会一直持有file引用,内核会等到该引用释放才销毁socket,导致对端收不到FIN、本端连接也迟迟不释放
    //所以这里先shutdown()半关闭,它会立刻发出FIN并唤醒挂起的recv
    void close_now()
    {
        //保证只有一个调用者真正执行关闭
        //否则shutdown()与on_send_complete()可能同时对同一个fd提交两次CLOSE,第一次关闭之后fd号会被系统复用,第二次关闭就会误伤别的连接
        if(_closing.exchange(true,std::memory_order_acq_rel)){return;}
        int fd=_fd.load(std::memory_order_acquire);
        if(fd<0){return;}
        ::shutdown(fd,SHUT_RDWR);
        auto self=shared_from_this();
        if(!_uring->async_close(fd,[self](IoTask& task,ssize_t size){self->on_close_complete(task,size);}))
        {
            ::close(fd);//提交失败时退回到同步关闭,避免fd泄漏
            on_close_complete();
        }
    }
private:
    //发送
    bool submit_send()
    {
        auto self=shared_from_this();
        return _uring->async_send(_fd,_out_buffer,[self](IoTask& task,ssize_t size){self->on_send_complete(task,size);});
    }
    //发送缓冲同样从池里借
    std::shared_ptr<Buffer> acquire_send_buffer(){return _buffer_pool?_buffer_pool->acquire():std::make_shared<Buffer>();}
private:
    //回调注册
    //生命周期回调一旦冻结(establish之后)就不再接受修改,避免数据竞争
    template<typename F>
    bool assign_callback(const char* setter,F& slot,const F& cb)
    {
        if(_callbacks_frozen.load(std::memory_order_acquire))
        {
            WARN("{} ignored: conn {} is already established and its callbacks are frozen",setter,_id);
            return false;
        }
        slot=cb;
        return true;
    }
};

//关于析构:对象的析构顺序是"派生类先、基类后",而worker线程是在基类析构(~TcpServer)里才被停掉的。
//所以如果派生类还有堆成员,而析构时还有存活连接,worker可能正在用户回调里用那些成员 —— 那时派生部分已经析构,就是use-after-free。
//正确做法(强制要求):在派生类析构函数的第一件事里调用 stop()。
//  ~MyServer(){ stop(); }        // 之后再让基类析构去收尾,此时worker已经停了
//stop()在worker回调里调用是安全的(worker上的wait()会推迟join),但在回调里直接delete本对象仍然不支持。
class TcpServer
{
public:
    enum class ServerState
    {
        Stopped,        //已停止(初始/结束)
        Starting,       //正在启动(初始化socket/uring/线程)
        Running,        //运行中(正常服务)
        Paused,         //暂停接收新连接
        Draining,       //停止接收新连接,等待在途请求结束(可以resume回来)
        Stopping,       //正在停止(优雅关闭连接,释放资源)
        Failed          //启动失败(异常)
    };
    using PtrConnection=std::shared_ptr<Connection>;
protected:
    //监听socket与worker
    Socket                        _socket;
    std::vector<IoUring::Ptr>     _urings;//IoUring::Ptr自带删除器
    std::atomic<ServerState>      _state{ServerState::Stopped};
    //start/stop/resume/pause/begin_drain之间没有锁:并发stop()的双join问题已由IoUring::wait()内部的_wait_mtx+_joined兜底(join只做一次),
    //并发stop()的收尾逻辑全部幂等(状态CAS/连接shutdown的begin_close门闩/连接表清理)。
    //pause()/resume()这类非幂等操作并发调用可能交错出错(例如resume途中被stop),调用方必须自行串行;
    //从worker回调里调stop()是安全的,也正是因为没有这把锁:回调阻塞在控制锁上会和stop()的join形成死锁。
    //连接表:_conns/_mutex_conns/_state/_buffer_pool保持protected,子类与回归用例会直接访问
    std::unordered_map<uint64_t,std::shared_ptr<Connection>>            _conns;
    mutable std::mutex                                                  _mutex_conns;
    std::condition_variable                                             _conns_empty_cond;
    std::shared_ptr<BufferPool>                                         _buffer_pool{std::make_shared<BufferPool>()};//Buffer对象池,必须使用shared_ptr<BufferPool>这样才能自动回收对象
    std::atomic_uint64_t                                                _next_id{0};
    //accept:每个ring一个"独占缓存行"的标志位,直接用std::atomic_bool[]会让几个ring的变量挤在同一缓存行里互相打无效(伪共享),用alignas隔开
    struct alignas(64) PaddedFlag{std::atomic_bool value{false};};
    struct alignas(64) PaddedCounter{std::atomic_int value{0};};
    std::unique_ptr<PaddedFlag[]>                                       _accept_inflight;//每个ring是否已经有一个在途的accept
    std::unique_ptr<PaddedCounter[]>                                    _accept_err_streak;//每个ring的accept连续失败次数(用于持续错误的退避)
    std::atomic_bool                                                    _accept_paused{false};//是否因为连接数达到上限而暂停accept
    std::atomic_bool                                                    _accept_retry_armed{false};//是否已经挂了accept重试定时器
    std::atomic_bool                                                    _accept_multishot{false};//多线程会破坏负载均衡默认关闭,单线程默认打开
    std::atomic_bool                                                    _accept_multishot_explicit{false};//用户是否显式配置过
    //正在析构:派生类先析构、基类析构里才会调stop(),此时vtable已经退化成基类,再做虚调用就是"pure virtual method called"直接abort
    std::atomic_bool                                                    _destroying{false};
    std::atomic<int>                                                    _rescue_fd{-1};//fd耗尽时用来救急的fd
    //连接与停机参数
    std::atomic_size_t                                                  _max_connections{0};//0表示不限制
    std::atomic_uint32_t                                                _connection_timeout{0};//新连接默认的空闲超时(秒),0表示不启用
    uint32_t                                                            _drain_timeout{5};//stop()等待连接排空的上限(秒)
    bool                                                                _keepalive{false};
    int                                                                 _keepalive_idle{60};
    int                                                                 _keepalive_interval{10};
    int                                                                 _keepalive_count{3};
protected:
    //子类覆盖的回调。故意不是纯虚(空实现而不是=0):
    //析构顺序是"派生类先、基类后",派生类析构一结束vptr就指回本类;worker线程若恰好先通过了_destroying检查再读vptr,
    //纯虚函数的vtable项就是__cxa_pure_virtual,直接abort(TSan实测能抓到这条vptr读写竞争)。
    //空实现把最坏情况退化成一次无害的空调用;_destroying检查负责拦住正常路径上的所有回调。
    virtual void on_connected(std::shared_ptr<Connection>){}
    virtual void on_recv(std::shared_ptr<Connection>,std::shared_ptr<Buffer>){}
    virtual void on_close(std::shared_ptr<Connection>){}
    //回调统一走这三个非虚转发,而不是直接虚调用:进入析构后(_destroying立起)把回调安全丢弃。
    //仍然建议在派生类析构函数里先主动调一次stop():那样连"回调打在已经析构的派生成员上"都不会发生
    void dispatch_connected(const std::shared_ptr<Connection>& conn)
    {
        if(_destroying.load(std::memory_order_acquire)){return;}
        on_connected(conn);
    }
    void dispatch_recv(const std::shared_ptr<Connection>& conn,std::shared_ptr<Buffer> buf)
    {
        if(_destroying.load(std::memory_order_acquire)){return;}
        on_recv(conn,std::move(buf));
    }
    void dispatch_close(const std::shared_ptr<Connection>& conn)
    {
        if(_destroying.load(std::memory_order_acquire)){return;}
        on_close(conn);
    }
public:
    TcpServer(const TcpServer&)=delete;
    TcpServer& operator=(const TcpServer&)=delete;
    TcpServer(TcpServer&&)=delete;
    TcpServer& operator=(TcpServer&&)=delete;
    explicit TcpServer(uint16_t port,const std::string& ip="0.0.0.0")
    :_socket(port,ip)
    {}
    ~TcpServer()
    {
        //必须先立起"正在析构"的旗子,再stop():stop()会关闭存活连接并等它们的完成事件,
        //那些回调正是在"派生类已经析构"的窗口里到达的
        _destroying.store(true,std::memory_order_release);
        (void)stop();
    }
public:
    //生命周期控制
    [[nodiscard]] bool start(size_t thread_num)
    {
        if(thread_num==0)
        {
            ERROR("tcp server start failed: threads must be greater than 0, got {}",thread_num);
            return false;
        }
        ServerState expected=ServerState::Stopped;
        if(!_state.compare_exchange_strong(expected,ServerState::Starting,std::memory_order_acq_rel,std::memory_order_acquire))
        {
            //Failed也允许重新启动:一次"端口被占用"之类的失败不该让这个对象永久不可用
            expected=ServerState::Failed;
            if(!_state.compare_exchange_strong(expected,ServerState::Starting,std::memory_order_acq_rel,std::memory_order_acquire))
            {
                ERROR("tcp server start ignored: state={}, expected stopped or failed",static_cast<int>(_state.load(std::memory_order_acquire)));
                return false;
            }
        }
        try{
            INFO("tcp server starting: listen={}:{}, threads={}, backlog={}",_socket.get_ip().c_str(),static_cast<unsigned>(_socket.get_port()),thread_num,_socket.get_backlog());
            if(!_socket.create_tcp_server(false))
            {
                //不检查返回值的话,创建/绑定失败会一路走到submit_accept:那里拿到fd=-1才报错,既多建了thread_num个ring与线程,日志也指向"不能提交accept"而不是"端口起不来"
                ERROR("cannot create the listen socket: listen={}:{}",_socket.get_ip().c_str(),static_cast<unsigned>(_socket.get_port()));
                _state.store(ServerState::Failed,std::memory_order_release);
                return false;
            }
            //创建thread_num个IoUring实例
            bool urings_ready=true;
            try{
                _urings.clear();
                _urings.reserve(thread_num);
                for(size_t i=0;i<thread_num;i++)
                {
                    _urings.emplace_back(IoUring::create());
                    _urings.back()->set_index(i);
                }
            }catch(const std::exception& e){
                ERROR("cannot create IoUring instances: count={}, error={}",thread_num,e.what());
                _urings.clear();
                urings_ready=false;
            }
            if(!urings_ready)
            {
                _state.store(ServerState::Failed,std::memory_order_release);
                _socket.close();
                return false;
            }
            //所有与worker共享的容器都必须在worker起来之前建好:worker一起来就会读_accept_inflight这个指针,之后再赋值就是对指针本身的数据竞争
            _accept_inflight=std::make_unique<PaddedFlag[]>(_urings.size());
            _accept_err_streak=std::make_unique<PaddedCounter[]>(_urings.size());
            _accept_paused.store(false,std::memory_order_release);
            _accept_retry_armed.store(false,std::memory_order_release);
            //没有显式配置过时按线程数取默认值:单线程开multishot,多线程关(避免破坏负载均衡)
            if(!_accept_multishot_explicit.load(std::memory_order_acquire))
            {
                _accept_multishot.store(_urings.size()==1,std::memory_order_release);
            }
            else if(_accept_multishot.load(std::memory_order_acquire)&&_urings.size()>1)
            {
                //显式打开要提醒:多ring上同时挂着multishot accept时新连接会集中到其中一个ring,accept路径退化成单线程。
                //本机两处独立实测一致:矩阵tcp-ms(8线程)2.58M rps -> 显式multishot 1.44M rps;
                //交错A/B(8线程/256连接/128B/P16)1.357M -> 0.779M rps=0.568倍(6/6轮全负,无重叠),1线程无差异(1.018)。
                //库不改用户的选择,但要把代价说出来(见PERF_REPORT_STEP2.md第3.7节)
                WARN("accept multishot is explicitly enabled with {} threads: new connections concentrate on one ring, expect the accept path to collapse to single-thread throughput (measured 0.57x at 8 threads); leave it off unless connections are few and long-lived",_urings.size());
            }
            open_rescue_fd();
            //启动所有worker线程,中途失败会回滚已经启动的部分
            bool workers_ready=true;
            if(_urings.empty())
            {
                ERROR("cannot start the IoUring workers: no IoUring instance exists, init_urings must run first");
                workers_ready=false;
            }
            else
            {
                try{
                    for(size_t i=0;i<_urings.size();i++)
                    {
                        if(!_urings[i]->start())
                        {
                            ERROR("cannot start the IoUring worker: index={}",i);
                            stop_urings(i);
                            workers_ready=false;
                            break;
                        }
                    }
                }catch(const std::exception& e){
                    ERROR("exception while starting the IoUring workers: {}",e.what());
                    stop_urings(_urings.size());
                    workers_ready=false;
                }catch(...){
                    ERROR("unexpected exception while starting the IoUring workers");
                    stop_urings(_urings.size());
                    workers_ready=false;
                }
            }
            if(!workers_ready)
            {
                _state.store(ServerState::Failed,std::memory_order_release);
                _socket.close();
                return false;
            }
            //必须先把状态发布成Running,再提交accept
            //worker线程早就在跑了,如果先提交后发布,已经排在accept队列里的客户端会立刻完成事件,此时on_accept_complete看到的状态还是Starting,会把连接关掉而且不再重提交,该ring就永久不再接受连接了
            _state.store(ServerState::Running,std::memory_order_release);
            for(size_t i=0;i<_urings.size();i++)
            {
                if(!submit_accept(i))
                {
                    ERROR("cannot submit the initial accept: index={}, listen_fd={}",i,_socket.get_fd());
                    //回滚顺序:先停ring再关socket,反过来worker可能拿着已关闭的fd号提交accept,fd号一旦被复用就会接到别人的socket上
                    stop_urings();
                    _socket.close();
                    _state.store(ServerState::Failed,std::memory_order_release);
                    return false;
                }
            }
            INFO("tcp server is running: listen={}:{}, threads={}, multishot_accept={}",_socket.get_ip().c_str(),static_cast<unsigned>(_socket.get_port()),_urings.size(),_accept_multishot.load(std::memory_order_acquire)?1:0);
            return true;
        }catch(const std::exception& e){
            ERROR("tcp server start failed: {}",e.what());
            stop_urings();
            _socket.close();
            _state.store(ServerState::Failed,std::memory_order_release);
            return false;
        }catch(...){
            ERROR("tcp server start failed with an unexpected exception");
            stop_urings();
            _socket.close();
            _state.store(ServerState::Failed,std::memory_order_release);
            return false;
        }
    }
    //停止接受新连接,但不动已有连接:进入Draining状态并关掉监听socket,这样在途的accept完成时不会再建立新连接,已有连接可以继续把请求跑完
    //与旧实现的区别:状态不再是Running,所以不会被"关闭监听fd仍然完成"的accept漏进来
    void begin_drain()
    {
        ServerState expected=ServerState::Running;
        if(!_state.compare_exchange_strong(expected,ServerState::Draining,std::memory_order_acq_rel,std::memory_order_acquire))
        {
            expected=ServerState::Paused;
            if(!_state.compare_exchange_strong(expected,ServerState::Draining,std::memory_order_acq_rel,std::memory_order_acquire)){return;}
        }
        //先取消所有挂起的accept:accept请求持有listen socket的file引用,只close(fd)的话socket不会真正销毁,端口会一直被占着,导致resume()无法重新bind
        int listen_fd=_socket.get_fd();
        for(size_t i=0;i<_urings.size();i++){_urings[i]->cancel_fd(listen_fd);}
        _socket.close();
        INFO("tcp server is draining: the listen socket is closed and existing connections keep running");
    }
    [[nodiscard]] bool stop()
    {
        ServerState expected=ServerState::Running;
        if(!_state.compare_exchange_strong(expected,ServerState::Stopping,std::memory_order_acq_rel,std::memory_order_acquire))
        {
            auto cur=_state.load(std::memory_order_acquire);
            if(cur==ServerState::Stopped||cur==ServerState::Failed){return false;}
            //Starting期间绝不能往下走:stop_urings()会并发遍历_urings,而start()正在往里emplace_back,两边不拿锁就是数据竞争。
            //而且start()的失败回滚自己会做完整清理,这里直接拒绝即可
            if(cur==ServerState::Starting)
            {
                WARN("tcp server stop() rejected: the server is still starting; wait for start() to finish first");
                return false;
            }
            _state.store(ServerState::Stopping,std::memory_order_release);
        }
        _socket.close();
        //救急fd不能在这里释放:worker可能刚好通过Running检查、随后才补开一个新的救急fd,而stop()不会再有机会释放它。
        //挪到stop_urings()之后,那时所有worker都已回收,不可能再有人补开
        std::vector<std::shared_ptr<Connection>> conns_copy;
        {
            std::lock_guard<std::mutex> lock(_mutex_conns);
            for(auto& kv:_conns){conns_copy.push_back(kv.second);}
        }
        for(auto& conn:conns_copy){conn->shutdown();}
        //等待连接全部清理(上限可配,见set_drain_timeout)
        //在worker线程的回调里调stop()时直接跳过:本worker正堵在这个回调里,它那条ring上连接的完成事件全部积压,
        //等下去必然吃满整个drain超时(实测约5秒)才走兜底强制关闭,纯属白等;业务线程调stop()才走正常排空
        if(IoUring::current()==nullptr)
        {
            std::unique_lock<std::mutex> lock(_mutex_conns);
            if(!_conns_empty_cond.wait_for(lock,std::chrono::seconds(_drain_timeout),[this]{return _conns.empty();}))
            {
                //这里已经持有_mutex_conns,不能再去调用connection_count()(它也会加同一把锁):一旦真的走到超时分支就会自己把自己锁死,stop()永远不返回。直接用_conns.size()
                WARN("stop timed out after {} seconds with {} connections still alive",_drain_timeout,_conns.size());
            }
        }
        stop_urings();
        release_rescue_fd();
        //收尾:强制关闭所有还留在表里的连接
        //异步close的完成回调可能永远不回来(例如ring已经停止),不强制关闭就是fd与内存泄漏
        {
            std::vector<std::shared_ptr<Connection>> remain;
            {
                std::lock_guard<std::mutex> lock(_mutex_conns);
                for(auto& kv:_conns){remain.push_back(kv.second);}
                _conns.clear();
            }
            for(auto& conn:remain){conn->force_close();}
            if(!remain.empty()){WARN("force-closed {} connections that were still alive at shutdown",remain.size());}
        }
        _state.store(ServerState::Stopped,std::memory_order_release);
        return true;
    }
    [[nodiscard]] bool resume()
    {
        auto cur=_state.load(std::memory_order_acquire);
        if(cur==ServerState::Draining)
        {
            //等所有被取消的accept真正完成(它们释放listen file引用后端口才会释放),有任何ring的accept还在途就继续等
            for(int wait=0;wait<100;wait++)
            {
                const bool any=_accept_inflight&&std::any_of(_accept_inflight.get(),_accept_inflight.get()+_urings.size(),[](const PaddedFlag& f){return f.value.load(std::memory_order_acquire);});
                if(!any){break;}
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            //begin_drain()关掉了监听socket,恢复时重新创建
            if(!_socket.create_tcp_server(false))
            {
                ERROR("cannot re-create the listen socket while resuming: address={}:{}",_socket.get_ip().c_str(),static_cast<unsigned>(_socket.get_port()));
                return false;
            }
            open_rescue_fd();
            _accept_paused.store(false,std::memory_order_release);
            _state.store(ServerState::Running,std::memory_order_release);
            for(size_t i=0;i<_urings.size();i++)
            {
                if(!submit_accept(i))
                {
                    ERROR("cannot re-submit the accept while resuming from draining: index={}, listen_fd={}",i,_socket.get_fd());
                    begin_drain();
                    return false;
                }
            }
            INFO("tcp server resumed: accepting connections again on {}:{}",_socket.get_ip().c_str(),static_cast<unsigned>(_socket.get_port()));
            return true;
        }
        ServerState expected=ServerState::Paused;
        if(!_state.compare_exchange_strong(expected,ServerState::Running,std::memory_order_acq_rel,std::memory_order_acquire)){return false;}
        _accept_paused.store(false,std::memory_order_release);
        for(size_t i=0;i<_urings.size();i++)
        {
            if(!submit_accept(i))
            {
                ERROR("cannot re-submit the accept while resuming from paused: index={}, listen_fd={}",i,_socket.get_fd());
                pause();
                return false;
            }
        }
        return true;
    }
    //暂停服务(可以resume回来)
    bool pause()
    {
        auto expected=ServerState::Running;
        if(!_state.compare_exchange_strong(expected,ServerState::Paused,std::memory_order_acq_rel,std::memory_order_acquire)){return false;}
        //必须把已经挂上的accept取消掉:multishot accept是"一次提交、持续完成"的,不取消的话暂停期间照样会把新连接收进来,
        //然后因为状态不是Running又立刻close——客户端表现成"刚连上就被断",还按新连接速率白烧fd与CPU。
        //监听fd本身不动(只取消它上面挂着的请求),连接会留在内核的backlog里,resume()之后正常建立
        const int listen_fd=_socket.get_fd();
        if(listen_fd>=0)
        {
            for(size_t i=0;i<_urings.size();i++){_urings[i]->cancel_fd(listen_fd);}
        }
        return true;
    }
    //当前状态
    ServerState state()const{return _state.load(std::memory_order_acquire);}
    //是否处于运行中
    bool is_running()const{return _state.load(std::memory_order_acquire)==ServerState::Running;}
public:
    //参数设置
    //设置listen backlog(必须在start()之前调用,默认SOMAXCONN)
    void set_backlog(int backlog){_socket.set_backlog(backlog);}
    //IPv6监听地址(如"::")是否只接受v6连接(默认true),关掉之后同一个socket也能收到v4-mapped地址的流量
    void set_v6only(bool enable){_socket.set_v6only(enable);}
    //设置监听socket的接收缓冲区大小(必须在start()之前调用)
    void set_listen_buffer_size(int size){_socket.set_recv_buffer_size(size);}
    //设置最大并发连接数(0表示不限制,必须在start()之前调用),达到上限后不再挂新的accept,已有连接关闭后再自动恢复
    void set_max_connections(size_t max_connections){_max_connections.store(max_connections,std::memory_order_release);}
    //设置新连接的空闲超时(秒),0表示不启用(必须在start()之前调用),到期后默认行为是关闭连接,可用Connection::set_timeout_callback区分"请求超时"与"连接空闲"
    void set_connection_timeout(uint32_t seconds){_connection_timeout.store(seconds,std::memory_order_release);}
    //设置TCP keepalive(必须在start()之前调用),idle:开始探测前的空闲秒数;interval:探测间隔;count:失败多少次后断开
    void set_keepalive(bool enable,int idle=60,int interval=10,int count=3)
    {
        _keepalive=enable;
        if(idle>0){_keepalive_idle=idle;}
        if(interval>0){_keepalive_interval=interval;}
        if(count>0){_keepalive_count=count;}
    }
    //是否启用multishot accept(必须在start()之前调用),多线程下默认为false(会破坏accept负载均衡),单线程下默认为true,连接建立密集(短连接)的负载可以显式打开,代价是连接会集中到某一个ring。
    //多线程显式打开有明确的代价:本机实测8线程吞吐掉到0.56~0.57倍(矩阵tcp-ms 2.58M->1.44M;交错A/B 1.357M->0.779M,6/6轮全负),start()时会打WARN提醒
    void set_accept_multishot(bool enable)
    {
        _accept_multishot.store(enable,std::memory_order_release);
        _accept_multishot_explicit.store(true,std::memory_order_release);
    }
    //优雅停机时等待连接排空的上限(秒)
    void set_drain_timeout(uint32_t seconds){if(seconds>0){_drain_timeout=seconds;}}
public:
    //观测
    //监听地址是否是IPv6(按构造时传入的IP字符串判定)
    bool is_ipv6()const{return _socket.is_ipv6();}
    size_t get_max_connections()const{return _max_connections.load(std::memory_order_acquire);}
    uint32_t get_connection_timeout()const{return _connection_timeout.load(std::memory_order_acquire);}
    bool get_accept_multishot()const{return _accept_multishot.load(std::memory_order_acquire);}
    //当前存活连接数(供指标/观测使用)
    size_t get_connection_count()const
    {
        std::lock_guard<std::mutex> lock(_mutex_conns);
        return _conns.size();
    }
private:
    //worker停机:等待前count个IoUring停掉,失败只记录不改变返回值
    void stop_urings(){stop_urings(_urings.size());}
    void stop_urings(size_t count)
    {
        for(size_t i=0;i<count&&i<_urings.size();i++)
        {
            try{
                (void)_urings[i]->wait();
            }catch(const std::exception& e){
                ERROR("error while waiting for an IoUring to stop during rollback: index={}, error={}",i,e.what());
            }catch(...){
                ERROR("unexpected error while waiting for an IoUring to stop during rollback: index={}",i);
            }
        }
    }
private:
    //accept:挂载、重试与恢复
    //fd耗尽时预留一个"救急fd":出问题就把它关掉,给accept腾出一个位置
    //open_rescue_fd()/release_rescue_fd()会被main线程(start/stop)与worker线程(EMFILE处理)同时调用,所以必须用原子+CAS:
    //否则两个线程可能各打开一次/dev/null,后写入的那个覆盖掉前一个fd号,那个fd就再也没人关,偏偏是在fd已经耗尽的场景下泄漏fd
    void open_rescue_fd()
    {
        if(_rescue_fd.load(std::memory_order_acquire)>=0){return;}
        int fd=::open("/dev/null",O_RDONLY|O_CLOEXEC);
        if(fd<0){return;}
        int expect=-1;
        if(!_rescue_fd.compare_exchange_strong(expect,fd,std::memory_order_acq_rel))
        {
            ::close(fd);//别的线程已经先开好了,本次多开的这个直接关掉
        }
    }
    void release_rescue_fd()
    {
        int fd=_rescue_fd.exchange(-1,std::memory_order_acq_rel);
        if(fd>=0){::close(fd);}
    }
    //给指定ring挂一个accept,返回false表示提交失败
    //连接数达到上限、连接已经暂停/停止、该ring已经有一个在途accept时都会直接返回true(不算失败)
    bool submit_accept(size_t idx)
    {
        if(idx>=_urings.size()){return false;}
        if(_state.load(std::memory_order_acquire)!=ServerState::Running){return true;}
        size_t max_conn=_max_connections.load(std::memory_order_acquire);
        if(max_conn>0)
        {
            //"连接数是否到顶"与"置暂停标志"必须在同一次_mutex_conns临界区里完成
            //分两段的话,两次之间有一条连接关闭(clear回调读到的还是"没暂停"从而不补挂),随后这里把暂停标志置上并直接返回,最终状态就是"已暂停、连接数没到顶、没有任何在途accept",而且再也不会有人来恢复它,服务器从此拒绝所有新连接
            bool full=false;
            {
                std::lock_guard<std::mutex> lock(_mutex_conns);
                full=_conns.size()>=max_conn;
                if(full){_accept_paused.store(true,std::memory_order_release);}
            }
            if(full){return true;}
        }
        if(!_accept_inflight){return true;}
        bool expected=false;
        if(!_accept_inflight[idx].value.compare_exchange_strong(expected,true,std::memory_order_acq_rel)){return true;}
        //提交accept同样要在_socket的管理锁内拿fd:与stop()/begin_drain()的Socket::close()互斥,
        //否则读完监听fd号之后它可能被关闭并被复用,这一发accept就挂到无关的socket上
        const bool submitted=_socket.with_fd([this,idx](int listen_fd){
            return listen_fd>=0&&_urings[idx]->async_accept(listen_fd,[this](IoTask& task,ssize_t res){on_accept_complete(task,res);},_accept_multishot.load(std::memory_order_acquire));
        });
        if(!submitted)
        {
            _accept_inflight[idx].value.store(false,std::memory_order_release);
            return false;
        }
        return true;
    }
    //连接数回落之后,重新给所有ring挂上accept
    void rearm_accepts()
    {
        if(_state.load(std::memory_order_acquire)!=ServerState::Running){return;}
        for(size_t i=0;i<_urings.size();i++){submit_accept(i);}
    }
    void on_accept_complete(IoTask& task,ssize_t client_fd)
    {
        //multishot accept在中间完成时请求仍然在途,_accept_inflight必须保持为真,只有最后一次完成(没有IORING_CQE_F_MORE)才能清标记
        const bool has_more=task._has_more;
        size_t idx=0;
        if(task._uring)
        {
            idx=task._uring->index();
            if(_accept_inflight&&idx<_urings.size()&&!has_more){_accept_inflight[idx].value.store(false,std::memory_order_release);}
        }
        if(client_fd<0)
        {
            int err=static_cast<int>(-client_fd);
            auto state=_state.load(std::memory_order_acquire);
            if(state!=ServerState::Running){return;}//正在暂停/排空/停止,不再重提交
            if((err==EINVAL||err==EOPNOTSUPP)&&_accept_multishot.load(std::memory_order_acquire))
            {
                //内核不支持multishot accept,降级成单次accept重试
                WARN("this kernel does not support multishot accept ({}): falling back to one accept per connection",errno_text(err).c_str());
                _accept_multishot.store(false,std::memory_order_release);
                submit_accept(idx);
                return;
            }
            if(err==EMFILE||err==ENFILE)
            {
                //fd耗尽:先释放救急fd,给accept腾位置,然后延迟1秒重试,避免日志风暴与忙等
                release_rescue_fd();
                if(!_accept_retry_armed.exchange(true,std::memory_order_acq_rel))
                {
                    WARN("accept failed: error={}, retrying in 1 second",errno_text(err).c_str());
                    auto self=this;
                    if(task._uring->add_timer(TIMER_TICK,[self]()
                       {
                           self->_accept_retry_armed.store(false,std::memory_order_release);
                           if(self->_state.load(std::memory_order_acquire)==ServerState::Running)
                           {
                               //这里绝对不能再调open_rescue_fd():救急fd的意义是"关键时刻腾出一个fd号给accept用",
                               //刚在重试之前把它重新占上,accept就又会因为EMFILE失败,重试变成每秒一次的永久空转。救急fd只在accept真正成功之后补回(见下方成功分支)
                               //重试必须覆盖所有ring:fd耗尽时每个ring的accept都会以EMFILE结束,它们各自清掉了_accept_inflight又都拿不到重试名额,
                               //只重挂触发本次定时器的那个ring,其余ring就永久没有accept了(accept能力悄悄掉到1/N)
                               self->rearm_accepts();
                           }
                       })==0)
                    {
                        _accept_retry_armed.store(false,std::memory_order_release);
                        ERROR("cannot arm the accept retry timer: no further connection will be accepted");
                    }
                }
                return;
            }
            if(err==ECANCELED||err==EBADF||err==EINVAL)
            {
                //监听fd已经关闭(处于排空状态)或者请求被取消;如果此时监听fd又有效了(例如begin_drain之后又resume),必须把accept重新挂上
                if(_socket.get_fd()>=0){submit_accept(idx);}
                return;
            }
            ERROR("accept failed: listen_fd={}, error={}",_socket.get_fd(),errno_text(err).c_str());
            //持续性错误(EPERM/ENOBUFS之类)"立刻重挂->立刻失败"就是100%CPU的忙循环加日志风暴:连续失败3次后退避1秒再试
            const int streak=_accept_err_streak?_accept_err_streak[idx].value.fetch_add(1,std::memory_order_relaxed)+1:1;
            if(streak<=2)
            {
                submit_accept(idx);
                return;
            }
            _accept_err_streak[idx].value.store(0,std::memory_order_relaxed);
            WARN("accept keeps failing on ring {} ({} times in a row, last error={}), retrying in 1 second",idx,streak,errno_text(err).c_str());
            {
                auto self=this;
                if(_urings[idx]->add_timer(TIMER_TICK,[self,idx]()
                   {
                       if(self->_state.load(std::memory_order_acquire)==ServerState::Running){self->submit_accept(idx);}
                   })==0)
                {
                    ERROR("cannot arm the accept error retry timer on ring {}: that ring stops accepting",idx);
                }
            }
            return;
        }
        //成功accept
        if(_accept_err_streak){_accept_err_streak[idx].value.store(0,std::memory_order_relaxed);}
        std::shared_ptr<Connection> conn;
        try{
            if(_state.load(std::memory_order_acquire)!=ServerState::Running)
            {
                ::close(client_fd);
                return;
            }
            //服务确实在接收新连接时才补回救急fd
            //放在状态检查之前的话,一次正在收尾的stop()(已经release_rescue_fd并进入Stopped)可能被这里重新打开一个/dev/null,而stop()不会再有机会释放它,这个fd就漏了
            open_rescue_fd();
            //每个连接都打一条INFO日志在高并发下会明显拖慢accept速率,降级为DEBUG
            DEBUG("accepted a tcp connection: fd={}",client_fd);
            //创建连接对象
            uint64_t conn_id=_next_id.fetch_add(1,std::memory_order_relaxed);
            conn=std::make_shared<Connection>(task._uring,client_fd,conn_id,_buffer_pool);
            //按服务器配置给新连接套上keepalive
            //keepalive用来发现"对端主机断电/网络中断"这种既没有FIN也没有RST的假死连接
            if(_keepalive)
            {
                int on=1;
                if(setsockopt(client_fd,SOL_SOCKET,SO_KEEPALIVE,&on,sizeof(on))<0)
                {
                    WARN("cannot enable TCP keepalive on fd {}: {}",client_fd,errno_text(errno).c_str());
                }
                else
                {
                    if(setsockopt(client_fd,IPPROTO_TCP,TCP_KEEPIDLE,&_keepalive_idle,sizeof(_keepalive_idle))<0)
                    {
                        WARN("cannot set TCP_KEEPIDLE to {} seconds on fd {}: {}",_keepalive_idle,client_fd,errno_text(errno).c_str());
                    }
                    if(setsockopt(client_fd,IPPROTO_TCP,TCP_KEEPINTVL,&_keepalive_interval,sizeof(_keepalive_interval))<0)
                    {
                        WARN("cannot set TCP_KEEPINTVL to {} seconds on fd {}: {}",_keepalive_interval,client_fd,errno_text(errno).c_str());
                    }
                    if(setsockopt(client_fd,IPPROTO_TCP,TCP_KEEPCNT,&_keepalive_count,sizeof(_keepalive_count))<0)
                    {
                        WARN("cannot set TCP_KEEPCNT to {} probes on fd {}: {}",_keepalive_count,client_fd,errno_text(errno).c_str());
                    }
                }
            }
            //注册回调
            conn->set_connected_callback([this](std::shared_ptr<Connection> conn){dispatch_connected(conn);});
            conn->set_recv_callback([this](std::shared_ptr<Connection> conn,std::shared_ptr<Buffer> buffer){dispatch_recv(conn,std::move(buffer));});
            conn->set_close_callback([this](std::shared_ptr<Connection> conn){dispatch_close(conn);});
            conn->set_clear_callback([this](std::shared_ptr<Connection> conn)
            {
                bool rearm=false;
                {
                    std::lock_guard<std::mutex> lock(_mutex_conns);
                    auto it=_conns.find(conn->get_id());
                    if(it!=_conns.end())
                    {
                        _conns.erase(it);
                    }
                    if(_conns.empty())
                    {
                        //可能有多个线程在等(Drain/stop),必须notify_all:notify_one会让其余的等满一个drain超时
                        _conns_empty_cond.notify_all();
                    }
                    if(_accept_paused.load(std::memory_order_acquire))
                    {
                        size_t max_conn=_max_connections.load(std::memory_order_acquire);
                        if(max_conn==0||_conns.size()<max_conn)
                        {
                            _accept_paused.store(false,std::memory_order_release);
                            rearm=true;
                        }
                    }
                }
                if(rearm){rearm_accepts();}
            });
            //插入连接表:stop()取快照和这里插入用的是同一把_mutex_conns,但上面那句Running检查没有锁保护
            //如果stop()正好在检查之后、插入之前完成快照,这条新连接就永远不会被排空,它的fd、缓冲与连接对象会一直留着(连接表里还被clear回调反过来引用)
            //所以在锁内再确认一次状态,状态已经变了就当场把这条连接关掉,不再往表里放
            //连接数上限检查和插入放在同一次加锁里
            //原来"先connection_count()判断上限,再加锁插入"是两段临界区:多个ring并发accept时会一起通过检查,连接数最多能超出(线程数-1)条;而且同一条accept路径要取两次_mutex_conns
            bool inserted=false;
            bool limit_hit=false;
            bool was_paused=false;
            {
                std::lock_guard<std::mutex> lock(_mutex_conns);
                size_t max_conn=_max_connections.load(std::memory_order_acquire);
                if(max_conn>0&&_conns.size()>=max_conn)
                {
                    limit_hit=true;
                    //"到顶"与"置暂停标志"必须在同一次_mutex_conns临界区里完成:
                    //分两段的话,中间有一条连接关闭时clear回调读到的还是"没暂停",不会补挂accept,随后暂停标志被置上、本ring也不再挂accept,服务器就永久停在"已暂停且连接数没到顶"
                    was_paused=_accept_paused.exchange(true,std::memory_order_acq_rel);
                }
                else if(_state.load(std::memory_order_acquire)==ServerState::Running)
                {
                    _conns.insert(std::make_pair(conn_id,conn));
                    inserted=true;
                }
            }
            if(limit_hit)
            {
                //达到上限:把多接受出来的这条连接直接关掉
                //accept是"完成之后才检查上限",不补这一刀的话并发下仍会超出
                DEBUG("connection limit reached: closing the newly accepted fd={}",client_fd);
                //只释放fd,不走force_close:这条连接从来没有establish()过,上层也没在on_connected里见过它,触发on_close回调会让"按on_connected/on_close配对计数"的应用逻辑错位
                conn->discard();
                if(!was_paused)
                {
                    //multishot accept会持续投递新连接,达到上限时必须把所有ring上的都取消掉:
                    //只取消当前ring的话,其余ring的在途multishot accept会继续"接受再丢弃",一直白烧fd与CPU直到有连接关闭
                    //cancel_fd是幂等的(没在途请求时就是一次空操作),单次accept被取消了也无妨——上限满时submit_accept本来就拒绝重挂
                    const int listen_fd=_socket.get_fd();
                    if(listen_fd>=0)
                    {
                        for(size_t i=0;i<_urings.size();i++){_urings[i]->cancel_fd(listen_fd);}
                    }
                }
                return;
            }
            if(!inserted)
            {
                DEBUG("the server is stopping, dropping the newly accepted fd={}",client_fd);
                //同上:这条连接还没有establish(),不能用会自动触发on_close的force_close
                conn->discard();
                return;
            }
            //建立连接
            conn->establish();
            //套用服务器默认的空闲超时
            //放在establish()之后:on_connected里若自己调过enable_timed_release,会优先生效(enable_timed_release对已挂表的连接是空操作)
            const uint32_t conn_timeout=_connection_timeout.load(std::memory_order_acquire);
            if(conn_timeout>0&&conn->is_connected()){conn->enable_timed_release(std::chrono::seconds(conn_timeout));}
        }catch(const std::exception& e){
            ERROR("unexpected exception while handling an accepted connection: fd={}, error={}",client_fd,e.what());
            if(conn){conn->discard();}
            else if(client_fd>=0){::close(client_fd);}
        }catch(...){
            ERROR("unexpected exception while handling an accepted connection: fd={}",client_fd);
            if(conn){conn->discard();}
            else if(client_fd>=0){::close(client_fd);}
        }
        //multishot accept仍在在途时不需要重新提交,内核会继续投递
        if(!has_more&&!submit_accept(idx))
        {
            //运行期重挂失败(例如SQ暂时满了):不处理的话这个ring永久失去accept能力——没有任何在途accept,
            //只能靠"有连接关闭时rearm"偶然恢复,空载时永远恢复不了。退到1秒后重试
            ERROR("cannot re-submit accept on ring {}: retrying in 1 second",idx);
            auto self=this;
            if(_urings[idx]->add_timer(TIMER_TICK,[self,idx]()
               {
                   if(self->_state.load(std::memory_order_acquire)==ServerState::Running){self->submit_accept(idx);}
               })==0)
            {
                ERROR("cannot arm the accept retry timer on ring {}: that ring stops accepting",idx);
            }
        }
    }
};
