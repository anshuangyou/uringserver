#pragma once
#include <vector>
#include <string>
#include <cstring>
#include <cstdlib>
#include <memory>
#include <functional>
#include <system_error>
#include <stdexcept>
#include <atomic>
#include <mutex>
#include <thread>
#include <liburing.h>
#include <climits>
#include <cstdint>
#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <format>
#include <optional>
#include <span>
#include <sys/socket.h>
#include <poll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <unordered_map>
#include <list>
#include <latch>
#include "log.hpp"
#include "buffer.hpp"
using namespace std::chrono_literals;
//时间轮槽数(同时也是"一次最多能挂多少秒")与默认连接空闲超时。
constexpr uint32_t TIMING_WHEEL_SLOTS=60;
constexpr std::chrono::seconds TIMEOUT_SECONDS{30};
//时间轮的精度就是1秒:定时器的实际触发时间在[delay-1s,delay+1s]之间(取决于挂进来时秒针的相位),
//绝不早于"上一圈"、也不会晚于delay+1s。所有超时类语义(连接空闲、排空、accept重试)都建立在这个粒度上,
//要更细就得加槽数或换最小堆,当前设计选了"每秒只唤醒一次"的低开销
constexpr std::chrono::seconds TIMER_TICK{1};
constexpr size_t URING_ENTRIES = 32768;
constexpr size_t CQE_BATCH = 128;
//UDP单个数据报的最大负载(IPv4下为65507字节,这里留出余量)
constexpr size_t UDP_RECV_BUFFER_SIZE = 65536;
//UDP内核socket缓冲区请求值(会被net.core.rmem_max/wmem_max截断)
constexpr int UDP_SOCKET_BUFFER_SIZE = 4*1024*1024;
//TCP接收缓冲区自适应扩容的上限
//缓冲区默认只有1KB,大于1KB的报文会被拆成多次recv回调,次数多了会明显拉低吞吐,
//所以一次性读满缓冲区时就把缓冲区翻倍,上限取64KB以兼顾高并发下的内存占用
constexpr size_t MAX_RECV_BUFFER_SIZE = 64*1024;
//常量之间的隐含关系写成编译期契约(改错了直接编不过,而不是运行期才发现)
static_assert(std::has_single_bit(URING_ENTRIES),"io_uring entry count must be a power of two");
static_assert(URING_ENTRIES>=1024,"io_uring entry count is too small to be useful");
static_assert(UDP_RECV_BUFFER_SIZE>=64*1024,"UDP receive buffer must hold the largest datagram plus the recvmsg header");

//v4/v6地址与sockaddr_storage之间的转换
//统一用storage保存之后,IoTask与内核交互时不必区分地址族。
//这些类型都必须可以按位搬(内核的地址结构都是POD),写成编译期契约:
//否则下面的memcpy/bit_cast就不再成立,而且是"改了结构体定义以后才炸"的隐蔽问题。
static_assert(std::is_trivially_copyable_v<sockaddr_in>&&std::is_trivially_copyable_v<sockaddr_in6>,"socket address types must stay trivially copyable");
static_assert(sizeof(sockaddr_storage)>=sizeof(sockaddr_in6),"sockaddr_storage must hold an IPv6 address");

//把storage里的内容按位读成T(调用方必须先确认地址族)
template<typename T>
inline T storage_as(const sockaddr_storage& src)noexcept
{
    static_assert(std::is_trivially_copyable_v<T>,"storage_as requires a trivially copyable type");
    static_assert(sizeof(T)<=sizeof(sockaddr_storage),"storage_as target must fit in sockaddr_storage");
    T out{};
    std::memcpy(&out,&src,sizeof(out));
    return out;
}
inline socklen_t sockaddr_to_storage(const sockaddr_in& src,sockaddr_storage& dst)
{
    dst=sockaddr_storage{};
    std::memcpy(&dst,&src,sizeof(src));
    return sizeof(sockaddr_in);
}
inline socklen_t sockaddr_to_storage(const sockaddr_in6& src,sockaddr_storage& dst)
{
    dst=sockaddr_storage{};
    std::memcpy(&dst,&src,sizeof(src));
    return sizeof(sockaddr_in6);
}
inline std::optional<sockaddr_in> storage_to_sockaddr_in(const sockaddr_storage& src)noexcept
{
    if(src.ss_family!=AF_INET){return std::nullopt;}
    return storage_as<sockaddr_in>(src);
}
inline std::optional<sockaddr_in6> storage_to_sockaddr_in6(const sockaddr_storage& src)noexcept
{
    if(src.ss_family!=AF_INET6){return std::nullopt;}
    return storage_as<sockaddr_in6>(src);
}
//把storage里的地址格式化成可读字符串
inline std::string storage_to_ip_string(const sockaddr_storage& src)
{
    char buf[INET6_ADDRSTRLEN]={0};
    const void* addr=nullptr;
    sockaddr_in6 v6{};
    sockaddr_in  v4{};
    if(src.ss_family==AF_INET6){v6=storage_as<sockaddr_in6>(src);addr=&v6.sin6_addr;}
    else if(src.ss_family==AF_INET){v4=storage_as<sockaddr_in>(src);addr=&v4.sin_addr;}
    else{return "";}
    if(inet_ntop(src.ss_family,addr,buf,sizeof(buf))==nullptr){return "";}
    return buf;
}
//取storage里的端口(主机字节序)
inline uint16_t storage_to_port(const sockaddr_storage& src)
{
    if(src.ss_family==AF_INET6){return ntohs(storage_as<sockaddr_in6>(src).sin6_port);}
    if(src.ss_family==AF_INET){return ntohs(storage_as<sockaddr_in>(src).sin_port);}
    return 0;
}

//IoTask的原始内存池
//每个网络事件(recv/send/accept/close/poll/post)都要new/delete一个IoTask,
//这是整条链路上最热的堆分配;池化之后只剩"取一个指针/还一个指针"
//关键:必须用线程本地缓存而不是全局锁。
//recv/send这类任务通常是同一个worker线程自己分配、自己释放,线程本地缓存完全没有锁竞争;
//如果用一把全局锁,4个worker线程每次收发都要抢同一把锁,实测反而比直接malloc慢约10%
//IoTask带alignas(64),所以对齐版与非对齐版的operator new/delete都要接管,避免配对错乱
struct TlsTaskCache
{
    //固定容量:池子的上限本来就是编译期常量,用数组之后"缓存本身"也不再需要堆分配,
    //内存上限一眼可见(每线程CAPACITY*8字节)
    static constexpr size_t CAPACITY=512;
    std::array<void*,CAPACITY> free_blocks{};
    size_t count{0};
    ~TlsTaskCache()
    {
        for(size_t i=0;i<count;i++){::operator delete(free_blocks[i],std::align_val_t(64));}
    }
};
inline TlsTaskCache& tls_task_cache()
{
    static thread_local TlsTaskCache cache;
    return cache;
}
class IoTaskPool
{
public:
    void* acquire(size_t size)
    {
        auto& cache=tls_task_cache();
        if(cache.count>0){return cache.free_blocks[--cache.count];}
        return ::operator new(size,std::align_val_t(64));
    }
    void release(void* p)noexcept
    {
        if(!p){return;}
        auto& cache=tls_task_cache();
        if(cache.count<TlsTaskCache::CAPACITY)
        {
            cache.free_blocks[cache.count++]=p;
            return;
        }
        ::operator delete(p,std::align_val_t(64));
    }
};
inline IoTaskPool& iotask_pool()
{
    static IoTaskPool pool;
    return pool;
}

class IoUring;
struct alignas(64) IoTask
{
public:
    //接管IoTask的堆分配:从池里取/还原始内存
    static void* operator new(size_t size){return iotask_pool().acquire(size);}
    static void operator delete(void* p)noexcept{iotask_pool().release(p);}
    static void* operator new(size_t size,std::align_val_t){return iotask_pool().acquire(size);}
    static void operator delete(void* p,std::align_val_t)noexcept{iotask_pool().release(p);}
public:
    enum class IoEvent
    {
        ACCEPT,
        READ,
        WRITE,
        CLOSE,
        RECV,
        SEND,
        RECVMSG,
        SENDMSG,
        SPLICE,
        TIMEOUT,
        POST,       //把函数投递到worker线程执行(空操作+回调)
        POLL,       //等待fd可读/可写(poll_add)
    };
public:
    using Callback = std::function<void(IoTask&,ssize_t res)>;
    IoUring*                                        _uring{nullptr};
    int                                             _fd{-1};
    int                                             _fd_out{-1};
    int64_t                                         _fd_offset{-1};
    int64_t                                         _fd_out_offset{-1};
    size_t                                          _splice_len{0};
    IoEvent                                         _event;
    std::shared_ptr<Buffer>                         _buf{nullptr};
    sockaddr_storage                                _addr{};
    socklen_t                                       _addr_len{sizeof(sockaddr_storage)};
    iovec                                           _iov{};
    msghdr                                          _msg{};
    std::function<void(IoTask&,ssize_t res)>        _cb;
    uint32_t                                        _poll_mask{POLLIN};//POLL事件等待的掩码
    bool                                            _auto_buf=true;
    bool                                            _multishot{false};//是否是"一次提交、多次完成"的请求
    bool                                            _has_more{false};//本次完成之后内核是否还会继续投递完成事件(来自IORING_CQE_F_MORE)
    bool                                            _buffer_select{false};//是否使用provided buffer ring(缓冲由内核从环里挑)
    unsigned                                        _provided_bid{0};//本次完成使用的是哪个provided buffer(由CQE的flags解出)
    bool                                            _has_provided_buffer{false};//本次完成是否真的带回了缓冲(见IORING_CQE_F_BUFFER)
    std::atomic_bool                                _completed{false};
public:
    IoTask(IoUring* uring,int fd,IoEvent event,std::shared_ptr<Buffer> buf,Callback cb,bool auto_buf=true)
    :_uring(uring)
    ,_fd(fd)
    ,_event(event)
    ,_buf(std::move(buf))
    ,_cb(std::move(cb))
    ,_auto_buf(auto_buf)
    {
        memset(&_addr,0,sizeof(_addr));
        memset(&_iov,0,sizeof(_iov));
        memset(&_msg,0,sizeof(_msg));
    }
    //带目标/来源地址的构造(v4/v6通用,地址长度由调用方给出)
    IoTask(IoUring* uring,int fd,IoEvent event,std::shared_ptr<Buffer> buf,const sockaddr_storage& addr,socklen_t addr_len,Callback cb,bool is_recv=true,bool auto_buf=true)
    :_uring(uring)
    ,_fd(fd)
    ,_event(event)
    ,_buf(std::move(buf))
    ,_addr(addr)
    ,_addr_len(addr_len)
    ,_cb(std::move(cb))
    ,_auto_buf(auto_buf)
    {
        if(_buf)
        {
            if(is_recv)
            {
                _iov.iov_base=_buf->write_ptr();
                _iov.iov_len=_buf->writable_size();
            }
            else
            {
                _iov.iov_base=_buf->read_ptr();
                _iov.iov_len=_buf->readable_size();
            }
            _msg.msg_name=&_addr;
            _msg.msg_namelen=_addr_len;
            _msg.msg_iov=&_iov;
            _msg.msg_iovlen=1;
        }
    }
    IoTask(Callback cb)
    :_fd(-1)
    ,_event(IoEvent::TIMEOUT)
    , _buf(nullptr)
    ,_cb(std::move(cb))
    {
        memset(&_addr,0,sizeof(_addr));
        memset(&_iov,0,sizeof(_iov));
        memset(&_msg,0,sizeof(_msg));
    }
    IoTask(IoUring* uring,int fd_in,int64_t in_offset,int fd_out,int64_t out_offset,size_t len,Callback cb)
    :_uring(uring)
    ,_fd(fd_in)
    ,_fd_out(fd_out)
    ,_fd_offset(in_offset)
    ,_fd_out_offset(out_offset)
    ,_splice_len(len)
    ,_event(IoEvent::SPLICE)
    ,_cb(std::move(cb))
    {
        memset(&_addr,0,sizeof(_addr));
        memset(&_iov,0,sizeof(_iov));
        memset(&_msg,0,sizeof(_msg));
    }
    ~IoTask()=default;
    IoTask(const IoTask&)=delete;
    IoTask& operator=(const IoTask&)=delete;
    IoTask(IoTask&&)=delete;
    IoTask& operator=(IoTask&&)=delete;
};

class IoUring
{
public:
    enum class UringState
    {
        Stopped,     //已停止(初始/结束)
        Starting,    //正在启动(初始化uring、线程)
        Running,     //运行中(正常服务)
        Stopping,    //正在停止(已经请求退出,等待worker线程回收)
        Failed       //启动失败(异常)
    };
private:
    io_uring                                                    _ring{};
    mutable std::mutex                                          _submit_mtx;
    std::thread                                                 _worker_thread;
    mutable std::mutex                                          _wait_mtx;//wait()可能被多个线程同时调用,而std::thread::join只能有一个调用者
    bool                                                        _joined{false};//worker线程是否已经被回收过(_wait_mtx保护)
    std::atomic<uint64_t>                                       _worker_gen{0};//worker换代计数:start()每换一代就+1。wait()按代次认worker——等锁期间ring被并发重启的话,自己要等的那一代已经被start()里的wait()回收完毕,绝不能去join新一代worker(那次stop作用在旧代身上,新代没人通知停,join就是永久卡死)
    std::atomic_bool                                            _worker_exited{false};//worker线程是否已经跑到函数末尾(与"是否已join"不同:worker退出后到被join之间joinable()仍为真)
    std::atomic_bool                                            _stop_requested{false};//请求worker线程退出
    std::atomic<UringState>                                     _state{UringState::Stopped};
    size_t                                                      _index{0};//在所属服务器中的下标,便于把回调定位回自己的socket
    std::shared_ptr<std::latch>                                 _start_latch;//当前这一代worker的启动握手闩锁(_wait_mtx保护)。
                                                                              //必须按代独立:用std::optional<latch>就地重建的话,
                                                                              //上一代worker的count_down会和新一代的emplace/reset撞在同一块存储上(TSan实测报data race)
    bool                                                        _leak_ring{false};//join失败时放弃queue_exit,宁可泄漏也不use after free
    bool                                                        _has_ext_arg{false};//内核是否支持IORING_FEAT_EXT_ARG(<5.11时wait_cqe_timeout会自己取SQE,见worker_loop)
    std::atomic_bool                                            _abandoned{false};//对象在worker线程里被判定自毁,由worker自己释放ring
    std::atomic<int64_t>                                        _pending_count{0};//已经提交、还没收到CQE的请求数
    //在途任务登记表:正开地址散列表,容量为2的幂,空槽为nullptr
    //唯一目的是"停机兜底":取消没能把所有请求收回来时,内核只通过user_data持有这些IoTask,
    //用户态再没有别的引用,找不到就是永久泄漏(IoTask + 它持有的Buffer + 捕获的shared_ptr<Connection>)。
    //插入与删除都在_submit_mtx之内完成,不额外加锁;表按并发量翻倍增长,稳态下不再分配
    std::unique_ptr<IoTask*[]>                                  _inflight_table;
    size_t                                                      _inflight_cap{0};
    size_t                                                      _inflight_used{0};
    size_t                                                      _inflight_untracked{0};//登记失败(表满/分配失败)的请求数
    std::atomic<size_t>                                         _forced_count{0};//被cleanup强制收尾的请求数
    //本批已经完成、等待统一注销并释放的任务(只由worker线程访问)
    std::vector<IoTask*>                                        _retired;
    //取消不回来、被强制收尾的任务:它们的请求可能仍在内核里(addr还指着它们的Buffer),
    //必须留到queue_exit(保证内核不再碰这个ring)之后再释放,否则就是"内核往已释放内存里写"。
    //已知残留(极窄):若同一个ring被重启,这类请求迟到的"中间完成"(F_MORE)可能再走一次multishot分支,
    //让回调多被调用一次(不会UAF、不会重复释放);在有取消能力的内核上forced恒为0,打不到这条路径
    std::vector<IoTask*>                                        _forced_retired;
    //退役的缓冲环内存块:与_forced_retired同理,内核可能还攥着里面的缓冲,统一推迟到queue_exit之后free
    std::vector<void*>                                          _retired_buf_ring_mem;
    //provided buffer ring(multishot recvmsg用,缓冲由内核从环里挑)
    struct io_uring_buf_ring*                                   _buf_ring{nullptr};
    void*                                                       _buf_ring_mem{nullptr};//一次性分配的连续缓冲内存
    std::vector<void*>                                          _buf_ring_addrs;//bid -> 缓冲地址
    unsigned                                                    _buf_ring_bgid{1};
    size_t                                                      _buf_ring_entries{0};
    size_t                                                      _buf_ring_buf_size{0};
    std::atomic_bool                                            _buf_ring_ready{false};
    std::atomic_bool                                            _buf_ring_status_supported{false};//内核是否支持PBUF_STATUS查询(6.8+)
    std::atomic_uint64_t                                        _buf_ring_recycled{0};//已归还给内核的缓冲块数(观测用)
    //当前线程正在为哪个ring跑worker循环
    //必须是thread_local:它描述的是"调用线程是不是本ring的worker",
    //如果做成普通成员,别的线程提交请求时也会被误判成worker,从而把SQE攒着不提交,
    //直到worker的1秒定时器把它唤醒 —— 表现为所有外部提交的延迟高达1秒
    static thread_local IoUring*                                tls_worker_ring;
private:
    struct TimerTask
    {
        uint64_t                                                _id;
        std::function<void(void)>                               _timed_callback;
    };
    uint32_t                                                    _capacity=TIMING_WHEEL_SLOTS;//时间轮容量(外层容器大小)(决定最大定时长度)
    std::atomic_uint64_t                                        _next_timertask_id{1};
    std::unordered_map<uint64_t,uint64_t>                       _id_map;//task_id:slot_idx
    std::vector<std::list<std::shared_ptr<TimerTask>>>          _slots;//时间轮
    std::atomic_uint32_t                                        _current_slot_id{0};//秒针(实际是容器下标)
    std::unique_ptr<IoTask>                                     _iotask_timer;
    std::atomic_bool                                            _timeout_armed{false};//1秒多shot定时器当前是否已经武装(防止重启后两份同时触发)
    std::mutex                                                  _timer_mtx;
    bool                                                        _ring_inited{false};
public:
    IoUring()
    {
        try{
            //初始化io_uring
            io_uring_params params{};
            int ret=io_uring_queue_init_params(URING_ENTRIES,&_ring,&params);
            if(ret<0)
            {
                _state.store(UringState::Stopped,std::memory_order_release);
                FATAL("io_uring_queue_init_params failed: requested_entries={}, error={}",URING_ENTRIES,errno_text(ret).c_str());
                throw std::system_error(-ret,std::generic_category(),"io_uring init failed");
            }
            _ring_inited=true;
            //老内核(没有EXT_ARG)上liburing的io_uring_wait_cqe_timeout会在内部临时取一个SQE当超时,
            //而worker取SQE是不持_submit_mtx的,会和并发提交撞sqe_tail —— 这种内核上worker改用阻塞等待
            _has_ext_arg=(_ring.features&IORING_FEAT_EXT_ARG)!=0;
            if(!_has_ext_arg){WARN("this kernel does not support IORING_FEAT_EXT_ARG: the worker will use a blocking wait without a wait timeout");}
            //初始化时间轮
            _slots.resize(_capacity);
            _iotask_timer=std::make_unique<IoTask>([this](IoTask&,ssize_t){this->tick();});
            INFO("io_uring queue created: entries={}",URING_ENTRIES);
        }catch(const std::exception&e){
            if(_ring_inited){io_uring_queue_exit(&_ring);}
            _state.store(UringState::Failed,std::memory_order_release);
            FATAL("IoUring construction failed: {}",e.what());
            throw;
        }
    }
    //析构函数私有:只允许通过destroy() 销毁
    //destroy() 会先确保worker线程已经退出(worker线程上则改为整块泄漏),所以这里可以放心释放成员
private:
    ~IoUring()
    {
        (void)wait();//析构无法处理失败:worker线程内自毁时join已经被推迟,见_abandoned
        //worker线程上wait()会拒绝join(见wait()),但此刻worker已经跑到末尾,detach是安全的:
        //不detach的话std::thread成员析构遇到joinable目标会直接terminate(线程已结束,不会再碰本对象)
        if(_worker_thread.joinable()&&_worker_exited.load(std::memory_order_acquire))
        {
            try{_worker_thread.detach();}catch(...){FATAL("cannot detach the finished IoUring worker thread: index={}",_index);}
        }
        teardown_recv_buffers();
        _inflight_table.reset();
        _inflight_cap=0;
        _inflight_used=0;
        if(_ring_inited&&!_leak_ring)
        {
            io_uring_queue_exit(&_ring);
            //queue_exit之后内核不会再碰这个ring上任何请求,此时才能安全释放强制收尾的任务
            for(IoTask* task:_forced_retired){delete task;}
            _forced_retired.clear();
        }
        //缓冲环内存同理:也要等queue_exit之后才轮到它们
        for(void* p:_retired_buf_ring_mem){::free(p);}
        _retired_buf_ring_mem.clear();
        INFO("IoUring destroyed: index={}",_index);
    }
public:
    //自定义删除器:所有IoUring都必须用Ptr(unique_ptr + 这个删除器)持有
    struct Deleter
    {
        void operator()(IoUring* p)const{IoUring::destroy(p);}
    };
    using Ptr=std::unique_ptr<IoUring,Deleter>;
    static Ptr create(){return Ptr(new IoUring());}
    //统一的销毁入口
    //正常线程上:先stop()+join再真正析构,所有成员都能安全释放
    //worker线程自己的回调里:此刻线程还在使用本对象,一旦析构成员就是use after free
    //  此时选择"整块泄漏",并置位_abandoned让worker在退出前自己释放ring与缓冲环,
    //  这样既没有UB,也不会把io_uring的fd一起漏掉
    static void destroy(IoUring* p)
    {
        if(!p){return;}
        //只要是worker线程来析构就整块泄漏:
        //删自己的ring=worker还在用它,释放成员就是UAF;
        //删别的ring会走~IoUring里的join,而对方的worker可能正在join本worker,两边互等就是死锁。
        //置_abandoned后worker退出前会自己释放io_uring与缓冲环,fd不会漏,只漏这个很小的对象
        //worker仍在跑时只能泄漏:删自己的ring=UAF,删别的ring会走~IoUring里的join,而对方worker可能正在join本worker。
        //worker已经跑到末尾的情形是安全的:没有线程再碰它,也不存在交叉join,正常delete才能把io_uring的fd收回去
        //(worker退出后到被join之间joinable()仍为真,所以只能看_worker_exited,不能看joinable())
        if(tls_worker_ring!=nullptr&&!p->_worker_exited.load(std::memory_order_acquire))
        {
            FATAL("IoUring destroyed from inside a worker callback: index={}, the object is leaked on purpose and the worker releases the ring on exit",p->_index);
            p->_abandoned.store(true,std::memory_order_release);
            p->_stop_requested.store(true,std::memory_order_release);
            return;
        }
        delete p;
    }
    bool is_worker_thread()const
    {
        //先走thread_local快路径(不加锁):worker在自己的回调里析构ring时会走到这里,
        //而此刻很可能正有别的线程持_wait_mtx在join本worker,worker再去抢这把锁就是互等死锁
        //(实测:回调里调wait()/stop_urings且外部同时在wait(),进程100%卡死)
        if(tls_worker_ring==this){return true;}
        //非worker线程:_worker_thread会被start()在锁内整体替换,不加锁直接读joinable()/get_id()是数据竞争(UB)
        std::lock_guard<std::mutex> lock(_wait_mtx);
        return _worker_thread.joinable()&&std::this_thread::get_id()==_worker_thread.get_id();
    }
private:
    //对象被判定为"在worker里自毁"时,由worker自己释放io_uring资源(两条退出路径都会走到)
    void release_if_abandoned()
    {
        if(!_abandoned.load(std::memory_order_acquire)){return;}
        INFO("abandoned IoUring releases its io_uring resources from the worker thread: index={}",_index);
        teardown_recv_buffers();
        if(_ring_inited){io_uring_queue_exit(&_ring);_ring_inited=false;}
    }
    //释放provided buffer ring与其内存(必须在queue_exit之前、worker停止之后调用)
    void teardown_recv_buffers()
    {
        if(!_buf_ring){return;}
        if(_leak_ring){return;}//ring已经泄漏,缓冲内存一并放弃,避免线程仍在使用时释放
        io_uring_free_buf_ring(&_ring,_buf_ring,static_cast<unsigned>(_buf_ring_entries),static_cast<int>(_buf_ring_bgid));
        _buf_ring=nullptr;
        //内存块本身不能在这里free:被force_complete强制收尾的buffer-select multishot请求可能还在内核手里,
        //queue_exit之前内核仍可能往这些缓冲里写数据报(与_forced_retired推迟释放是同一个理由)
        if(_buf_ring_mem){_retired_buf_ring_mem.push_back(_buf_ring_mem);_buf_ring_mem=nullptr;}
        _buf_ring_addrs.clear();
        _buf_ring_ready.store(false,std::memory_order_release);
    }
    IoUring(const IoUring&) = delete;
    IoUring& operator=(const IoUring&) = delete;
public:
    [[nodiscard]] bool start()
    {
        //worker线程里不允许start:本函数要在_wait_mtx下回收/换线程,而持锁的那个线程可能正在join本worker,
        //worker去抢这把锁就是互等死锁;两个worker互相start()对方的ring同样是交叉join(wait()/destroy()同一条规矩)
        if(tls_worker_ring!=nullptr)
        {
            WARN("IoUring::start called from a worker thread: index={}, rejected to avoid a join deadlock",_index);
            return false;
        }
        //"回收旧worker + 抢占状态 + 换上新线程"必须在同一把_wait_mtx下完成,中间不能留任何窗口:
        //并发start/wait会插进这个窗口,把_worker_thread换成新一代并bump代次;本次调用预回收时看到的还是旧线程、
        //wait()又因为代次不符直接返回true(它认为要等的那一代已被回收),随后本次CAS成功,
        //移动赋值到那个刚创建、还没人join的新线程对象上——std::thread对joinable对象移动赋值直接terminate
        //(并发start/stop/wait压测实测复现,事件序列:start_prejoin→wait_genmismatch→别人spawn→本次CAS成功→SPAWN_JOINABLE)
        std::unique_lock<std::mutex> lifecycle(_wait_mtx);
        UringState cur=_state.load(std::memory_order_acquire);
        if(cur==UringState::Running||cur==UringState::Starting)
        {
            ERROR("IoUring start rejected: index={} is already running or starting (state={})",_index,static_cast<int>(cur));
            return false;
        }
        //先把上一次的worker线程彻底回收
        //只看到状态是Stopped并不代表线程已经退出(worker在cleanup() 之前就把状态改成了Stopped),
        //如果此时直接创建新线程,新旧两个worker会同时操作同一个ring
        if(_worker_thread.joinable())
        {
            if(std::this_thread::get_id()==_worker_thread.get_id())
            {
                ERROR("cannot start the IoUring from inside its own worker thread: index={}",_index);
                return false;
            }
            //join前必须发出停止请求,否则还在正常循环里的worker永远不会退出
            //锁序是_wait_mtx→_submit_mtx(stop只拿_submit_mtx),而worker的退出路径不碰_wait_mtx,不会死锁
            (void)stop();
            _worker_thread.join();
            _joined=true;
            //旧worker已经join回来,状态归位Stopped:它可能停在Stopping/Failed,不清掉下面的CAS永远失败
            //此刻锁在手上、旧worker已死,没有别人能并发改状态,这一拍是安全的
            _state.store(UringState::Stopped,std::memory_order_release);
        }
        UringState expected = UringState::Stopped;
        if(!_state.compare_exchange_strong(expected,UringState::Starting,std::memory_order_acq_rel,std::memory_order_acquire))
        {
            ERROR("IoUring start rejected: index={} lost the state race (state={})",_index,static_cast<int>(_state.load(std::memory_order_acquire)));
            return false;
        }
        _stop_requested.store(false,std::memory_order_release);
        _pending_count.store(0,std::memory_order_release);
        //上一次运行的残留(正常情况下cleanup()已经清空),重新启动前必须归零
        clear_inflight();
        _retired.clear();
        _retired.reserve(256);
        //注意:_forced_retired不能在这里清空。里面的任务要等queue_exit之后才能释放,
        //一旦清空就既没释放也找不回来,任务与它持有的Buffer会永久泄漏
        {
            //必须和add_timer/cancel_timer用同一把锁:tick()会在锁内换槽并推进秒针
            std::lock_guard<std::mutex> lock(_timer_mtx);
            _slots.resize(_capacity);
            _current_slot_id.store(0,std::memory_order_release);
            _id_map.clear();
        }
        std::shared_ptr<std::latch> latch;
        try{
            latch=std::make_shared<std::latch>(1);
        }catch(const std::bad_alloc&){
            //分配失败必须把状态放回Stopped:留在Starting的话start()永远拒绝、wait()也不归位,这个ring就废了
            _state.store(UringState::Stopped,std::memory_order_release);
            ERROR("cannot allocate the startup latch: index={}",_index);
            return false;
        }
        _start_latch=latch;//只登记本代闩锁;worker拿到的是自己的shared_ptr,不会和下一代抢同一块存储
        uint64_t my_gen=0;
        try{
            //到这一步_worker_thread一定是非joinable的:生命周期锁从头拿到这里,换代只在锁内发生
            _worker_thread=std::thread([this,latch]{worker_loop(latch);});
            _joined=false;
            _worker_exited.store(false,std::memory_order_release);
            my_gen=_worker_gen.fetch_add(1,std::memory_order_acq_rel)+1;
        }catch(const std::exception& e){
            //这里必须落回Stopped:线程压根没建出来,不会有wait() 去把状态归位,
            //留在Failed的话下一次start() 的CAS(从Stopped出发)永远失败,这个ring就再也起不来了
            _start_latch.reset();
            _state.store(UringState::Stopped,std::memory_order_release);
            ERROR("cannot create the IoUring worker thread: index={}, error={}",_index,e.what());
            return false;
        }
        lifecycle.unlock();
        latch->wait();
        {
            std::lock_guard<std::mutex> lock(_wait_mtx);
            //只清理自己这一代登记的闩锁:等latch期间可能已经有别的start()换过代了
            if(_start_latch==latch){_start_latch.reset();}
        }
        if(_state.load(std::memory_order_acquire)!=UringState::Running)
        {
            //join必须走_wait_mtx/_joined这套规矩:并发wait()也在join同一个std::thread,各拿各的裸join就是double-join(UB)
            {
                std::lock_guard<std::mutex> lock(_wait_mtx);
                //只回收"自己这一代"的worker:等latch期间可能已经有别的start()换代、
                //并把线程对象交给它自己那套逻辑管;拿着旧代次去join会把别人的活worker挂死,还会错误置位_joined
                if(_worker_gen.load(std::memory_order_acquire)==my_gen&&!_joined&&_worker_thread.joinable())
                {
                    _worker_thread.join();
                    _joined=true;
                }
            }
            //worker自己把状态置成了Failed(例如1秒tick挂不上),这里要把它归位成Stopped才能再次start()
            //但不能无条件拍:worker尾巴可能已经归位Stopped,而另一个start()已经从Stopped CAS成Starting,
            //这一拍把人家的Starting覆盖掉,新worker的Starting→Running CAS必然失败,start就被误杀
            {
                UringState cur=_state.load(std::memory_order_acquire);
                if(cur!=UringState::Starting)
                {
                    _state.store(UringState::Stopped,std::memory_order_release);
                }
            }
            return false;
        }
        return true;
    }
    [[nodiscard]] bool stop()
    {
        _stop_requested.store(true,std::memory_order_release);
        if(!disable_multishot_timeout())
        {
            //取不到SQE时旧的1秒multishot timeout会继续武装着
            //之后重新start()会再挂一个,导致tick()每秒跑两次、所有定时器只等一半时间
            WARN("cannot disable the 1s multishot timeout: index={}, after a restart the timer wheel may tick twice per second",_index);
        }
        {
            std::lock_guard lock(_submit_mtx);
            //在锁内把状态从Running/Starting摘掉
            //这样submit_task不会再往这个ring里放新请求,后面join之后的queue_exit
            //就不会和别的线程的io_uring_submit并发
            UringState cur=_state.load(std::memory_order_acquire);
            while(cur==UringState::Running||cur==UringState::Starting)
            {
                if(_state.compare_exchange_weak(cur,UringState::Stopping,std::memory_order_acq_rel,std::memory_order_acquire)){break;}
            }
            io_uring_sqe* sqe=io_uring_get_sqe(&_ring);
            if(sqe)
            {
                io_uring_prep_nop(sqe);
                //必须显式清空user_data
                //io_uring_get_sqe返回的是SQ数组里被复用过的槽位,里面的user_data还是上一次提交留下的指针,
                //worker收到这个CQE后会把它当成IoTask* 处理并delete,直接导致非法释放
                io_uring_sqe_set_data(sqe,nullptr);
                io_uring_submit(&_ring);
            }
            else if(_has_ext_arg)
            {
                //有EXT_ARG时worker的等待自带1秒超时,晚一秒总能自己醒过来
                WARN("cannot submit the stop NOP: index={} has no free SQE, the worker will notice the stop request via its 1 second wait timeout",_index);
            }
            else
            {
                //没有EXT_ARG的内核上worker是无限期等CQE的,而stop()刚把1秒multishot tick拆掉、这条NOP又没提交出去,
                //可能一直睡到下一个完成事件到来(调用方若在join就会等这么久)
                WARN("cannot submit the stop NOP on a kernel without EXT_ARG: index={} may not notice the stop request until the next completion arrives",_index);
            }
        }
        return true;
    }
    //等待worker线程退出
    //返回值:true表示线程已经回收,false表示当前正处于worker线程内,无法自我join
    //注意:在worker线程里调用时不能join,交给持有IoUring的线程稍后再回收
    [[nodiscard]] bool wait()
    {
        const uint64_t gen=_worker_gen.load(std::memory_order_acquire);
        (void)stop();//stop() 的返回值只表示"停止请求是否被接受",wait只关心能否join
        //worker线程一律不join,且判定必须在拿_wait_mtx之前:
        //1)自己的worker去抢_wait_mtx会和"持有它正在join本worker"的线程互等;
        //2)别的worker来join本ring会形成A等B、B等A的交叉等待(多ring并发stop实测100%死锁)。
        //上面已经stop过,停止请求发出去了,worker会自己退出,回收交给非worker线程
        if(tls_worker_ring!=nullptr)
        {
            WARN("IoUring::wait called from a worker thread: index={}, stop requested but the join is deferred to the owner thread",_index);
            return false;
        }
        //std::thread::join只能有一个调用者:两个线程同时对同一个std::thread join是UB
        //(实测:2个线程并发wait就有50%挂死,32线程有5/8直接SIGSEGV或futex abort)
        //这里用一把锁让join"只做一次",后来的调用者等到回收完成后直接返回
        std::lock_guard<std::mutex> lock(_wait_mtx);
        if(_joined){return true;}//本ring的worker已经回收过了,这次调用不做事
        //等锁期间ring可能被另一个线程重新start():_worker_thread已经换成新一代worker,
        //而本线程刚才那次stop()作用在上一代(已由start()开头的wait()回收)身上。
        //此时绝不能去join新worker——没人通知它停,join就是永久卡死;上一代既然已被回收,本次wait的目标已经达成
        if(_worker_gen.load(std::memory_order_acquire)!=gen){return true;}
        if(_worker_thread.joinable()&&std::this_thread::get_id()==_worker_thread.get_id())
        {
            WARN("IoUring::wait called from its own worker thread: index={}, stop requested but the join is deferred to the owner thread",_index);
            return false;
        }
        try{
            if(_worker_thread.joinable())
            {
                _worker_thread.join();
            }
        }catch(...){
            ERROR("cannot join the IoUring worker thread: index={}, the ring is leaked instead of freed",_index);
            //join失败时如果detach之后再queue_exit,线程可能还在使用这个ring
            //这里detach只是为了不让std::thread的析构触发terminate,同时标记ring泄漏
            try{
                if(_worker_thread.joinable()){_worker_thread.detach();}
            }catch(...){
                FATAL("cannot detach the IoUring worker thread after a failed join: index={}",_index);
            }
            _leak_ring=true;
        }
        _joined=true;//即使这次join失败(已detach)也要标记成已回收,否则别的线程会再去join同一个std::thread
        //join完不能无条件把状态拍成Stopped:join期间并发的start()可能已经把状态CAS成Starting(它的CAS不拿_wait_mtx),
        //这一拍覆盖回去,那个start与"下一个从Stopped出发的start"会同时成立——两边都往下spawn,
        //后一次对仍joinable的_worker_thread移动赋值直接terminate(并发start/stop/wait压测实测复现)
        {
            //除Starting外都归位Stopped(含Failed):worker可能以Failed退出(例如1秒tick定时器武装失败),
            //不归位的话"从Stopped出发"的CAS永远失败,这个ring就再也起不来了;Failed的原因已经打过ERROR日志
            UringState cur=_state.load(std::memory_order_acquire);
            if(cur!=UringState::Starting)
            {
                _state.store(UringState::Stopped,std::memory_order_release);
            }
        }
        INFO("IoUring worker stopped and joined: index={}",_index);
        return true;
    }
    void set_index(size_t index){_index=index;}
    size_t index()const{return _index;}
    //当前线程正在为哪个ring跑worker循环,不在worker上下文时返回nullptr
    //用途:让"没有显式传uring"的发送走当前线程自己的ring,避免跨线程抢别人的提交锁
    static IoUring* current(){return tls_worker_ring;}
    UringState state()const{return _state.load(std::memory_order_acquire);}
    bool is_running()const{return state()==UringState::Running;}
    //已经提交,还没收到完成事件的请求数(供观测/停机诊断使用)
    int64_t pending_count()const{return _pending_count.load(std::memory_order_relaxed);}
    //停机时有几个请求取消没成功,由cleanup强制按-ECANCELED收尾,正常(所有请求都可取消)时应当恒为0;非0说明内核对某些请求的取消没能在超时时间内生效
    size_t forced_cancellations()const{return _forced_count.load(std::memory_order_relaxed);}
public:
    //所有async_* 的公共契约:从提交到回调之间,调用方不得再动这块Buffer
    //(append/commit/consume/reserve/reserve_for/shrink/trim/reset都不行)。
    //内核收发时用的是提交那一刻的指针与长度,中途重新分配会让它往已经释放的内存里写:轻则数据丢失,重则堆破坏。
    //需要"边发边攒"就用另一块缓冲(pending buffer),不要动正在途中的那一块。
    [[nodiscard]] bool async_read(int fd,std::shared_ptr<Buffer> buf, IoTask::Callback cb,bool auto_buf=true)
    {
        if(fd<0||!buf||buf->writable_size()==0){return false;}//长度0的read在内核侧会返回0,调用方会误判成EOF
        return submit_task(std::make_unique<IoTask>(this,fd,IoTask::IoEvent::READ,buf,std::move(cb),auto_buf));
    }
    [[nodiscard]] bool async_read(int fd,int64_t offset,std::shared_ptr<Buffer> buf,IoTask::Callback cb,bool auto_buf=true)
    {
        if(fd<0||!buf||buf->writable_size()==0){return false;}//与上面的async_read同一条约:长度0的read会被内核返回0,调用方会误判成EOF
        auto task=std::make_unique<IoTask>(this,fd,IoTask::IoEvent::READ,buf,std::move(cb),auto_buf);
        task->_fd_offset=offset;
        return submit_task(std::move(task));
    }
    [[nodiscard]] bool async_write(int fd,std::shared_ptr<Buffer> buf,IoTask::Callback cb,bool auto_buf=true)
    {
        if(fd<0||!buf||buf->readable_size()==0){return false;}
        return submit_task(std::make_unique<IoTask>(this,fd,IoTask::IoEvent::WRITE,buf,std::move(cb),auto_buf));
    }
    //提交accept,multishot=true时使用IORING_ACCEPT_MULTISHOT一次提交之后内核会持续投递新连接,每次完成都会调用回调(回调里可通过task._has_more判断是否还有后续),直到出错或队列空
    [[nodiscard]] bool async_accept(int fd,IoTask::Callback cb,bool multishot=false)
    {
        if(fd<0){return false;}
        auto task=std::make_unique<IoTask>(this,fd,IoTask::IoEvent::ACCEPT,nullptr,std::move(cb));
        task->_multishot=multishot;
        return submit_task(std::move(task));
    }
    [[nodiscard]] bool async_recv(int fd,std::shared_ptr<Buffer> buf,IoTask::Callback cb,bool auto_buf=true)
    {
        //写入空间为0时内核会立刻返回0,调用方会把它误判成对端关闭,所以直接拒绝提交
        if(fd<0||!buf||buf->writable_size()==0){return false;}
        return submit_task(std::make_unique<IoTask>(this,fd,IoTask::IoEvent::RECV,buf,std::move(cb),auto_buf));
    }
    [[nodiscard]] bool async_send(int fd,std::shared_ptr<Buffer> buf,IoTask::Callback cb,bool auto_buf=true)
    {
        if(fd<0||!buf||buf->readable_size()==0){return false;}
        return submit_task(std::make_unique<IoTask>(this,fd,IoTask::IoEvent::SEND,buf,std::move(cb),auto_buf));
    }
    [[nodiscard]] bool async_recvmsg(int fd,std::shared_ptr<Buffer> buf,IoTask::Callback cb,bool auto_buf=true)
    {
        if(fd<0||!buf||buf->writable_size()==0){return false;}
        sockaddr_storage dummy{};
        return submit_task(std::make_unique<IoTask>(this,fd,IoTask::IoEvent::RECVMSG,buf,dummy,sizeof(sockaddr_storage),std::move(cb),true,auto_buf));
    }
    //提交multishot recvmsg,缓冲由provided buffer ring提供
    //每次完成都会回调:内核把struct io_uring_recvmsg_out+源地址+payload一起写进被选中的缓冲,
    //所以每个数据报的源地址都能正确拿到(用io_uring_recvmsg_validate/name/payload解析)
    //回调通过task._has_more判断是否还有后续,通过task._provided_bid拿到缓冲编号
    [[nodiscard]] bool async_recvmsg_multishot(int fd,IoTask::Callback cb)
    {
        if(fd<0||!_buf_ring_ready.load(std::memory_order_acquire)){return false;}
        auto task=std::make_unique<IoTask>(this,fd,IoTask::IoEvent::RECVMSG,nullptr,std::move(cb));
        task->_multishot=true;
        task->_buffer_select=true;
        //只请求源地址,不要控制消息(sockaddr_storage足够放下v4与v6)
        task->_msg.msg_name=nullptr;
        task->_msg.msg_namelen=sizeof(sockaddr_storage);
        task->_msg.msg_iov=nullptr;
        task->_msg.msg_iovlen=0;
        task->_msg.msg_control=nullptr;
        task->_msg.msg_controllen=0;
        return submit_task(std::move(task));
    }
    //通用版本:v4/v6都通过sockaddr_storage传进来
    [[nodiscard]] bool async_sendmsg(int fd,std::shared_ptr<Buffer> buf,const sockaddr_storage& dest,socklen_t dest_len,IoTask::Callback cb,bool auto_buf=true)
    {
        if(fd<0||!buf){return false;}
        if(dest_len==0||dest_len>sizeof(sockaddr_storage))
        {
            ERROR("async_sendmsg rejected an invalid destination length: fd={}, dest_len={}, limit={}",fd,static_cast<unsigned>(dest_len),sizeof(sockaddr_storage));
            return false;
        }
        return submit_task(std::make_unique<IoTask>(this,fd,IoTask::IoEvent::SENDMSG,buf,dest,dest_len,std::move(cb),false,auto_buf));
    }
    [[nodiscard]] bool async_sendmsg(int fd,std::shared_ptr<Buffer> buf,const sockaddr_in& dest,IoTask::Callback cb,bool auto_buf=true)
    {
        if(fd<0||!buf){return false;}
        sockaddr_storage storage{};
        socklen_t len=sockaddr_to_storage(dest,storage);
        return async_sendmsg(fd,std::move(buf),storage,len,std::move(cb),auto_buf);
    }
    [[nodiscard]] bool async_sendmsg(int fd,std::shared_ptr<Buffer> buf,const sockaddr_in6& dest,IoTask::Callback cb,bool auto_buf=true)
    {
        if(fd<0||!buf){return false;}
        sockaddr_storage storage{};
        socklen_t len=sockaddr_to_storage(dest,storage);
        return async_sendmsg(fd,std::move(buf),storage,len,std::move(cb),auto_buf);
    }
    [[nodiscard]] bool async_close(int fd,IoTask::Callback cb={})
    {
        if(fd<0){return false;}
        return submit_task(std::make_unique<IoTask>(this,fd,IoTask::IoEvent::CLOSE,nullptr,std::move(cb)));
    }
    //创建provided buffer ring并一次性填满缓冲(entries会被向上取整到2的幂),缓冲内存由IoUring自己分配并持有,析构时统一释放
    [[nodiscard]] bool setup_recv_buffers(size_t entries,size_t buf_size,unsigned bgid=1)
    {
        //缓冲环是worker收包时读、回收时写的,只能在worker起来之前建好。
        //这个检查必须放在teardown之前:否则一次"运行期重建"的非法调用会先把正在用的环释放掉,
        //worker可能正在用其中的缓冲(use after free),而调用方只看到一个false
        if(_state.load(std::memory_order_acquire)!=UringState::Stopped)
        {
            ERROR("setup_recv_buffers must be called before start(): index={}, state={}",_index,static_cast<int>(_state.load(std::memory_order_acquire)));
            return false;
        }
        //重启时先把上一轮的环和缓冲释放掉
        if(_buf_ring_ready.load(std::memory_order_acquire)){teardown_recv_buffers();}
        if(!_ring_inited||entries<2||buf_size==0){return false;}
        //内核的provided buffer ring有硬上限,而且下面按16字节对齐会做乘加,参数必须先在安全范围内:
        //(buf_size+15) 在buf_size接近SIZE_MAX时会回绕成0 -> malloc(0) 却给内核登记4GB缓冲 -> 内核越界写
        if(entries>32768||buf_size>UINT_MAX||buf_size>SIZE_MAX-15)
        {
            ERROR("setup_recv_buffers rejected invalid parameters: entries={}, buffer_size={}",entries,buf_size);
            return false;
        }
        //bitset/bit_ceil是C++20的标准写法:输入为2的幂时原样返回,否则向上取整
        const size_t n=std::bit_ceil(entries);
        int err=0;
        _buf_ring=io_uring_setup_buf_ring(&_ring,static_cast<unsigned>(n),static_cast<int>(bgid),0,&err);
        if(!_buf_ring)
        {
            ERROR("io_uring_setup_buf_ring failed: index={}, entries={}, bgid={}, error={}",_index,n,bgid,errno_text(err).c_str());
            return false;
        }
        //每块缓冲的起始地址必须满足内核要写进去的那些结构体的对齐要求:
        //multishot recvmsg会在缓冲开头写io_uring_recvmsg_out(需要4字节对齐),
        //而buf_size往往是奇数(如16+128+65507),直接按buf_size步进会让第2块之后落在奇数地址上
        //那是未对齐访问,C++标准里是UB,x86能跑、严格对齐的平台会直接崩。
        //这里把步进向上取整到16的倍数,登记给内核的长度仍按调用方给的buf_size。
        const size_t stride=(buf_size+15)&~static_cast<size_t>(15);
        if(stride==0||n>SIZE_MAX/stride)
        {
            ERROR("setup_recv_buffers: the buffer ring size overflows: entries={}, buffer_size={}, stride={}",n,buf_size,stride);
            io_uring_free_buf_ring(&_ring,_buf_ring,static_cast<unsigned>(n),static_cast<int>(bgid));
            _buf_ring=nullptr;
            return false;
        }
        _buf_ring_mem=::malloc(n*stride);
        if(!_buf_ring_mem)
        {
            ERROR("cannot allocate memory for the provided buffer ring: index={}, bytes={} (entries={}, buffer_size={}, stride={})",_index,n*stride,n,buf_size,stride);
            io_uring_free_buf_ring(&_ring,_buf_ring,static_cast<unsigned>(n),static_cast<int>(bgid));
            _buf_ring=nullptr;
            return false;
        }
        //resize可能抛bad_alloc:本函数的契约是"失败返回false",而且不清理的话已注册的buf ring与_malloc出来的内存就永久泄漏
        try{
            _buf_ring_addrs.resize(n);
        }catch(const std::exception& e){
            ERROR("cannot allocate the buffer address table: index={}, entries={}, error={}",_index,n,e.what());
            ::free(_buf_ring_mem);
            _buf_ring_mem=nullptr;
            io_uring_free_buf_ring(&_ring,_buf_ring,static_cast<unsigned>(n),static_cast<int>(bgid));
            _buf_ring=nullptr;
            return false;
        }
        int mask=io_uring_buf_ring_mask(static_cast<unsigned>(n));
        for(size_t i=0;i<n;i++)
        {
            void* p=static_cast<char*>(_buf_ring_mem)+i*stride;
            _buf_ring_addrs[i]=p;
            io_uring_buf_ring_add(_buf_ring,p,static_cast<unsigned>(buf_size),static_cast<unsigned short>(i),mask,static_cast<int>(i));
        }
        io_uring_buf_ring_advance(_buf_ring,static_cast<int>(n));
        _buf_ring_bgid=bgid;
        _buf_ring_entries=n;
        _buf_ring_buf_size=buf_size;
        _buf_ring_ready.store(true,std::memory_order_release);
        //探测一次PBUF_STATUS是否可用(内核<6.8没有这个查询)
        _buf_ring_status_supported.store(io_uring_buf_ring_available(&_ring,_buf_ring,static_cast<unsigned short>(bgid))>=0,std::memory_order_release);
        INFO("provided buffer ring ready: index={}, entries={}, buffer_size={}, bgid={}",_index,n,buf_size,bgid);
        return true;
    }
    bool has_recv_buffers()const{return _buf_ring_ready.load(std::memory_order_acquire);}
    size_t recv_buffers_count()const{return _buf_ring_entries;}
    size_t recv_buffer_size()const{return _buf_ring_buf_size;}
    //provided buffer ring里还有多少块缓冲没被内核取走
    //用途:multishot recvmsg以-ENOBUFS结束时,判断"立刻重挂"会不会马上又失败(缓冲真的用光时立刻重挂只会得到同样的错误,变成100% CPU忙循环)
    unsigned recv_buffers_available()const
    {
        const int n=recv_buffer_ring_status();
        //内核不支持查询时返回"全部可用",而不是0:0表示"环是空的",会让调用方误判成"现在重挂一定-ENOBUFS"
        if(n<0){return static_cast<unsigned>(_buf_ring_entries);}
        return static_cast<unsigned>(n);
    }
    //io_uring_buf_ring_available的原始返回值(负数表示内核不支持PBUF_STATUS查询)
    int recv_buffer_ring_status()const
    {
        if(!_buf_ring||!_buf_ring_ready.load(std::memory_order_acquire)||!_buf_ring_status_supported.load(std::memory_order_relaxed)){return -1;}
        return io_uring_buf_ring_available(const_cast<io_uring*>(&_ring),_buf_ring,static_cast<unsigned short>(_buf_ring_bgid));
    }
    //userspace侧的tail计数(每归还一块缓冲+1),观测用
    unsigned recv_bufring_tail()const{return _buf_ring?_buf_ring->tail:0;}
    uint64_t recv_buffers_recycled()const{return _buf_ring_recycled.load(std::memory_order_relaxed);}
    void* recv_buffer(unsigned bid)const{return bid<_buf_ring_addrs.size()?_buf_ring_addrs[bid]:nullptr;}
    //把缓冲还给内核(只能由该ring的worker线程调用)
    void recycle_recv_buffer(unsigned bid)
    {
        if(!_buf_ring||bid>=_buf_ring_addrs.size()){return;}
        int mask=io_uring_buf_ring_mask(static_cast<unsigned>(_buf_ring_entries));
        io_uring_buf_ring_add(_buf_ring,_buf_ring_addrs[bid],static_cast<unsigned>(_buf_ring_buf_size),static_cast<unsigned short>(bid),mask,0);
        io_uring_buf_ring_advance(_buf_ring,1);
        _buf_ring_recycled.fetch_add(1,std::memory_order_relaxed);
    }
    //按fd取消该ring上所有还在途的请求
    //用途:排空时取消挂起的accept,listen fd上挂着的accept会持有socket的file引用,只close(fd)并不会真正销毁socket,端口仍然被占着,resume() 重新bind同一端口就会失败
    bool cancel_fd(int fd)
    {
        if(fd<0){return false;}
        std::lock_guard lock(_submit_mtx);
        if(_state.load(std::memory_order_acquire)!=UringState::Running){return false;}
        io_uring_sqe* sqe=io_uring_get_sqe(&_ring);
        if(!sqe)
        {
            io_uring_submit(&_ring);
            sqe=io_uring_get_sqe(&_ring);
        }
        if(!sqe){return false;}
        io_uring_prep_cancel_fd(sqe,fd,IORING_ASYNC_CANCEL_ALL);
        io_uring_sqe_set_data(sqe,nullptr);//这个SQE必须清空user_data,否则worker会拿到残留指针
        return io_uring_submit(&_ring)>=0;
    }
    //把一个函数投递到该IoUring的worker线程执行
    //实现方式:提交一个NOP,worker收到CQE后在它自己的线程上调用回调
    //用途:业务线程池处理完请求后,把"发送响应"这一步交回拥有该连接的uring线程,避免在别的线程上直接操作连接上下文
    [[nodiscard]] bool post(std::function<void()> func)
    {
        if(!func){return false;}
        auto task=std::make_unique<IoTask>(IoTask::Callback([fn=std::move(func)](IoTask&,ssize_t){fn();}));
        task->_uring=this;
        task->_event=IoTask::IoEvent::POST;
        return submit_task(std::move(task));
    }
    //等待fd可写后执行回调,用于没有内置缓冲的发送路径(例如splice直接写socket)做背压:socket发送缓冲区满时splice会返回EAGAIN,必须等它重新可写再重试
    [[nodiscard]] bool async_poll_out(int fd,IoTask::Callback cb)
    {
        if(fd<0){return false;}
        auto task=std::make_unique<IoTask>(this,fd,IoTask::IoEvent::POLL,nullptr,std::move(cb));
        task->_poll_mask=POLLOUT;
        return submit_task(std::move(task));
    }
    [[nodiscard]] bool async_splice(int fd_in,int64_t in_offset,int fd_out,int64_t out_offset,size_t len,IoTask::Callback cb)
    {
        if(fd_in<0||fd_out<0||len==0){return false;}
        //io_uring的长度字段是unsigned int:超过4GB会被静默截断成只搬一小段,调用方却以为整段都搬完了,这里显式拒绝,而不是悄悄截断
        if(len>static_cast<size_t>(UINT_MAX))
        {
            ERROR("async_splice length {} exceeds the io_uring limit of {} bytes",len,UINT_MAX);
            return false;
        }
        return submit_task(std::make_unique<IoTask>(this,fd_in,in_offset,fd_out,out_offset,len,std::move(cb)));
    }
    //添加定时任务,返回任务id
    [[nodiscard]] uint64_t add_timer(std::chrono::seconds delay,const std::function<void(void)>& cb)
    {
        if(_state.load(std::memory_order_acquire)!=UringState::Running){return 0;}
        if(!cb){return 0;}
        const int64_t wanted=delay.count();
        const int64_t clamped=wanted<1?1:std::min<int64_t>(wanted,static_cast<int64_t>(_capacity)-1);
        const uint32_t time=static_cast<uint32_t>(clamped);
        auto task = std::make_shared<TimerTask>();
        std::lock_guard<std::mutex> lock(_timer_mtx);
        //cleanup()会清空时间轮,此时若状态还没切到Stopped,这里必须自己确保下标
        if(_slots.size()!=_capacity){return 0;}
        //槽位必须在锁内计算:tick()会在同一把锁里换槽并推进秒针。如果在锁外算,新定时器可能落进刚刚被换空的槽,最坏要等一整圈(59秒)才触发
        uint32_t slot_id=(_current_slot_id.load(std::memory_order_relaxed)+time)%_capacity;
        uint64_t id=_next_timertask_id.fetch_add(1,std::memory_order_relaxed);
        task->_id=id;
        task->_timed_callback=cb;
        _slots[slot_id].push_back(task);
        _id_map[id]=slot_id;
        return id;
    }
    //取消定时任务
    bool cancel_timer(uint64_t id)
    {
        if(_state.load(std::memory_order_acquire)!=UringState::Running){return false;}
        std::lock_guard<std::mutex> lock(_timer_mtx);
        if(auto it=_id_map.find(id);it!=_id_map.end())
        {
            std::erase_if(_slots[it->second],[id](auto& task){return task->_id==id;});
            _id_map.erase(it);
            return true;
        }
        return false;
    }
private:
    void worker_loop(std::shared_ptr<std::latch> latch)//latch是这一代自己的启动握手闩锁,和成员指针无关,避免跨代共享存储
    {
        tls_worker_ring=this;
        if(enable_multishot_timeout(1,0)==false)//启动1s的定时器
        {
            _state.store(UringState::Failed, std::memory_order_release);
            if(latch){latch->count_down();}
            ERROR("cannot arm the 1 second tick timer: index={}, the worker loop will not start",_index);
            cleanup();
            release_if_abandoned();
            tls_worker_ring=nullptr;
            _worker_exited.store(true,std::memory_order_release);
            return;
        }
        {
            UringState expected=UringState::Starting;
            if(!_state.compare_exchange_strong(expected,UringState::Running,std::memory_order_acq_rel,std::memory_order_acquire))
            {
                //启动期间就被要求停止(或状态已被别人夺走),不要把Stopping覆盖回Running
                //这里顺手把停止标志置上:本worker接下来必须退出,否则start()的失败路径join本线程就会挂死
                WARN("stop was requested while the IoUring worker was still starting: index={}",_index);
                _stop_requested.store(true,std::memory_order_release);
            }
        }
        if(latch){latch->count_down();}
        INFO("IoUring worker loop started: index={}",_index);
        try{
            //循环条件必须同时盯着状态:worker只在Running时才允许活着。
            //只看_stop_requested的话,存在一种致命的组合——start()在锁内把_stop_requested重置成false,
            //而并发的stop()/wait()已经把状态CAS成Stopping(那个stop请求被这次重置吃掉了),
            //本worker的Starting→Running CAS必然失败,却因为标志是false而一直转下去;
            //此时start()的失败路径正在join本线程,于是永久挂死(压测实测:start的失败路径join + worker卡在wait_cqe)
            //状态一旦不是Running就退出,保证任何对该worker的join都是有界的
            while(!_stop_requested.load(std::memory_order_acquire)&&_state.load(std::memory_order_acquire)==UringState::Running)
            {
                //把worker自己攒下的SQE提交出去,否则下面会等一个还没交给内核的请求,直接卡死
                flush_submit();
                io_uring_cqe* wait_cqe = nullptr;
                struct __kernel_timespec ts={.tv_sec=1,.tv_nsec=0};
                int wait_ret=_has_ext_arg?io_uring_wait_cqe_timeout(&_ring,&wait_cqe,&ts):io_uring_wait_cqe(&_ring,&wait_cqe);
                if(wait_ret==-EINTR){continue;}
                else if(wait_ret<0&&wait_ret!=-ETIME)
                {
                    disable_multishot_timeout();
                    _state.store(UringState::Failed,std::memory_order_release);
                    ERROR("io_uring_wait_cqe_timeout failed: index={}, error={}",_index,errno_text(wait_ret).c_str());
                    break;
                }
                io_uring_cqe* cqes[CQE_BATCH];
                do{
                    if(_state.load(std::memory_order_acquire)!=UringState::Running){break;}
                    unsigned int readynum=io_uring_peek_batch_cqe(&_ring,cqes,CQE_BATCH);
                    if(readynum==0){break;}
                    process_cqes(cqes,readynum);
                    io_uring_cq_advance(&_ring,readynum);
                    //回调里重新提交的请求必须在这一批处理完立刻交出去
                    //否则在CQE持续不断的场景下,这些请求会一直攒着得不到执行
                    flush_submit();
                }while(true);
            }
        }catch(const std::exception& e){
            disable_multishot_timeout();
            _state.store(UringState::Failed,std::memory_order_release);
            ERROR("IoUring worker loop stopped by an exception: index={}, error={}",_index,e.what());
        }catch(...){
            disable_multishot_timeout();
            _state.store(UringState::Failed,std::memory_order_release);
            ERROR("IoUring worker loop stopped by an unexpected exception: index={}",_index);
        }
        //清理资源
        //先把状态切走:回调里可能再次提交io,submit_task会因为状态不是Running而直接失败,
        //从而避免在即将销毁的ring上继续压入新的请求
        //Failed状态要保留下来,方便上层判断是异常退出
        {
            UringState cur=_state.load(std::memory_order_acquire);
            if(cur==UringState::Running||cur==UringState::Stopping)
            {
                _state.store(UringState::Stopped,std::memory_order_release);
            }
        }
        cleanup();
        //对象已被判定为"在worker里自毁"时:不会再有析构函数来释放资源,由本线程收尾
        release_if_abandoned();
        tls_worker_ring=nullptr;
        //放在最后:置位之后本线程不再碰任何成员,destroy()看到它就敢直接delete
        _worker_exited.store(true,std::memory_order_release);
        INFO("IoUring worker loop exited: index={}",_index);
    }
    //处理一批CQE(worker正常循环与停机清理共用)
    void process_cqes(io_uring_cqe** cqes,unsigned int readynum)
    {
        //与提交线程建立happens-before:IoTask的字段是调用线程在submit_task之外就填好的,
        //worker读它们之前必须先"接住"提交线程在解锁_submit_mtx之前写的所有内容。
        //(TSan实测过:worker在下面读_event/_has_more/_completed与提交线程的写入构成数据竞争,
        // 因为中间那次"交棒"是经过内核的CQE,内存模型里没有边。这里加一次锁/解锁把这条边补上。)
        //只加锁/解锁一次,绝不在锁内跑回调:回调里通常会立刻重新提交,持锁回调会自己等自己
        {
            std::lock_guard<std::mutex> handoff(_submit_mtx);
        }
        for(unsigned int i=0;i<readynum;i++)
        {
            try{
                io_uring_cqe* cqe = cqes[i];
                IoTask* task = static_cast<IoTask*>(io_uring_cqe_get_data(cqe));
                ssize_t res = cqe->res;
                if(!task)[[unlikely]]{continue;}//正常情况下user_data一定是IoTask*
                if(task->_event==IoTask::IoEvent::TIMEOUT)[[unlikely]]
                {//单独处理定时器超时事件
                    //multishot超时死了(完成不再带F_MORE,请求已终止):老内核不支持IORING_TIMEOUT_MULTISHOT会-EINVAL。
                    //不处理的话时间轮从此静默停摆:所有定时器永远不到期,没有任何日志
                    if(!(cqe->flags&IORING_CQE_F_MORE)&&res!=-ECANCELED&&res!=-ENOENT)
                    {
                        ERROR("the 1s multishot timeout terminated unexpectedly: index={}, res={}, the timer wheel stops working; stopping the ring",_index,res);
                        _state.store(UringState::Failed,std::memory_order_release);
                        _stop_requested.store(true,std::memory_order_release);
                        continue;
                    }
                    //忽略定时器取消/移除事件
                    if (res!=-ECANCELED&&res!=-ENOENT&&task->_cb)
                    {
                        try{task->_cb(*task,res);}
                        catch(const std::exception& e){ERROR("timer callback threw an exception: index={}, error={}",_index,e.what());}
                    }
                    continue;
                }
                bool more=(cqe->flags&IORING_CQE_F_MORE)!=0;
                task->_has_more=more;
                if(task->_buffer_select)
                {
                    //解出本次用的是哪块provided buffer。
                    //必须先判IORING_CQE_F_BUFFER:出错(例如-ENOBUFS)的完成不带缓冲,flags的高位是垃圾,
                    //直接当bid用会让上层把bid 0再归还一次,环里就出现同一块缓冲被两个请求同时用(数据错乱)
                    task->_has_provided_buffer=(cqe->flags&IORING_CQE_F_BUFFER)!=0;
                    if(task->_has_provided_buffer){task->_provided_bid=static_cast<unsigned>(cqe->flags>>IORING_CQE_BUFFER_SHIFT);}
                }
                //multishot请求的中间完成:请求仍然在途,不能置_completed、不能释放、也不能减计数
                if(task->_multishot&&more)
                {
                    dispatch_task(task,res);
                    continue;
                }
                if(task->_completed.exchange(true)){continue;}
                dispatch_task(task,res);
                _pending_count.fetch_sub(1,std::memory_order_relaxed);
                //注销与释放推迟到本批结束
                //注销要拿_submit_mtx,逐条拿会明显拖慢完成路径;而且回调里通常会立刻重新提交请求,
                //在持有该锁的情况下dispatch会自死锁。放进_retired,由flush_retired() 统一处理。
                _retired.push_back(task);
            }catch(...){
                ERROR("unexpected exception while processing a completion: index={}",_index);
            }
        }
        //把本批已经完成的任务统一注销(需要锁)并释放(不需要锁)
        if(!_retired.empty())
        {
            {
                std::lock_guard lock(_submit_mtx);
                for(IoTask* task:_retired){unregister_inflight_locked(task);}
            }
            for(IoTask* task:_retired){delete task;}
            _retired.clear();
        }
    }
    //在途任务登记表相关:散列、登记、注销、扩容
    //全部要求调用方已经持有_submit_mtx
    static size_t inflight_hash(const IoTask* task,size_t cap)
    {
        //IoTask是alignas(64),指针低位恒为0:把"右移多少位"写成countr_zero(alignof(IoTask)),
        //以后改对齐值时不必再回来改这个魔数(否则哈希会退化成同槽线性探测)
        static_assert(std::has_single_bit(alignof(IoTask)),"IoTask alignment must be a power of two");
        uintptr_t v=std::bit_cast<uintptr_t>(task)>>std::countr_zero(alignof(IoTask));
        v^=v>>16;
        v*=0x9E3779B97F4A7C15ULL;
        return static_cast<size_t>(v)&(cap-1);
    }
    //把表扩到至少want个槽(必须是2的幂),返回false表示分配失败
    bool inflight_reserve_locked(size_t want)
    {
        if(want<=_inflight_cap){return true;}
        const size_t cap=std::bit_ceil(std::max<size_t>(want,1024));
        std::unique_ptr<IoTask*[]> table;
        try{
            table=std::make_unique<IoTask*[]>(cap);
        }catch(const std::bad_alloc&){
            return false;
        }
        for(size_t i=0;i<_inflight_cap;i++)
        {
            IoTask* task=_inflight_table[i];
            if(!task){continue;}
            size_t pos=inflight_hash(task,cap);
            while(table[pos]){pos=(pos+1)&(cap-1);}
            table[pos]=task;
        }
        _inflight_table=std::move(table);//旧表由unique_ptr释放
        _inflight_cap=cap;
        return true;
    }
    void register_inflight_locked(IoTask* task)
    {
        //装载因子控制在0.5以内,线性探测才不会退化
        if(_inflight_used*2>=_inflight_cap&&!inflight_reserve_locked(_inflight_cap?_inflight_cap*2:1024))
        {
            //登记不上就退化为不登记:取消仍然覆盖它,只是停机兜底找不到它,如实计数以便报告
            _inflight_untracked++;
            return;
        }
        size_t pos=inflight_hash(task,_inflight_cap);
        while(_inflight_table[pos]){pos=(pos+1)&(_inflight_cap-1);}
        _inflight_table[pos]=task;
        _inflight_used++;
    }
    void unregister_inflight_locked(IoTask* task)
    {
        if(_inflight_cap==0){return;}
        size_t pos=inflight_hash(task,_inflight_cap);
        while(_inflight_table[pos])
        {
            if(_inflight_table[pos]==task){break;}
            pos=(pos+1)&(_inflight_cap-1);
        }
        if(!_inflight_table[pos]){return;}//不在表里(可能当初就没登记成功)
        //线性探测的删除必须把后续元素前移,否则探测链会断掉,后面的元素再也找不到
        _inflight_table[pos]=nullptr;
        _inflight_used--;
        size_t next=(pos+1)&(_inflight_cap-1);
        while(_inflight_table[next])
        {
            IoTask* moved=_inflight_table[next];
            _inflight_table[next]=nullptr;
            _inflight_used--;
            size_t target=inflight_hash(moved,_inflight_cap);
            while(_inflight_table[target]){target=(target+1)&(_inflight_cap-1);}
            _inflight_table[target]=moved;
            _inflight_used++;
            next=(next+1)&(_inflight_cap-1);
        }
    }
    //清空登记表(worker重新启动时用,正常情况下cleanup已经清空)
    void clear_inflight()
    {
        std::lock_guard lock(_submit_mtx);
        for(size_t i=0;i<_inflight_cap;i++){_inflight_table[i]=nullptr;}
        _inflight_used=0;
        _inflight_untracked=0;
    }
    //执行一次完成事件的缓冲区推进与用户回调
    void dispatch_task(IoTask* task,ssize_t res)
    {
        if(res>0&&task->_buf&&task->_auto_buf)
        {
            try{
                if(task->_event==IoTask::IoEvent::READ||task->_event==IoTask::IoEvent::RECV||task->_event==IoTask::IoEvent::RECVMSG)
                {
                    //res来自提交时给出的iov长度,不可能超过可写区;这里显式忽略返回值只是防御
                    (void)task->_buf->commit(res);
                }
                if(task->_event==IoTask::IoEvent::WRITE||task->_event==IoTask::IoEvent::SEND||task->_event==IoTask::IoEvent::SENDMSG)
                {
                    (void)task->_buf->consume(res);
                }
            }catch(...){ERROR("cannot advance the buffer offset after a completion: index={}, event={}, result={}",_index,static_cast<int>(task->_event),res); }
        }
        if(task->_cb)
        {
            try{task->_cb(*task,res);}
            catch(const std::exception& e){ERROR("IoTask callback threw an exception: index={}, event={}, result={}, error={}",_index,static_cast<int>(task->_event),res,e.what());}
        }
    }
    void cleanup()
    {
        //将worker自己攒着没提交的SQE交出去,否则它们既不会被取消也不会产生CQE
        flush_submit();
        //取消重复触发定时器
        if(!disable_multishot_timeout())
        {
            WARN("cannot disable the 1s multishot timeout during cleanup: index={}, a later restart may tick twice per second",_index);
        }
        //收割当前已经就绪的CQE
        drain_ready();
        //取消所有还在途的请求,并等待它们的完成事件
        //关键点:io_uring的请求持有fd的file引用,close(fd) 并不会让它产生CQE,
        //所以必须显式取消,否则这些IoTask(以及它们持有的Buffer/Connection)会永久泄漏,
        //上层(例如Connection)也永远收不到关闭回调,fd也就一直不释放
        cancel_all_pending();
        drain_pending();
        //最后一道兜底:取消之后仍然没能产生完成事件的那些请求,在这里逐个按 -ECANCELED收尾,
        //否则它们会连同Buffer与捕获的Connection一起永久泄漏
        force_complete_leftovers();
        //重置时间轮
        //锁内只把两张表换出来,真正的析构放到锁外:_slots里的TimerTask可能持有某条Connection的最后一份shared_ptr,
        //销毁它会走~Connection→disable_timed_release()→cancel_timer(),同一线程再次抢_timer_mtx就是死锁
        std::vector<std::list<std::shared_ptr<TimerTask>>> expired_slots;
        std::unordered_map<uint64_t,uint64_t> expired_ids;
        {
            std::lock_guard<std::mutex> lock(_timer_mtx);
            _current_slot_id.store(0,std::memory_order_release);
            //定时器id绝不能归零重用:别的线程可能还攥着上一代拿到的id,重启后id复用会让它的cancel_timer误杀同id的新定时器
            expired_slots.swap(_slots);
            expired_ids.swap(_id_map);
        }
    }
    //把还没交给内核的SQE一次性提交出去(调用方:worker线程)
    //判断"有没有攒着"必须看SQ的真实状态,不能只看自己记的计数:别的线程提交失败时会把它清成0,
    //那样worker就会永久漏交这批SQE,对应的请求再也等不到完成事件(挂死到停机清理为止)
    void flush_submit()
    {
        std::lock_guard lock(_submit_mtx);
        if(io_uring_sq_ready(&_ring)==0){return;}
        const int ret=io_uring_submit(&_ring);
        if(ret<0)
        {
            WARN("io_uring_submit failed while flushing deferred SQEs: index={}, error={}",_index,errno_text(ret).c_str());
        }
    }
    //收割当前已经就绪的CQE
    void drain_ready()
    {
        io_uring_cqe* cqes[CQE_BATCH];
        while(true)
        {
            unsigned int readynum=io_uring_peek_batch_cqe(&_ring,cqes,CQE_BATCH);
            if(readynum==0){break;}
            process_cqes(cqes,readynum);
            io_uring_cq_advance(&_ring,readynum);
        }
    }
    //取消所有还在途的请求(不区分user_data)
    void cancel_all_pending()
    {
        //注意(运维特性):带CANCEL_ALL的这条提交是内核在提交线程的上下文里"内联"处理完所有取消的,
        //耗时与在途请求数成正比(实测约0.14ms/个:200个约2ms、5000个约100ms、20000个约2.8s),
        //这期间_submit_mtx是被占着的,drain_pending()那个2秒deadline只管它自己的循环。
        //也就是说在途请求上万时stop()可能明显超过2秒——这是取消本身的成本,不是泄漏或死锁(实测pending必定归零、forced=0)。
        //要缩短只能分批取消(每批之后放一次锁)或者接受它;不要在持锁期间做别的长操作。
        //一个cancel SQE默认只取消"一个"匹配到的请求
        //IORING_ASYNC_CANCEL_ANY只是把匹配键换成"任意请求",按文档语义它并不扩大取消数量;
        //要一次取消全部必须同时带上IORING_ASYNC_CANCEL_ALL。
        //本内核实测ANY单独就能取消全部(见回归用例s81与测试报告),但显式带上ALL才能
        //在语义为"单发"的内核上保持同样的行为。这里做成"最多重试4轮"的循环:
        //每轮发一条cancel并收割一次完成事件,正常情况下第一轮就把所有请求收回来,后面几轮直接退出。
        for(int round=0;round<4;round++)
        {
            if(_pending_count.load(std::memory_order_relaxed)<=0){break;}
            int ret=0;
            {
                std::lock_guard lock(_submit_mtx);
                io_uring_sqe* sqe=io_uring_get_sqe(&_ring);
                if(!sqe)
                {
                    io_uring_submit(&_ring);
                    sqe=io_uring_get_sqe(&_ring);
                }
                if(!sqe){break;}
                io_uring_prep_cancel64(sqe,0,IORING_ASYNC_CANCEL_ANY|IORING_ASYNC_CANCEL_ALL);
                //同上:这个SQE也必须清空user_data,否则worker会拿到残留指针
                io_uring_sqe_set_data(sqe,nullptr);
                ret=io_uring_submit(&_ring);
            }
            if(ret<0)
            {
                WARN("cannot submit a cancel-all request: index={}, error={}",_index,errno_text(ret).c_str());
                break;
            }
            //必须先把锁放掉再收割:完成事件会触发用户回调,回调里通常会重新提交请求,
            //持着_submit_mtx去dispatch会自死锁
            drain_ready();
            if(_pending_count.load(std::memory_order_relaxed)<=0){break;}
            //还有剩余,给内核一点时间处理取消,再补一轮
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    //等待在途请求被取消之后的完成事件
    void drain_pending()
    {
        auto deadline=std::chrono::steady_clock::now()+std::chrono::milliseconds(2000);
        while(_pending_count.load(std::memory_order_relaxed)>0)
        {
            auto remain=std::chrono::duration_cast<std::chrono::milliseconds>(deadline-std::chrono::steady_clock::now()).count();
            if(remain<=0)
            {
                WARN("cleanup timed out with {} requests still in flight: index={}, they will be completed with -ECANCELED so that the upper layer can release them",static_cast<long long>(_pending_count.load(std::memory_order_relaxed)),_index);
                break;
            }
            struct __kernel_timespec ts{};
            ts.tv_sec=0;
            ts.tv_nsec=100000000;//100ms
            struct io_uring_cqe* cqe=nullptr;
            //老内核(无IORING_FEAT_EXT_ARG)的io_uring_wait_cqe_timeout会在内部抢一个SQE来挂超时,它不知道_submit_mtx的存在;
            // cleanup期间别的线程仍可能调stop()/cancel_fd()抢SQE,两边不拿同一把锁就是SQ环数据竞争
            int ret=0;
            if(_has_ext_arg)
            {
                ret=io_uring_wait_cqe_timeout(&_ring,&cqe,&ts);
            }
            else
            {
                std::lock_guard lock(_submit_mtx);
                ret=io_uring_wait_cqe_timeout(&_ring,&cqe,&ts);
            }
            if(ret==-EINTR){continue;}
            if(ret==-ETIME){continue;}
            if(ret<0){break;}
            drain_ready();
        }
    }
    //把"取消之后仍然没能收回来"的请求强制收尾
    //走到这里说明上面的取消与等待都没能让它们产生CQE(例如内核侧暂时不可取消的长操作)。
    //不做这一步,登记表里的每个IoTask都会连同它持有的Buffer与捕获的Connection一起永久泄漏,
    //上层的连接数统计、关闭回调也永远不会执行。
    void force_complete_leftovers()
    {
        std::vector<IoTask*> leftovers;
        size_t untracked=0;
        {
            std::lock_guard lock(_submit_mtx);
            for(size_t i=0;i<_inflight_cap;i++)
            {
                if(_inflight_table[i]){leftovers.push_back(_inflight_table[i]);_inflight_table[i]=nullptr;}
            }
            _inflight_used=0;
            //和register_inflight_locked / clear_inflight一样必须在锁内读写:
            //读在锁外的话,和并发submit的写入就是数据竞争
            untracked=_inflight_untracked;
            _inflight_untracked=0;
        }
        if(untracked>0)
        {
            //这些请求连登记都没成功,用户态再也找不到它们,只能如实报告
            ERROR("cleanup cannot reclaim {} request(s) that were never registered: index={}, their memory is leaked",untracked,_index);
        }
        if(leftovers.empty()){return;}
        WARN("cleanup is forcing {} in-flight request(s) to complete with -ECANCELED: index={}",leftovers.size(),_index);
        _forced_count.fetch_add(leftovers.size(),std::memory_order_relaxed);
        for(IoTask* task:leftovers)
        {
            if(task->_completed.exchange(true)){continue;}
            //回调会走正常的错误路径:上层的Connection收到负返回值后关闭并释放自己的资源
            dispatch_task(task,-ECANCELED);
            _pending_count.fetch_sub(1,std::memory_order_relaxed);
            //不在这里delete:见_forced_retired的说明
            _forced_retired.push_back(task);
        }
    }


    [[nodiscard]] bool submit_task(std::unique_ptr<IoTask> task)
    {
        //失败路径上待释放的任务:声明在锁之前,析构顺序才反过来——先解锁、再delete
        std::unique_ptr<IoTask> failed_task;
        std::lock_guard lock(_submit_mtx);
        if(_state.load(std::memory_order_acquire)!=UringState::Running){return false;}
        if(task->_event==IoTask::IoEvent::TIMEOUT)//TIMEOUT事件只能通过enable_multishot_timeout提交,不允许走submit_task
        {
            ERROR("submit_task received a TIMEOUT event: index={}, only enable_multishot_timeout may submit it",_index);
            return false;
        }
        io_uring_sqe* sqe=io_uring_get_sqe(&_ring);
        if(!sqe)
        {
            //SQ环满:先把已排队的请求提交给内核腾出空间,重试一次
            io_uring_submit(&_ring);
            sqe=io_uring_get_sqe(&_ring);
        }
        if(!sqe)
        {
            ERROR("no free SQE even after submitting the queue: index={}, the request is dropped",_index);
            return false;
        }
        IoTask* raw = task.release();
        switch(raw->_event)
        {
            //READ/WRITE的偏移量直接透传_fd_offset,默认偏移量-1在io_uring里的语义是"使用文件当前偏移"
            case IoTask::IoEvent::READ:
            {
                unsigned len=static_cast<unsigned>(std::min<size_t>(raw->_buf->writable_size(),UINT_MAX));
                io_uring_prep_read(sqe,raw->_fd,raw->_buf->write_ptr(),len,static_cast<__u64>(raw->_fd_offset));
                break;
            }
            case IoTask::IoEvent::WRITE:
            {
                unsigned len=static_cast<unsigned>(std::min<size_t>(raw->_buf->readable_size(),UINT_MAX));
                io_uring_prep_write(sqe,raw->_fd,raw->_buf->read_ptr(),len,static_cast<__u64>(raw->_fd_offset));
                break;
            }
            case IoTask::IoEvent::ACCEPT:
            {
                //accept出来的fd必须带上CLOEXEC
                const int accept_flags=SOCK_NONBLOCK|SOCK_CLOEXEC;
                if(raw->_multishot)
                {
                    io_uring_prep_multishot_accept(sqe,raw->_fd,reinterpret_cast<sockaddr*>(&raw->_addr),&raw->_addr_len,accept_flags);
                }
                else
                {
                    io_uring_prep_accept(sqe,raw->_fd,reinterpret_cast<sockaddr*>(&raw->_addr),&raw->_addr_len,accept_flags);
                }
                break;
            }
            case IoTask::IoEvent::RECV:
            {
                io_uring_prep_recv(sqe,raw->_fd,raw->_buf->write_ptr(),static_cast<unsigned>(std::min<size_t>(raw->_buf->writable_size(),UINT_MAX)),0);
                break;
            }
            case IoTask::IoEvent::SEND:
            {
                io_uring_prep_send(sqe,raw->_fd,raw->_buf->read_ptr(),static_cast<unsigned>(std::min<size_t>(raw->_buf->readable_size(),UINT_MAX)),0);
                break;
            }
            case IoTask::IoEvent::SPLICE:
            {
                io_uring_prep_splice(sqe,raw->_fd,raw->_fd_offset,raw->_fd_out,raw->_fd_out_offset,raw->_splice_len,0);
                break;
            }
            case IoTask::IoEvent::RECVMSG:
            {
                if(raw->_buffer_select)
                {
                    io_uring_prep_recvmsg_multishot(sqe,raw->_fd,&raw->_msg,0);
                    sqe->flags|=IOSQE_BUFFER_SELECT;
                    sqe->buf_group=_buf_ring_bgid;
                }
                else
                {
                    io_uring_prep_recvmsg(sqe,raw->_fd,&raw->_msg,0);
                }
                break;
            }
            case IoTask::IoEvent::SENDMSG:
            {
                io_uring_prep_sendmsg(sqe,raw->_fd,&raw->_msg,0);
                break;
            }
            case IoTask::IoEvent::CLOSE:
            {
                io_uring_prep_close(sqe,raw->_fd);
                break;
            }
            case IoTask::IoEvent::POST:
            {
                io_uring_prep_nop(sqe);
                break;
            }
            case IoTask::IoEvent::POLL:
            {
                io_uring_prep_poll_add(sqe,raw->_fd,raw->_poll_mask);
                break;
            }
            default:break;
        }
        io_uring_sqe_set_data(sqe,raw);
        _pending_count.fetch_add(1,std::memory_order_relaxed);
        //worker线程自己发起的请求先攒着,等这一批CQE处理完再统一submit,这样一次回调里连发多个请求只需要付出一次io_uring_enter系统调用
        if(tls_worker_ring==this)
        {
            //worker自己发起的请求先攒着,由本批处理完之后的flush_submit() 统一交出去
            register_inflight_locked(raw);
            return true;
        }
        const int ret=io_uring_submit(&_ring);
        if(ret<0)
        {
            ERROR("io_uring_submit failed: index={}, error={}",_index,errno_text(ret).c_str());
            //提交失败,将当前SQE覆盖为无操作NOP,并清除用户数据,然后再次提交(这次不管是否提交成功)
            io_uring_prep_nop(sqe);
            io_uring_sqe_set_data(sqe,nullptr);
            io_uring_submit(&_ring);
            _pending_count.fetch_sub(1,std::memory_order_relaxed);
            //delete必须挪到锁外(见下面的failed_task):IoTask的回调可能捕获了用户对象,它的析构里若再调本ring的提交类API,持锁delete就是自死锁
            failed_task.reset(raw);
            return false;
        }
        register_inflight_locked(raw);
        return true;
    }
    bool enable_multishot_timeout(long long sec,long long nsec)
    {
        struct __kernel_timespec ts;
        ts.tv_sec=sec;//s
        ts.tv_nsec=nsec;//ns
        std::lock_guard lock(_submit_mtx);
        //已经武装着就先卸掉:重启时旧的multishot timeout如果没卸干净,再挂一个会让tick()每秒跑两次(所有定时器只等一半时间)
        if(_timeout_armed.load(std::memory_order_relaxed))
        {
            io_uring_sqe* old=io_uring_get_sqe(&_ring);
            if(old)
            {
                io_uring_prep_timeout_remove(old,reinterpret_cast<uint64_t>(_iotask_timer.get()),0);
                io_uring_sqe_set_data(old,nullptr);
                io_uring_submit(&_ring);
                _timeout_armed.store(false,std::memory_order_relaxed);
            }
            else
            {
                //卸不掉旧的绝不能再挂新的:两个multishot timeout并存会让tick()每秒跑两次,所有定时器只等一半时间
                WARN("cannot disarm the previous 1s multishot timeout: index={} has no free SQE, refusing to arm a second one",_index);
                return false;
            }
        }
        struct io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        if(!sqe){return false;}
        io_uring_prep_timeout(sqe,&ts,0,IORING_TIMEOUT_MULTISHOT);//一次调用,一直触发
        io_uring_sqe_set_data(sqe,_iotask_timer.get());
        int ret=io_uring_submit(&_ring);
        if(ret<0)
        {
            ERROR("cannot enable the 1 second multishot timeout: index={}, error={}",_index,errno_text(ret).c_str());
            return false;
        }
        _timeout_armed.store(true,std::memory_order_relaxed);
        return true;
    }
    bool disable_multishot_timeout()
    {
        std::lock_guard lock(_submit_mtx);
        io_uring_sqe* sqe=io_uring_get_sqe(&_ring);
        if(!sqe){return false;}
        io_uring_prep_timeout_remove(sqe,reinterpret_cast<uint64_t>(_iotask_timer.get()),0);
        io_uring_sqe_set_data(sqe,nullptr);//io_uring_prep_timeout_remove不会设置user_data,不显式清空的话,worker会把SQ槽位里残留的指针当成IoTask*并delete
        int ret=io_uring_submit(&_ring);
        if(ret<0)
        {
            ERROR("cannot disable the multishot timeout: index={}, error={}",_index,errno_text(ret).c_str());
            return false;
        }
        _timeout_armed.store(false,std::memory_order_relaxed);
        return true;
    }
    void tick()
    {
        try{
            std::list<std::shared_ptr<TimerTask>> slot;
            {
                std::lock_guard<std::mutex> lock(_timer_mtx);
                uint32_t current_slot_id=_current_slot_id.load(std::memory_order_relaxed);//取出当前位置的任务槽
                if(current_slot_id>=_slots.size()){return;}//时间轮已经被cleanup()清空
                slot.swap(_slots[current_slot_id]);
                for(auto& task:slot){_id_map.erase(task->_id);}//在锁内删除任务
                _current_slot_id.store((current_slot_id+1)%_capacity,std::memory_order_relaxed);//推进秒针
            }
            //处理当前位置任务
            for(auto& task:slot)
            {
                try{
                    if(task->_timed_callback){task->_timed_callback();}
                }catch(...){
                    ERROR("timer callback threw an unexpected exception: index={}, timer_id={}",_index,static_cast<unsigned long long>(task->_id));
                }
            }
            slot.clear();
        }catch(...){
            ERROR("unexpected exception while advancing the timer wheel: index={}",_index);
        }
    }
};
//thread_local成员的定义(header-only用inline避免多TU重复定义)
inline thread_local IoUring* IoUring::tls_worker_ring=nullptr;
