#pragma once
#include "log.hpp"
#include "buffer.hpp"
#include "iouring.hpp"
#include "socket.hpp"
#include <deque>
#include <thread>

//UDP每个ring默认的接收缓冲深度(同时在途的recvmsg数量)
constexpr size_t UDP_RECV_DEPTH=4;
//IPv4下UDP的最大负载
constexpr size_t UDP_MAX_DATAGRAM_SIZE=65507;
//IPv6下UDP的最大负载比IPv4多20字节(IPv6头更长,没有IPv4的65535-20限制)
//接收缓冲如果按下限配到65507,IPv6的65527字节数据报会被静默截断20字节
constexpr size_t UDP_MAX_DATAGRAM_SIZE_V6=65527;
//下面这些上限之间有真实耦合:写错就是"内核静默截断数据报"或"上限形同虚设"
static_assert(UDP_MAX_DATAGRAM_SIZE_V6>UDP_MAX_DATAGRAM_SIZE,"the IPv6 datagram limit must exceed the IPv4 one");
//每个ring的发送缓冲池上限(条数)
constexpr size_t UDP_SEND_POOL_SIZE=1024;
//每个ring的发送缓冲池上限(总字节)
//只按条数封顶挡不住"被大报文撑大的缓冲长期占着内存":Buffer的reset()不会缩容,
//1024条64KB的缓冲就是64MB,4个ring能长期占住256MB,而且不需要任何背压就能做到
constexpr size_t UDP_SEND_POOL_BYTES=4*1024*1024;
//每个ring允许同时在途的发送条数
//关键:io_uring在sendmsg拿到-EAGAIN时会把请求挂在POLLOUT上一直持有(不产生完成事件),
//对端不读回包时在途请求会随流量无限增长,不封顶就是一条远端可触发的OOM路径
constexpr size_t UDP_MAX_INFLIGHT_SENDS=4096;
//每个ring允许同时在途的发送总字节(按缓冲容量算):大报文场景由这一条封顶,小报文场景由条数封顶
constexpr size_t UDP_MAX_INFLIGHT_SEND_BYTES=8*1024*1024;
//单个接收缓冲的大小上限:UDP一个缓冲只装一个数据报,真实数据报最大也就65527字节,1MB已经远远够用。
//这个上限是安全必需的:multishot路径要算sizeof(io_uring_recvmsg_out)+sizeof(sockaddr_storage)+_recv_buffer_size,
//_recv_buffer_size接近SIZE_MAX时这个加法会回绕成几十字节,内核之后每次recvmsg都返回-EFAULT
constexpr size_t UDP_RECV_BUFFER_MAX=1024*1024;
static_assert(UDP_SEND_POOL_SIZE<=UDP_MAX_INFLIGHT_SENDS,"the send pool cannot exceed the in-flight cap");
static_assert(UDP_SEND_POOL_BYTES<=UDP_MAX_INFLIGHT_SEND_BYTES,"the send pool byte cap cannot exceed the in-flight byte cap");
static_assert(UDP_RECV_BUFFER_SIZE>=UDP_MAX_DATAGRAM_SIZE,"the receive buffer must hold the largest IPv4 datagram");
static_assert(UDP_RECV_BUFFER_SIZE>=UDP_MAX_DATAGRAM_SIZE_V6,"the receive buffer must hold the largest IPv6 datagram");

//关于析构:对象的析构顺序是"派生类先、基类后",而worker线程是在基类析构(~UdpServer)里才被停掉的。
//所以如果你的派生类还有堆成员,而析构时还有在途数据报,worker可能正在用户回调里用那些成员 —— 那时派生部分已经析构,就是use-after-free。
//正确做法(强制要求):在派生类析构函数的第一件事里调用stop()。
//  ~MyServer(){ stop(); }        // 之后再让基类析构去收尾,此时worker已经停了
//stop()在worker回调里调用是安全的(worker上的wait()会推迟join),但在回调里直接delete本对象仍然不支持。
class UdpServer
{
public:
    enum class ServerState
    {
        Stopped,        //已停止(初始/结束)
        Starting,       //正在启动(初始化socket,uring,线程)
        Running,        //运行中(正常服务)
        Paused,         //暂停接收新数据报
        Stopping,       //正在停止(优雅关闭,释放资源)
        Failed          //启动失败(异常)
    };
protected:
    Socket                      _socket;
    std::vector<IoUring::Ptr>   _urings;//IoUring::Ptr自带删除器
    //Buffer对象池,必须使用shared_ptr<BufferPool>这样才能自动回收对象
    std::shared_ptr<BufferPool> _buffer_pool{std::make_shared<BufferPool>()};
    std::atomic<ServerState>    _state{ServerState::Stopped};
    //正在析构:见析构函数的注释(派生类析构完成后基类析构里的stop()会让回调落在已退化的vtable上)
    std::atomic_bool             _destroying{false};
private:
    //每个ring一个"独占缓存行"的标志位与计数器:直接用std::atomic_bool[]会让几个ring的变量挤在同一缓存行里互相打无效(伪共享),用alignas隔开
    struct alignas(64) PaddedFlag{std::atomic_bool value{false};};
    struct alignas(64) PaddedCounter{std::atomic_int value{0};};
    //配置项:全部在start()之前设置,运行期只读
    //原子:set_recv_buffer_size()可能在运行期调用,而worker热路径会读它(池阈值/multishot的buf_size/截断告警)
    std::atomic_size_t _recv_buffer_size{UDP_RECV_BUFFER_SIZE};
    size_t           _recv_depth{UDP_RECV_DEPTH};
    //是否使用"multishot recvmsg+provided buffer ring":每个ring只提交一次recvmsg,内核持续投递数据报,
    //并直接从缓冲环里挑一块缓冲,同时把io_uring_recvmsg_out+源地址+payload一起写进去(所以源地址仍然拿得到)
    std::atomic_bool _recv_multishot{false};
    size_t           _recv_bufring_entries{32};//缓冲环里的缓冲数量(会被向上取整到2的幂)
    int              _socket_buffer_size{UDP_SOCKET_BUFFER_SIZE};
    bool             _multi_socket{true};//是否每个IoUring一个socket+SO_REUSEPORT(单线程时等价于单socket)
    //端口共享策略:实测本内核下UDP服务端只要开着SO_REUSEADDR或SO_REUSEPORT,另一个进程就能用同样的选项bind同一端口把流量分走,
    //所以默认口径是"单socket模式两个都不开",只有多socket模式(内核按四元组分发,必须共享端口)才打开
    std::atomic_bool _shared_port{false};
    std::atomic_bool _shared_port_explicit{false};//是否被set_shared_port()显式指定过
    //本次启动实际采用的口径(启动时定好,之后只读)
    //不能现场用_urings.size()判定:socket是在init_urings()之前创建的,那时_urings还是空的,现场判定会把多socket模式误判成单socket,
    //主socket不带SO_REUSEPORT,额外socket就绑不上同一端口,多socket模式静默退化成单socket
    std::atomic_bool _shared_port_effective{false};
    //每个ring一组接收缓冲:UDP是数据报协议,recvmsg的长度不够时内核会直接把数据报截断并且不报错,所以接收缓冲必须能放下UDP的最大负载
    //用"空闲缓冲池+多个在途recvmsg"代替前后台双缓冲:每个在途recvmsg独占一块缓冲,深度可配,慢回调期间内核仍然可以继续收包
    std::vector<std::deque<std::shared_ptr<Buffer>>> _recv_free;
    //_recv_free是唯一一个"可能被非本ring线程访问"的池子:启动阶段由main线程refill,运行期由本ring的worker取/放,resume()又是外部线程调的,
    //而std::deque并发读开会直接破坏内部指针,所以按index配一把锁(临界区只有一次deque取/放,稳态下竞争几乎为零)
    //用std::mutex而不是自旋锁:实测两者在本形态(4线程各锁各的、锁内一次deque取/放)下同速(约23-25ns/次),差异落在轮间波动内,
    //而mutex在持有者被抢占时会park,不会让其他线程空转
    std::unique_ptr<std::mutex[]>                    _recv_free_mtx;
    //每个ring一组可复用的发送缓冲:每回一个包就make_shared<Buffer>()要付一次控制块分配+一次vector分配+一次拷贝,高pps下分配器就是瓶颈
    std::vector<std::deque<std::shared_ptr<Buffer>>> _send_free;
    //与_send_free配套,分别统计池内已占字节与在途发送的条数/字节,全部由_send_mtx[idx]保护,不加额外锁
    std::vector<size_t>                              _send_pool_bytes;
    std::vector<size_t>                              _inflight_send_count;
    std::vector<size_t>                              _inflight_send_bytes;
    std::unique_ptr<std::mutex[]>                    _send_mtx;
    //每个ring是否已经有一个在途的multishot recvmsg:pause()/resume()不会取消在途请求,没有这个标记的话resume()会再挂一个,
    //于是同一个socket上长期存在两个multishot接收,缓冲消耗翻倍、数据被两个请求瓜分
    std::unique_ptr<PaddedFlag[]>    _recvmsg_inflight;
    //每个ring连续遇到多少次"缓冲用光":内核按批投递,一批用光缓冲后必然报一次-ENOBUFS,此时回收已经把缓冲还回去了,应当立刻重挂,
    //只有连续多次"立刻重挂又立刻失败"才说明缓冲真的不够,这时才退避,避免100%CPU忙循环;不能依赖io_uring_buf_ring_available(),
    //它需要内核支持IORING_REGISTER_PBUF_STATUS,本机实测返回-EINVAL,拿它做判据会把正常情况误判成缓冲耗尽
    std::unique_ptr<PaddedCounter[]> _recvmsg_enobufs_streak;
    std::atomic_uint32_t             _next_uring{0};//发送负载均衡
    //统计与限速时间戳
    std::atomic_uint64_t _send_backpressure_drops{0};
    std::atomic_uint64_t _paused_drop_count{0};//暂停期间丢弃的数据报数
    std::atomic_uint64_t _truncate_count{0};
    std::atomic_uint64_t _send_reject_count{0};//因为服务不在Running而被直接拒发的次数
    std::atomic_uint64_t _send_error_count{0};//提交成功但完成事件报错的次数(权限/地址族/路由等)
    std::atomic_uint64_t _recv_bad_result_count{0};//接收完成事件本身不合法(取不到缓冲/解析失败)而丢弃的次数
    std::atomic<int64_t> _last_recv_bad_warn_ns{0};
    std::atomic<int64_t> _last_backpressure_warn_ns{0};
    std::atomic<int64_t> _last_truncate_warn_ns{0};
    std::atomic<int64_t> _last_send_err_warn_ns{0};
    //发送失败回调
    std::mutex                                   _send_error_cb_mtx;
    std::function<void(const sockaddr_in&,int)>  _send_error_callback;
    std::function<void(const sockaddr_in6&,int)> _send_error_callback_v6;
    //多socket模式:每个IoUring线程一个socket,全部以SO_REUSEPORT绑定同一端口
    //好处是消除单socket的内核锁竞争,接收缓冲区随线程数成倍放大;代价是内核按四元组哈希分发,单一来源的流量只会落到其中一个socket
    std::vector<std::unique_ptr<Socket>> _extra_sockets;//除主socket之外额外创建的socket
public:
    UdpServer(const UdpServer&)=delete;
    UdpServer& operator=(const UdpServer&)=delete;
    UdpServer(UdpServer&&)=delete;
    UdpServer& operator=(UdpServer&&)=delete;
    explicit UdpServer(uint16_t port,const std::string& ip="0.0.0.0"):_socket(port,ip){}
    ~UdpServer()
    {
        //先立起"正在析构"的旗子:派生类先析构、基类析构里才stop(),此时vtable已退化成基类。
        //on_recv故意做成非纯虚(见声明处注释),落在这个窗口里的回调最坏是一次无害的空调用,不会pure virtual abort。
        //注意:在回调里直接delete本对象是不支持的——析构返回后完成路径还会继续访问
        //this(归还缓冲/重挂接收),那是use-after-free。要在回调里停服务请调stop():worker线程上的wait()会推迟join
        _destroying.store(true,std::memory_order_release);
        (void)stop();
    }
    //析构期间的四个用户回调统一走这几个非虚转发(见析构函数注释)
    void dispatch_recv(std::shared_ptr<Buffer> buf,const sockaddr_in& src,IoUring* uring)
    {
        if(_destroying.load(std::memory_order_acquire)){return;}
        on_recv(std::move(buf),src,uring);
    }
    void dispatch_recv_v6(std::shared_ptr<Buffer> buf,const sockaddr_in6& src,IoUring* uring)
    {
        if(_destroying.load(std::memory_order_acquire)){return;}
        on_recv_v6(std::move(buf),src,uring);
    }
    void dispatch_recv_view(const char* data,size_t len,const sockaddr_in& src,IoUring* uring)
    {
        if(_destroying.load(std::memory_order_acquire)){return;}
        on_recv_view(data,len,src,uring);
    }
    void dispatch_recv_view_v6(const char* data,size_t len,const sockaddr_in6& src,IoUring* uring)
    {
        if(_destroying.load(std::memory_order_acquire)){return;}
        on_recv_view_v6(data,len,src,uring);
    }
    //控制:start/stop/pause/resume与状态查询
    [[nodiscard]] bool start(size_t thread_num)
    {
        if(thread_num==0)
        {
            ERROR("udp server start failed: threads must be greater than 0, got {}",thread_num);
            return false;
        }
        ServerState expected=ServerState::Stopped;
        if(!_state.compare_exchange_strong(expected,ServerState::Starting,std::memory_order_acq_rel,std::memory_order_acquire))
        {
            //Failed也允许重新启动:一次"端口被占用/缓冲环建不起来"之类的失败不该让这个对象永久不可用
            expected=ServerState::Failed;
            if(!_state.compare_exchange_strong(expected,ServerState::Starting,std::memory_order_acq_rel,std::memory_order_acquire))
            {
                ERROR("udp server start ignored: state={}, expected stopped or failed",static_cast<int>(_state.load(std::memory_order_acquire)));
                return false;
            }
        }
        try{
            INFO("udp server starting: listen={}:{}, threads={}, recv_multishot={}, socket_buffer={}",_socket.get_ip().c_str(),static_cast<unsigned>(_socket.get_port()),thread_num,_recv_multishot.load(std::memory_order_acquire)?1:0,_socket_buffer_size);
            //端口共享策略(见_shared_port成员说明):多socket模式必须共享端口才能让多个socket绑同一端口;
            //单socket模式则两个选项都不开,否则同机任何本地用户都能bind同一端口把流量分走
            const bool shared=_shared_port_explicit.load(std::memory_order_acquire)?_shared_port.load(std::memory_order_acquire):(_multi_socket&&thread_num>1);
            _shared_port_effective.store(shared,std::memory_order_release);
            _socket.set_reuseaddr(shared);
            _socket.set_reuseport(shared);
            _socket.set_recv_buffer_size(_socket_buffer_size);
            _socket.set_send_buffer_size(_socket_buffer_size);
            INFO("udp socket sharing: shared_port={}, reuseaddr={}, reuseport={} (threads={}, multi_socket={})",shared?1:0,_socket.get_reuseaddr()?1:0,_socket.get_reuseport()?1:0,thread_num,_multi_socket?1:0);
            if(!_socket.create_udp_server(false))
            {
                ERROR("cannot create the UDP socket: listen={}:{}",_socket.get_ip().c_str(),static_cast<unsigned>(_socket.get_port()));
                _socket.close();
                _state.store(ServerState::Failed,std::memory_order_release);
                return false;
            }
            //把内核实际生效的socket缓冲区大小打出来:net.core.rmem_max/wmem_max会把请求值悄悄截断,不打出来就无法判断"调大缓冲区"到底有没有生效
            {
                int rcv=-1;
                int snd=-1;
                socklen_t len=sizeof(int);
                getsockopt(_socket.get_fd(),SOL_SOCKET,SO_RCVBUF,&rcv,&len);
                len=sizeof(int);
                getsockopt(_socket.get_fd(),SOL_SOCKET,SO_SNDBUF,&snd,&len);
                if(rcv<_socket_buffer_size||snd<_socket_buffer_size)
                {
                    WARN("the kernel capped the UDP socket buffer: requested={} bytes, actual_receive={} bytes, actual_send={} bytes",_socket_buffer_size,rcv,snd);
                }
                INFO("UDP socket buffer in use: receive={} bytes, send={} bytes, requested={} bytes",rcv,snd,_socket_buffer_size);
            }
            if(!init_urings(thread_num))
            {
                close_all_sockets();
                _state.store(ServerState::Failed,std::memory_order_release);
                return false;
            }
            //provided buffer ring必须在worker启动之前建好
            if(!setup_recv_multishot_buffers())
            {
                close_all_sockets();
                _state.store(ServerState::Failed,std::memory_order_release);
                return false;
            }
            //多socket模式:再创建thread_num-1个socket,全部以SO_REUSEPORT绑定同一端口
            //必须在start_urings()之前:worker会读_extra_sockets,之后再push_back就是对vector的数据竞争
            if(!setup_extra_sockets())
            {
                close_all_sockets();
                stop_urings();
                _state.store(ServerState::Failed,std::memory_order_release);
                return false;
            }
            //为每个IoUring准备收发缓冲池(multishot接收时不需要软件接收缓冲池)
            //同样必须在start_urings()之前:worker一起来就会读_send_mtx/_recvmsg_inflight这些指针,
            //之后再赋值就是对指针本身的数据竞争(worker可能读到半写状态甚至旧指针)
            _recv_free.clear();
            _send_free.clear();
            _recv_free.resize(_urings.size());
            _send_free.resize(_urings.size());
            _send_pool_bytes.assign(_urings.size(),0);
            _inflight_send_count.assign(_urings.size(),0);
            _inflight_send_bytes.assign(_urings.size(),0);
            _recvmsg_inflight=std::make_unique<PaddedFlag[]>(_urings.size());
            _recvmsg_enobufs_streak=std::make_unique<PaddedCounter[]>(_urings.size());
            _send_mtx=std::make_unique<std::mutex[]>(_urings.size());
            _recv_free_mtx=std::make_unique<std::mutex[]>(_urings.size());
            if(!_recv_multishot.load(std::memory_order_acquire))
            {
                for(size_t i=0;i<_urings.size();i++)
                {
                    for(size_t k=0;k<_recv_depth;k++)
                    {
                        _recv_free[i].push_back(std::make_shared<Buffer>(_recv_buffer_size.load(std::memory_order_relaxed),_recv_buffer_size.load(std::memory_order_relaxed)));
                    }
                }
            }
            if(!start_urings())
            {
                close_all_sockets();
                _state.store(ServerState::Failed,std::memory_order_release);
                return false;
            }
            //必须先把状态发布成Running,再挂接收
            //worker线程早就在跑了,如果先挂接收后发布,端口上已经排队的数据报会立刻完成事件,
            //此时on_recvmsg_complete看到的状态还是Starting,会丢弃数据并且不再重提交,
            //经SO_REUSEPORT哈希到该socket的流就永久丢包了
            _state.store(ServerState::Running,std::memory_order_release);
            //为每个IoUring提交接收请求
            for(size_t i=0;i<_urings.size();i++)
            {
                if(_recv_multishot.load(std::memory_order_acquire))
                {
                    if(!submit_recvmsg_multishot(i))
                    {
                        ERROR("cannot submit the initial multishot recvmsg: index={}, fd={}",i,get_socket(i).get_fd());
                        //回滚顺序与stop()一致:先停ring再关socket,反过来worker可能拿着已关闭的fd号提交请求
                        stop_urings();
                        close_all_sockets();
                        _state.store(ServerState::Failed,std::memory_order_release);
                        return false;
                    }
                    continue;
                }
                refill_recv(i);
                //这里的size()也必须走加锁的读法:worker已经在跑,可能正在push_back归还缓冲
                size_t free_count=0;
                {
                    std::lock_guard<std::mutex> lock(_recv_free_mtx[i]);
                    free_count=_recv_free[i].size();
                }
                if(free_count>=_recv_depth)
                {
                    ERROR("cannot submit the initial recvmsg: index={}, fd={}",i,get_socket(i).get_fd());
                    //回滚顺序与stop()一致:先停ring再关socket
                    stop_urings();
                    close_all_sockets();
                    _state.store(ServerState::Failed,std::memory_order_release);
                    return false;
                }
            }
            INFO("udp server is running: sockets={}, per_socket_buffer={}, recv_depth={}, recv_multishot={}, kernel_socket_buffer={}",socket_count(),_recv_buffer_size.load(std::memory_order_relaxed),_recv_depth,_recv_multishot.load(std::memory_order_acquire)?1:0,_socket_buffer_size);
            return true;
        }catch(const std::exception&e){
            ERROR("udp server start failed: {}",e.what());
            stop_urings();
            close_all_sockets();
            _state.store(ServerState::Failed,std::memory_order_release);
            return false;
        }catch (...){
            ERROR("udp server start failed with an unexpected exception");
            stop_urings();
            close_all_sockets();
            _state.store(ServerState::Failed,std::memory_order_release);
            return false;
        }
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
                WARN("udp server stop() rejected: the server is still starting; wait for start() to finish first");
                return false;
            }
            _state.store(ServerState::Stopping,std::memory_order_release);
        }
        //顺序很重要:必须先停ring(其cleanup会取消所有在途recvmsg/sendmsg并等回完成事件),再关socket
        //反过来的话,worker可能刚读到fd号、正准备提交sendmsg,而这里已经把它close掉、fd号甚至已经被别的对象复用,就会把数据发到别的fd上
        //先停ring不会丢包:停的过程中socket仍开着,数据报只是排在接收队列里
        stop_urings();
        close_all_sockets();
        _state.store(ServerState::Stopped,std::memory_order_release);
        INFO("udp server stopped: truncated_datagrams={}, send_backpressure_drops={}, paused_drops={}",static_cast<unsigned long long>(_truncate_count.load(std::memory_order_relaxed)),static_cast<unsigned long long>(_send_backpressure_drops.load(std::memory_order_relaxed)),static_cast<unsigned long long>(_paused_drop_count.load(std::memory_order_relaxed)));
        return true;
    }
    bool pause()
    {
        auto expected=ServerState::Running;
        return _state.compare_exchange_strong(expected,ServerState::Paused,std::memory_order_acq_rel,std::memory_order_acquire);
    }
    [[nodiscard]] bool resume()
    {
        ServerState expected=ServerState::Paused;
        if(!_state.compare_exchange_strong(expected,ServerState::Running,std::memory_order_acq_rel,std::memory_order_acquire)){return false;}
        for(size_t i=0;i<_urings.size();i++)
        {
            if(_recv_multishot.load(std::memory_order_acquire))
            {
                //resume是multishot终止后的唯一重挂点:失败绝不能静默吞掉,否则这个socket永久停收且无日志;失败就挂1秒重试(与ENOBUFS退避同一套路)
                if(!submit_recvmsg_multishot(i))
                {
                    ERROR("cannot rearm the multishot recvmsg on resume: index={}, retrying in 1 second",i);
                    if(_urings[i]->add_timer(TIMER_TICK,[this,i]()
                       {
                           const ServerState st=_state.load(std::memory_order_acquire);
                           if(st==ServerState::Running||st==ServerState::Paused){submit_recvmsg_multishot(i);}
                       })==0)
                    {
                        ERROR("cannot arm the resume retry timer for the multishot recvmsg: index={}, that socket stops receiving",i);
                    }
                }
            }
            //每个在途recvmsg独占一块缓冲,按空闲缓冲数量补齐即可,不会重复挂同一块内存
            else{refill_recv(i);}
        }
        return true;
    }
    ServerState state()const{return _state.load(std::memory_order_acquire);}
    bool is_running()const{return _state.load(std::memory_order_acquire)==ServerState::Running;}
    //参数设置类:接收深度/缓冲区/多socket/端口共享/发送失败回调
    //设置单个接收缓冲区的大小,必须能放下业务上最大的数据报(IPv4下为65507),否则会被内核静默截断
    void set_recv_buffer_size(size_t size)
    {
        //下限按监听地址族取:IPv6的最大UDP负载是65527,比IPv4多20字节。
        //按下限配的时候如果一律用IPv4的值,v6上的最大包会被截断20字节(只有一条限速WARN,数据已经不完整)
        const size_t minimum=is_ipv6()?UDP_MAX_DATAGRAM_SIZE_V6:UDP_MAX_DATAGRAM_SIZE;
        if(size<minimum)
        {
            WARN("UDP receive buffer size raised: requested={} bytes, minimum={} bytes ({})",size,minimum,is_ipv6()?"IPv6":"IPv4");
            size=minimum;
        }
        //上限也要钳:SIZE_MAX这类值会让multishot的缓冲大小计算回绕,内核每次recvmsg都-EFAULT
        if(size>UDP_RECV_BUFFER_MAX)
        {
            WARN("UDP receive buffer size lowered: requested={} bytes, maximum={} bytes",size,UDP_RECV_BUFFER_MAX);
            size=UDP_RECV_BUFFER_MAX;
        }
        _recv_buffer_size.store(size,std::memory_order_release);
    }
    //设置每个ring同时在途的recvmsg数量(默认4):深度越大抗慢回调的能力越强,内存占用也越高,必须在start()之前调用
    void set_recv_depth(size_t depth)
    {
        if(depth<1){depth=1;}
        if(depth>64){depth=64;}
        _recv_depth=depth;
    }
    //是否启用multishot recvmsg+provided buffer ring(默认关闭,必须在start()之前调用):打开后每个ring只挂一个recvmsg,回调走on_recv_view(零拷贝视图)
    void set_recv_multishot(bool enable){_recv_multishot.store(enable,std::memory_order_release);}
    //缓冲环里的缓冲数量(会被向上取整到2的幂,默认32)
    void set_recv_bufring_entries(size_t entries){if(entries>=2){_recv_bufring_entries=entries;}}
    //设置内核socket收发缓冲区大小:UDP没有流量控制,服务端来不及收包时内核直接丢包,默认缓冲区很容易被打满
    //实际生效值会被net.core.rmem_max/wmem_max截断,start()时会把真实值打出来
    void set_socket_buffer_size(int size)
    {
        if(size<4096){size=4096;}
        _socket_buffer_size=size;
    }
    //是否启用"每个IoUring一个socket+SO_REUSEPORT"(默认开启,单线程时等价于单socket)
    void set_multi_socket(bool enable){_multi_socket=enable;}
    //IPv6监听地址(如"::")是否只接受v6数据报(默认true)
    void set_v6only(bool enable){_socket.set_v6only(enable);}
    //显式要求"端口可被共享"(多进程绑同一端口):默认不设置,多socket模式自动共享(那是设计必需),单socket模式独占端口
    void set_shared_port(bool enable)
    {
        _shared_port.store(enable,std::memory_order_release);
        _shared_port_explicit.store(true,std::memory_order_release);
    }
    //注册发送失败回调:sendto()的返回值只代表"是否成功提交",真正的发送结果在完成事件里才知道,没有注册回调时库会按每秒一条的频率打WARN
    void set_send_error_callback(std::function<void(const sockaddr_in&,int)> cb)
    {
        std::lock_guard<std::mutex> lock(_send_error_cb_mtx);
        _send_error_callback=std::move(cb);
    }
    //IPv6版本的发送失败回调
    void set_send_error_callback_v6(std::function<void(const sockaddr_in6&,int)> cb)
    {
        std::lock_guard<std::mutex> lock(_send_error_cb_mtx);
        _send_error_callback_v6=std::move(cb);
    }
    //观测类:配置回读/池子与在途统计/丢弃计数
    //当前接收缓冲大小的配置值(便于观测"下限被抬到多少")
    size_t recv_buffer_size()const{return _recv_buffer_size.load(std::memory_order_acquire);}
    size_t get_recv_depth()const{return _recv_depth;}
    bool get_recv_multishot()const{return _recv_multishot.load(std::memory_order_acquire);}
    size_t get_recv_bufring_entries()const{return _recv_bufring_entries;}
    //监听地址是否是IPv6(按构造时传入的IP字符串判定)
    bool is_ipv6()const{return _socket.is_ipv6();}
    bool is_shared_port()const{return _shared_port_effective.load(std::memory_order_acquire);}
    //某个uring对应的监听socket,索引越界时回退到主socket
    Socket& get_socket(size_t idx)
    {
        if(idx==0||idx-1>=_extra_sockets.size()){return _socket;}
        return *_extra_sockets[idx-1];
    }
    size_t socket_count()const{return 1+_extra_sockets.size();}
    //被接收缓冲区上限截断的数据报数
    uint64_t truncated_datagrams()const{return _truncate_count.load(std::memory_order_relaxed);}
    //服务不在Running时被拒发的次数:sendto()只返回false、不留痕迹,靠这个计数才能发现
    uint64_t send_rejected()const{return _send_reject_count.load(std::memory_order_relaxed);}
    //提交成功但完成事件报错的次数(没注册回调时只有限速WARN)
    uint64_t send_errors()const{return _send_error_count.load(std::memory_order_relaxed);}
    //接收完成事件本身不合法而丢弃的次数(provided buffer取不到/recvmsg结构解析失败)
    uint64_t recv_bad_results()const{return _recv_bad_result_count.load(std::memory_order_relaxed);}
    //因为发送背压(在途条数/字节超过上限)被丢弃的数据报数:非0说明对端读得比服务端发得慢,或者网卡/内核发送队列已经堵住
    uint64_t send_backpressure_drops()const{return _send_backpressure_drops.load(std::memory_order_relaxed);}
    //暂停(pause)期间收到并被丢弃的数据报数
    uint64_t paused_drops()const{return _paused_drop_count.load(std::memory_order_relaxed);}
    //当前某个ring上在途的发送条数与字节数(观测用)
    bool inflight_send_stats(size_t idx,size_t& count,size_t& bytes)
    {
        if(idx>=_send_free.size()||!_send_mtx){return false;}
        std::lock_guard<std::mutex> lock(_send_mtx[idx]);
        count=_inflight_send_count[idx];
        bytes=_inflight_send_bytes[idx];
        return true;
    }
    //当前某个ring的发送缓冲池占用的条数与字节数(观测用)
    bool send_pool_stats(size_t idx,size_t& count,size_t& bytes)
    {
        if(idx>=_send_free.size()||!_send_mtx){return false;}
        std::lock_guard<std::mutex> lock(_send_mtx[idx]);
        count=_send_free[idx].size();
        bytes=_send_pool_bytes[idx];
        return true;
    }
    //发送:带uring的重载让回包走收到该数据报的uring与socket,发送与接收在同一个线程上,既避免了跨线程往别人ring里压SQE的锁竞争,也让socket与uring的绑定保持一对一
    bool sendto(const char* data,size_t len,const sockaddr_in& dest){return sendto(data,len,dest,nullptr);}
    bool sendto(std::span<const char> data,const sockaddr_in& dest,IoUring* uring){return sendto(data.data(),data.size(),dest,uring);}
    bool sendto(std::span<const char> data,const sockaddr_in6& dest,IoUring* uring){return sendto(data.data(),data.size(),dest,uring);}
    [[nodiscard]] bool sendto(const char* data,size_t len,const sockaddr_in& dest,IoUring* uring){sockaddr_storage storage{};socklen_t storage_len=sockaddr_to_storage(dest,storage);return sendto_storage(data,len,storage,storage_len,uring);}
    [[nodiscard]] bool sendto(const char* data,size_t len,const sockaddr_in6& dest,IoUring* uring){sockaddr_storage storage{};socklen_t storage_len=sockaddr_to_storage(dest,storage);return sendto_storage(data,len,storage,storage_len,uring);}
    bool sendto(const std::string& data,const sockaddr_in& dest){return sendto(data.data(),data.size(),dest,nullptr);}
    bool sendto(const std::string& data,const sockaddr_in6& dest){return sendto(data.data(),data.size(),dest,nullptr);}
    bool sendto(const Buffer& buf,const sockaddr_in& dest){return sendto(buf.read_ptr(),buf.readable_size(),dest,nullptr);}
    bool sendto(const Buffer& buf,const sockaddr_in& dest,IoUring* uring){return sendto(buf.read_ptr(),buf.readable_size(),dest,uring);}
    bool sendto(const Buffer& buf,const sockaddr_in6& dest){return sendto(buf.read_ptr(),buf.readable_size(),dest,nullptr);}
    bool sendto(const Buffer& buf,const sockaddr_in6& dest,IoUring* uring){return sendto(buf.read_ptr(),buf.readable_size(),dest,uring);}
    //发送缓冲池
    //取一块发送缓冲,并在同一次加锁里完成背压登记:UDP发送是每包一次的极热路径,分成两次加锁会平白多一次锁开销
    //backpressure为true表示是被在途上限挡下来的(调用方应当丢包并计数),而不是分配失败
    std::shared_ptr<Buffer> acquire_send_buffer(size_t idx,size_t len,bool& backpressure)
    {
        backpressure=false;
        if(idx>=_send_free.size()||!_send_mtx){return nullptr;}
        std::lock_guard<std::mutex> lock(_send_mtx[idx]);
        if(_inflight_send_count[idx]>=UDP_MAX_INFLIGHT_SENDS)
        {
            backpressure=true;
            return nullptr;
        }
        std::shared_ptr<Buffer> buf;
        if(!_send_free[idx].empty())
        {
            buf=_send_free[idx].front();
            _send_free[idx].pop_front();
            size_t bytes=buf->capacity();
            _send_pool_bytes[idx]=(bytes<=_send_pool_bytes[idx])?(_send_pool_bytes[idx]-bytes):0;
        }
        else
        {
            try{
                buf=std::make_shared<Buffer>();
            }catch(const std::bad_alloc&){
                return nullptr;
            }
        }
        //先把容量撑到本次数据报所需大小,这样后面的Write不会再改变Size()
        //(取缓冲时按Size()记在途字节,归还时也按Size()扣,"同一个数字"才不会出现计数漂移)
        if(buf->capacity()<len)
        {
            try{
                buf->reserve(len);
            }catch(const std::exception& e){
                ERROR("cannot reserve {} bytes in a UDP send buffer: {}, the datagram is dropped",len,e.what());
                return_send_buffer_locked(idx,buf,false);
                return nullptr;
            }
        }
        //字节上限必须在容量确定之后再判,而且和下面的累加用同一个数字(capacity):
        //原来检查用len、累加用capacity,池里有大缓冲时一个小包也按大容量计入,会提前触发背压
        if(_inflight_send_bytes[idx]+buf->capacity()>UDP_MAX_INFLIGHT_SEND_BYTES)
        {
            backpressure=true;
            return_send_buffer_locked(idx,buf,false);
            return nullptr;
        }
        _inflight_send_count[idx]++;
        _inflight_send_bytes[idx]+=buf->capacity();
        return buf;
    }
    //把发送缓冲还回池子:inflight表示这块缓冲是否已经登记过在途发送(提交失败/写失败时可能还没登记)
    //inflight默认true:配对的是acquire_send_buffer()(它返回缓冲时已经登记了在途)。
    //默认false会让在途计数只增不减,之后本ring所有sendto都被判成背压丢包
    void return_send_buffer(size_t idx,const std::shared_ptr<Buffer>& buf,bool inflight=true)
    {
        if(idx>=_send_free.size()||!_send_mtx){return;}
        std::lock_guard<std::mutex> lock(_send_mtx[idx]);
        return_send_buffer_locked(idx,buf,inflight);
    }
protected:
    //v4数据报回调。故意不是纯虚(和TcpServer的三个回调同一个原因):
    //基类析构里stop()收尾时,vtable已退化回本类,worker若恰好通过了_destroying检查再来调纯虚on_recv就是直接abort;
    //空实现把最坏情况退化成"丢一个包+一条限速告警"
    virtual void on_recv(std::shared_ptr<Buffer> buf,const sockaddr_in& src_addr,IoUring* uring)
    {
        (void)buf;
        (void)uring;
        char ip_buf[INET_ADDRSTRLEN]={0};
        inet_ntop(AF_INET,&src_addr.sin_addr,ip_buf,sizeof(ip_buf));
        WARN("dropping an IPv4 datagram from [{}]:{} because on_recv is not overridden",ip_buf,ntohs(src_addr.sin_port));
    }
    //v6数据报回调(默认丢弃并告警):只有监听v6地址时才会被调用,如果你的服务只跑v4,可以不实现它
    virtual void on_recv_v6(std::shared_ptr<Buffer> buf,const sockaddr_in6& src_addr,IoUring* uring)
    {
        (void)buf;
        (void)uring;
        char ip_buf[INET6_ADDRSTRLEN]={0};
        inet_ntop(AF_INET6,&src_addr.sin6_addr,ip_buf,sizeof(ip_buf));
        WARN("dropping an IPv6 datagram from [{}]:{} because on_recv_v6 is not overridden",ip_buf,ntohs(src_addr.sin6_port));
    }
    //multishot路径的零拷贝回调:data指向provided buffer里的数据报负载,回调返回后这块缓冲会立刻被内核复用,所以只能在回调内使用
    //默认实现会拷贝一份到Buffer再调用老的on_recv,想要零拷贝就在子类里覆盖它
    virtual void on_recv_view(const char* data,size_t len,const sockaddr_in& src_addr,IoUring* uring)
    {
        try{
            //从缓冲池借一块复用缓冲:每包make_shared要付一次控制块分配+一次vector分配,高pps下分配器就是瓶颈
            auto buf=_buffer_pool?_buffer_pool->acquire():nullptr;
            if(!buf){buf=std::make_shared<Buffer>();}
            if(len>0){buf->append(data,len);}
            dispatch_recv(std::move(buf),src_addr,uring);
        }catch(const std::exception& e){
            ERROR("default on_recv_view bridge of the UDP server threw an exception: {}",e.what());
        }catch(...){
            ERROR("default on_recv_view bridge of the UDP server threw an unexpected exception");
        }
    }
    //v6版的零拷贝回调,默认实现桥接到on_recv_v6
    virtual void on_recv_view_v6(const char* data,size_t len,const sockaddr_in6& src_addr,IoUring* uring)
    {
        try{
            auto buf=_buffer_pool?_buffer_pool->acquire():nullptr;
            if(!buf){buf=std::make_shared<Buffer>();}
            if(len>0){buf->append(data,len);}
            dispatch_recv_v6(std::move(buf),src_addr,uring);
        }catch(const std::exception& e){
            ERROR("default on_recv_view_v6 bridge of the UDP server threw an exception: {}",e.what());
        }catch(...){
            ERROR("default on_recv_view_v6 bridge of the UDP server threw an unexpected exception");
        }
    }
private:
    //内部实现:发送
    //发送的公共实现:v4/v6都先把地址转成sockaddr_storage再提交
    bool sendto_storage(const char* data,size_t len,const sockaddr_storage& dest,socklen_t dest_len,IoUring* uring)
    {
        if(_urings.empty()||!data){return false;}
        //只有Running才回包:暂停/排空/停止期间一律返回false(调用方据此丢包)。这是有意的——暂停意味着这段时间不收也不发,而且这种拒发必须计数,否则完全不可观测
        if(_state.load(std::memory_order_acquire)!=ServerState::Running){_send_reject_count.fetch_add(1,std::memory_order_relaxed);return false;}
        size_t idx=0;
        if(uring){idx=uring->index();}
        else
        {
            //没传uring时优先用当前线程自己的ring:在recv回调里调用sendto是最常见的用法,这样可以完全避免跨线程提交的锁竞争
            IoUring* cur=IoUring::current();
            if(cur){idx=cur->index();}
            else{idx=_next_uring.fetch_add(1,std::memory_order_relaxed)%_urings.size();}
        }
        if(idx>=_urings.size()){idx=0;}
        //多socket模式下每个socket绑定的是同一个端口,所以从哪个socket回包对端看到的源端口都一样
        int fd=get_socket(idx).get_fd();
        if(fd<0){return false;}
        //取缓冲+背压登记(一次加锁):在途上限必须有,io_uring在sendmsg遇到EAGAIN时会把请求挂在POLLOUT上一直持有,不封顶的话对端不读回包就能把在途缓冲堆到OOM
        bool backpressure=false;
        auto buf=acquire_send_buffer(idx,len,backpressure);
        if(!buf)
        {
            if(backpressure){report_send_backpressure(idx);}
            else{ERROR("cannot allocate a {} byte send buffer for a UDP reply: the datagram is dropped",len);}
            return false;
        }
        try{
            buf->append(data,len);
        }catch(const std::exception& e){
            ERROR("cannot copy {} bytes into the UDP send buffer: {}, the datagram is dropped",len,e.what());
            return_send_buffer(idx,buf,true);
            return false;
        }
        //回调只捕获this与idx(16字节,能放进std::function的小对象缓冲,不会每次发包都堆分配);缓冲区直接从task._buf取,它由这个IoTask持有
        bool submitted=false;
        try{
            submitted=_urings[idx]->async_sendmsg(fd,buf,dest,dest_len,[this,idx](IoTask& task,ssize_t res)
               {
                   if(res<0&&res!=-ECANCELED&&res!=-ENOENT)
                   {
                       report_send_error(task._addr,static_cast<int>(-res));
                   }
                   return_send_buffer(idx,task._buf,true);
               });
        }catch(const std::exception& e){
            //提交抛异常(例如IoTask分配失败):在途计数是在acquire_send_buffer里登记的,必须把缓冲按inflight归还,否则计数永久泄漏,这个ring最终恒定背压
            ERROR("cannot submit a sendmsg: index={}, error={}",idx,e.what());
        }
        if(!submitted)
        {
            return_send_buffer(idx,buf,true);
            return false;
        }
        return true;
    }
    //调用方必须已经持有_send_mtx[idx]
    void return_send_buffer_locked(size_t idx,const std::shared_ptr<Buffer>& buf,bool inflight)
    {
        if(inflight)
        {
            size_t bytes=buf?buf->capacity():0;
            if(bytes<=_inflight_send_bytes[idx]){_inflight_send_bytes[idx]-=bytes;}
            else{_inflight_send_bytes[idx]=0;}
            if(_inflight_send_count[idx]>0){_inflight_send_count[idx]--;}
        }
        if(!buf){return;}
        //被超大报文撑大的缓冲不再放回池子,避免池子长期占着大块内存
        if(buf->capacity()>_recv_buffer_size.load(std::memory_order_relaxed)){return;}
        if(_send_free[idx].size()>=UDP_SEND_POOL_SIZE){return;}
        //再按总字节封顶:条数上限挡不住"每块都很便宜地涨到几十KB"的情况
        if(_send_pool_bytes[idx]+buf->capacity()>UDP_SEND_POOL_BYTES){return;}
        buf->reset();
        _send_pool_bytes[idx]+=buf->capacity();
        _send_free[idx].push_back(buf);
    }
    //内部实现:限速告警
    //当前单调时钟(ns):限速日志用
    static int64_t now_ns()
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    }
    //背压丢包:限速打日志并计数(不能每包一条,否则日志本身就成了放大器)
    void report_send_backpressure(size_t idx)
    {
        uint64_t total=_send_backpressure_drops.fetch_add(1,std::memory_order_relaxed)+1;
        int64_t now=now_ns();
        int64_t last=_last_backpressure_warn_ns.load(std::memory_order_relaxed);
        if(now-last>=1000000000LL&&_last_backpressure_warn_ns.compare_exchange_strong(last,now,std::memory_order_relaxed))
        {
            size_t count=0;
            size_t bytes=0;
            if(idx<_inflight_send_count.size()&&_send_mtx)
            {
                std::lock_guard<std::mutex> lock(_send_mtx[idx]);
                count=_inflight_send_count[idx];
                bytes=_inflight_send_bytes[idx];
            }
            WARN("UDP send backpressure: ring={} has {} in-flight send(s) ({} bytes), the datagram is dropped; dropped_total={}",idx,count,bytes,static_cast<unsigned long long>(total));
        }
    }
    //发送失败的统一出口:优先交给用户回调(v4/v6各一个),没注册就限速打日志
    void report_send_error(const sockaddr_storage& dest,int err)
    {
        _send_error_count.fetch_add(1,std::memory_order_relaxed);
        if(dest.ss_family==AF_INET6)
        {
            std::function<void(const sockaddr_in6&,int)> cb6;
            {
                std::lock_guard<std::mutex> lock(_send_error_cb_mtx);
                cb6=_send_error_callback_v6;
            }
            if(cb6)
            {
                const auto dst6=storage_to_sockaddr_in6(dest);
                if(!dst6){ERROR("malformed IPv6 destination on a failed send, the error callback is skipped");return;}
                try{cb6(*dst6,err);}
                catch(const std::exception& e){ERROR("UDP send error callback for IPv6 threw an exception: {}",e.what());}
                catch(...){ERROR("UDP send error callback for IPv6 threw an unexpected exception");}
                return;
            }
        }
        else
        {
            std::function<void(const sockaddr_in&,int)> cb;
            {
                std::lock_guard<std::mutex> lock(_send_error_cb_mtx);
                cb=_send_error_callback;
            }
            if(cb)
            {
                const auto dst4=storage_to_sockaddr_in(dest);
                if(!dst4){ERROR("malformed IPv4 destination on a failed send, the error callback is skipped");return;}
                try{cb(*dst4,err);}
                catch(const std::exception& e){ERROR("UDP send error callback threw an exception: {}",e.what());}
                catch(...){ERROR("UDP send error callback threw an unexpected exception");}
                return;
            }
        }
        int64_t now=now_ns();
        int64_t last=_last_send_err_warn_ns.load(std::memory_order_relaxed);
        if(now-last<1000000000LL){return;}
        if(!_last_send_err_warn_ns.compare_exchange_strong(last,now,std::memory_order_relaxed)){return;}
        std::string ip=storage_to_ip_string(dest);
        WARN("cannot send a UDP reply to {}:{}: {}",ip.empty()?"?":ip.c_str(),storage_to_port(dest),errno_text(err).c_str());
    }
    //截断告警限速:每个被截断的包都打一条WARN的话,持续的大包流量会把日志刷爆
    void warn_truncated(const sockaddr_storage& addr)
    {
        _truncate_count.fetch_add(1,std::memory_order_relaxed);
        int64_t now=now_ns();
        int64_t last=_last_truncate_warn_ns.load(std::memory_order_relaxed);
        if(now-last<1000000000LL){return;}
        if(!_last_truncate_warn_ns.compare_exchange_strong(last,now,std::memory_order_relaxed)){return;}
        std::string ip=storage_to_ip_string(addr);
        WARN("datagram from {}:{} was truncated: receive buffer={} bytes, truncated_datagrams={} so far",ip.empty()?"?":ip.c_str(),storage_to_port(addr),_recv_buffer_size.load(std::memory_order_relaxed),static_cast<unsigned long long>(_truncate_count.load(std::memory_order_relaxed)));
    }
    //内部实现:uring生命周期
    //创建thread_num个IoUring实例
    bool init_urings(size_t count)
    {
        try{
            _urings.clear();//先清空旧的IoUring
            _urings.reserve(count);
            for(size_t i=0;i<count;i++)
            {
                _urings.emplace_back(IoUring::create());
                _urings.back()->set_index(i);
            }
            return true;
        }catch(const std::exception& e){
            ERROR("cannot create IoUring instances: count={}, error={}",count,e.what());
            _urings.clear();
            return false;
        }
    }
    //回滚路径:等待前count个IoUring停掉,失败只记录不改变返回值
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
    //启动所有IoUring的worker线程,中途失败会回滚已经启动的部分
    bool start_urings()
    {
        if(_urings.empty())
        {
            ERROR("cannot start the IoUring workers: no IoUring instance exists, init_urings must run first");
            return false;
        }
        try{
            for(size_t i=0;i<_urings.size();i++)
            {
                if(!_urings[i]->start())
                {
                    ERROR("cannot start the IoUring worker: index={}",i);
                    stop_urings(i);
                    return false;
                }
            }
            return true;
        }catch(const std::exception& e){
            ERROR("exception while starting the IoUring workers: {}",e.what());
            stop_urings(_urings.size());
            return false;
        }catch(...){
            ERROR("unexpected exception while starting the IoUring workers");
            stop_urings(_urings.size());
            return false;
        }
    }
    //内部实现:socket
    //创建thread_num-1个额外的socket,全部以SO_REUSEPORT绑定同一端口,任何一个创建失败都只降级(少一个socket),不影响服务启动
    bool setup_extra_sockets()
    {
        _extra_sockets.clear();
        if(!_multi_socket){return true;}
        //端口不共享时(单socket口径)额外socket一定绑不上同一端口,直接不建
        if(!is_shared_port()){return true;}
        for(size_t i=1;i<_urings.size();i++)
        {
            auto sock=std::make_unique<Socket>(_socket.get_port(),_socket.get_ip());
            //多socket模式必须让所有socket都能绑同一端口
            sock->set_reuseaddr(true);
            sock->set_reuseport(true);
            //v6only也要跟着主socket走:Socket默认true,不复制的话set_v6only(false)只对主socket生效,
            //额外的socket收不到v4-mapped流量,多socket模式在v4上静默退化
            sock->set_v6only(_socket.get_v6only());
            sock->set_recv_buffer_size(_socket_buffer_size);
            sock->set_send_buffer_size(_socket_buffer_size);
            if(!sock->create_udp_server(false))
            {
                WARN("cannot create the extra UDP socket for ring {}: continuing with {} socket(s)",i,socket_count());
                break;
            }
            _extra_sockets.push_back(std::move(sock));
        }
        return true;
    }
    void close_all_sockets()
    {
        for(auto& sock:_extra_sockets)
        {
            if(sock){sock->close();}
        }
        _socket.close();
    }
    //内部实现:接收
    //把某个ring的接收深度补满:每次从空闲缓冲池里取一块(被取走的缓冲不再属于池子),取不到就停
    void refill_recv(size_t idx)
    {
        while(true)
        {
            if(idx>=_urings.size()||idx>=_recv_free.size()){return;}
            //暂停期间同样要继续挂接收:否则内核接收队列里堆着的包会在resume之后被原样投递给业务
            //(那是暂停之前的旧数据,而且不计入paused_drops)。multishot路径本来就是"继续收+丢弃计数",这里统一口径
            const ServerState st=_state.load(std::memory_order_acquire);
            if(st!=ServerState::Running&&st!=ServerState::Paused){return;}
            if(!_recv_free_mtx){return;}
            int fd=get_socket(idx).get_fd();
            if(fd<0){return;}
            std::shared_ptr<Buffer> buf;
            {
                std::lock_guard<std::mutex> lock(_recv_free_mtx[idx]);
                if(_recv_free[idx].empty()){return;}
                buf=_recv_free[idx].front();
                _recv_free[idx].pop_front();
            }
            //reset()与提交都在锁外:临界区里不做内存分配、不提交SQE,把持锁时间压到最小
            buf->reset();
            bool submitted=false;
            try{
                submitted=_urings[idx]->async_recvmsg(fd,buf,[this,idx](IoTask& task,ssize_t res){on_recvmsg_complete(task,res,idx);});
            }catch(const std::exception& e){
                //提交抛异常(例如IoTask分配失败):缓冲必须还回池子,否则这个ring的接收深度永久少一格
                ERROR("cannot submit a recvmsg: index={}, error={}",idx,e.what());
            }
            if(!submitted)
            {
                std::lock_guard<std::mutex> lock(_recv_free_mtx[idx]);
                _recv_free[idx].push_back(buf);
                return;
            }
        }
    }
    //为每个ring建provided buffer ring(multishot模式)
    bool setup_recv_multishot_buffers()
    {
        if(!_recv_multishot.load(std::memory_order_acquire)){return true;}
        //内核会把io_uring_recvmsg_out+源地址+payload一起写进缓冲,所以要多留出这部分头部空间
        size_t buf_size=sizeof(struct io_uring_recvmsg_out)+sizeof(sockaddr_storage)+_recv_buffer_size.load(std::memory_order_relaxed);
        for(size_t i=0;i<_urings.size();i++)
        {
            if(!_urings[i]->setup_recv_buffers(_recv_bufring_entries,buf_size))
            {
                ERROR("cannot set up the provided buffer ring: index={}, entries={}",i,_recv_bufring_entries);
                return false;
            }
        }
        return true;
    }
    //提交一次multishot recvmsg(每个ring只需要一个)
    bool submit_recvmsg_multishot(size_t idx)
    {
        if(idx>=_urings.size()){return false;}
        //与refill_recv同一口径:暂停期间也保持接收(收到即丢弃并计入paused_drops)
        const ServerState st=_state.load(std::memory_order_acquire);
        if(st!=ServerState::Running&&st!=ServerState::Paused){return false;}
        int fd=get_socket(idx).get_fd();
        if(fd<0){return false;}
        if(!_urings[idx]->has_recv_buffers()){return false;}
        if(!_recvmsg_inflight){return false;}
        //每个ring只允许有一个在途的multishot recvmsg,否则pause()/resume()每来回一次就多挂一个,同一个socket上会同时存在多个接收请求
        bool expected=false;
        if(!_recvmsg_inflight[idx].value.compare_exchange_strong(expected,true,std::memory_order_acq_rel))
        {
            return false;
        }
        bool submitted=false;
        try{
            submitted=_urings[idx]->async_recvmsg_multishot(fd,[this,idx](IoTask& task,ssize_t res){on_recvmsg_multishot_complete(task,res,idx);});
        }catch(const std::exception& e){
            //提交抛异常(例如IoTask分配失败):必须把inflight旗子还回去,否则这个ring永久不再重挂multishot,静默停收
            ERROR("cannot submit a multishot recvmsg: index={}, error={}",idx,e.what());
        }
        if(!submitted)
        {
            _recvmsg_inflight[idx].value.store(false,std::memory_order_release);
            return false;
        }
        return true;
    }
    //内部回调,但可能被非本ring的线程调用,所以和refill_recv共用同一把锁
    void return_recv_buffer(size_t idx,const std::shared_ptr<Buffer>& buf)
    {
        if(!buf||idx>=_recv_free.size()||!_recv_free_mtx){return;}
        //调用方可能把回调里拿到的shared_ptr留了下来:此刻库里应当只有两个引用(本函数入参 + task._buf),
        //多于两个就说明用户还攥着它,绝不能再入池复用——否则用户手里的引用会被下一次recvmsg直接改写(静默数据损坏)。
        //这种情况直接丢弃这块缓冲,用户的引用依然有效,代价只是这个ring的接收深度少一格
        if(buf.use_count()>2){return;}
        std::lock_guard<std::mutex> lock(_recv_free_mtx[idx]);
        _recv_free[idx].push_back(buf);
    }
    //接收完成回调
    void on_recvmsg_complete(IoTask& task,ssize_t res,size_t idx)
    {
        if(idx>=_urings.size()){return;}
        auto buf=task._buf;
        if(!buf){return;}
        if(res<0)
        {
            //先把这块缓冲还回池子,再尝试补齐
            return_recv_buffer(idx,buf);
            if(res==-ECANCELED||res==-ENOENT){return;}//取消/请求不存在:正在停机,不再重挂
            //持续失败时绝不能"立刻重挂->立刻失败"地空转:那会跑满一个核并把日志刷爆(EBADF/ENOBUFS这类错误会一直重复)。
            //和multishot路径用同一套退避:连续失败3次就隔1秒再试
            int streak=0;
            if(_recvmsg_enobufs_streak){streak=_recvmsg_enobufs_streak[idx].value.fetch_add(1,std::memory_order_relaxed)+1;}
            if(streak<=2)
            {
                ERROR("recvmsg failed: index={}, fd={}, error={}",idx,get_socket(idx).get_fd(),errno_text(res).c_str());
                refill_recv(idx);
            }
            else if(streak==3)
            {
                ERROR("recvmsg keeps failing: index={}, fd={}, error={}, retrying in 1 second",idx,get_socket(idx).get_fd(),errno_text(res).c_str());
                auto self=this;
                if(_urings[idx]->add_timer(TIMER_TICK,[self,idx]()
                   {
                       const ServerState st=self->_state.load(std::memory_order_acquire);
                       if(st==ServerState::Running||st==ServerState::Paused)
                       {
                           if(self->_recvmsg_enobufs_streak){self->_recvmsg_enobufs_streak[idx].value.store(0,std::memory_order_relaxed);}
                           self->refill_recv(idx);
                       }
                   })==0)
                {
                    ERROR("cannot arm the retry timer after repeated recvmsg failures: index={}, that socket stops receiving",idx);
                }
            }
            //streak>3:退避定时器已经在跑,这里不再重复挂
            return;
        }
        if(_recvmsg_enobufs_streak){_recvmsg_enobufs_streak[idx].value.store(0,std::memory_order_relaxed);}//成功一次就清掉连败计数
        const ServerState st=_state.load(std::memory_order_acquire);
        if(st!=ServerState::Running)
        {
            //暂停/停止期间收到的数据报同样要计入paused_drops(与multishot路径口径一致)
            _paused_drop_count.fetch_add(1,std::memory_order_relaxed);
            return_recv_buffer(idx,buf);
            //暂停期间必须继续把接收挂回去:否则只有depth个包被丢弃计数,其余留在内核接收队列里,
            //resume之后会被当成"新数据"投递给业务(既是暂停之前的旧数据,paused_drops也少报)
            if(st==ServerState::Paused){refill_recv(idx);}
            return;
        }
        //只用MSG_TRUNC判定:内核只在数据报放不下时才置这个位。
        //原来还并了一个"res>=缓冲大小"的启发式,但公开API保证接收缓冲至少能放下最大数据报,
        //于是"数据报长度正好等于缓冲大小"会被误报成截断(实测:发20个65507字节的包,数据完整却报了20次截断)
        if(task._msg.msg_flags&MSG_TRUNC)
        {
            warn_truncated(task._addr);
        }
        //尽力先把下一个recvmsg挂上:池子里还有空闲缓冲时能立刻补位,慢回调期间内核可以继续收包。
        //注意稳态下depth个缓冲都在途、池子是空的,这次补挂通常取不到缓冲直接返回(实际在途就是depth-1);
        //真正把深度补满的是回调返回后那次refill_recv(那时刚用完的缓冲已经归还)
        refill_recv(idx);
        //调用用户回调(按地址族分派v4/v6)
        //注意:buf会在回调返回后被还回缓冲池并可能被下一次recvmsg复用,回调返回后不能再持有它
        //地址族不匹配时转换会失败:检查后再调用,失败就丢弃这一包并告警
        if(task._addr.ss_family==AF_INET6)
        {
            const auto src6=storage_to_sockaddr_in6(task._addr);
            if(!src6){ERROR("malformed IPv6 source address in a recvmsg result, the datagram is dropped");}
            else
            {
                try{
                    dispatch_recv_v6(buf,*src6,_urings[idx].get());
                }catch(const std::exception& e){
                    ERROR("on_recv_v6 callback of the UDP server threw an exception: {}",e.what());
                }catch(...){
                    ERROR("on_recv_v6 callback of the UDP server threw an unexpected exception");
                }
            }
        }
        else
        {
            const auto src4=storage_to_sockaddr_in(task._addr);
            if(!src4){ERROR("malformed IPv4 source address in a recvmsg result, the datagram is dropped");}
            else
            {
                try{
                    dispatch_recv(buf,*src4,_urings[idx].get());
                }catch(const std::exception& e){
                    ERROR("on_recv callback of the UDP server threw an exception: {}",e.what());
                }catch(...){
                    ERROR("on_recv callback of the UDP server threw an unexpected exception");
                }
            }
        }
        return_recv_buffer(idx,buf);
        //回调期间被消耗掉的缓冲在这里补回来,保持接收深度
        refill_recv(idx);
    }
    //multishot接收完成:数据报在provided buffer里,解析出源地址与payload后交给on_recv_view
    void on_recvmsg_multishot_complete(IoTask& task,ssize_t res,size_t idx)
    {
        if(idx>=_urings.size()){return;}
        IoUring* ring=_urings[idx].get();
        const bool more=task._has_more;
        if(res>=0)
        {
            const bool running=_state.load(std::memory_order_acquire)==ServerState::Running;
            if(!running)
            {
                //暂停/停止期间收到的数据报:数据本身丢弃,但缓冲必须归还(见下方注释)
                _paused_drop_count.fetch_add(1,std::memory_order_relaxed);
            }
            else
            {
                //先确认这次完成真的带回了缓冲(IORING_CQE_F_BUFFER),否则_provided_bid是无效值
                if(!task._has_provided_buffer)
                {
                    _recv_bad_result_count.fetch_add(1,std::memory_order_relaxed);
                    int64_t now=now_ns();
                    int64_t last=_last_recv_bad_warn_ns.load(std::memory_order_relaxed);
                    if(now-last>=1000000000LL&&_last_recv_bad_warn_ns.compare_exchange_strong(last,now,std::memory_order_relaxed))
                    {
                        ERROR("a multishot recvmsg completion carried no buffer flag and was dropped: index={}",idx);
                    }
                }
                else
                {
                void* base=ring->recv_buffer(task._provided_bid);
                if(!base)
                {
                    //bid越界:内核给了个不认识的缓冲号,只能丢弃,但必须计数(不然环悄悄漏干都不可观测)
                    _recv_bad_result_count.fetch_add(1,std::memory_order_relaxed);
                }
                if(base)
                {
                    auto* out=io_uring_recvmsg_validate(base,static_cast<int>(ring->recv_buffer_size()),&task._msg);
                    if(out)
                    {
                        //源地址按地址族解释:IPV6_V6ONLY=0时v6 socket也可能收到v4-mapped地址
                        sockaddr_storage src_storage{};
                        if(out->namelen>=sizeof(sockaddr_storage))
                        {
                            memcpy(&src_storage,io_uring_recvmsg_name(out),sizeof(src_storage));
                        }
                        else if(out->namelen>0)
                        {
                            memcpy(&src_storage,io_uring_recvmsg_name(out),out->namelen);
                        }
                        if(out->flags&MSG_TRUNC)
                        {
                            warn_truncated(src_storage);
                        }
                        const char* payload=static_cast<const char*>(io_uring_recvmsg_payload(out,&task._msg));
                        //地址族不匹配时转换会失败:这里必须走"检查+丢弃并告警",不能解引用空optional
                        if(src_storage.ss_family==AF_INET6)
                        {
                            const auto src6=storage_to_sockaddr_in6(src_storage);
                            if(!src6){ERROR("malformed IPv6 source address in a multishot recvmsg result, the datagram is dropped");}
                            else
                            {
                                try{
                                    dispatch_recv_view_v6(payload,out->payloadlen,*src6,ring);
                                }catch(const std::exception& e){
                                    ERROR("on_recv_view_v6 callback of the UDP server threw an exception: {}",e.what());
                                }catch(...){
                                    ERROR("on_recv_view_v6 callback of the UDP server threw an unexpected exception");
                                }
                            }
                        }
                        else
                        {
                            const auto src4=storage_to_sockaddr_in(src_storage);
                            if(!src4){ERROR("malformed IPv4 source address in a multishot recvmsg result, the datagram is dropped");}
                            else
                            {
                                try{
                                    dispatch_recv_view(payload,out->payloadlen,*src4,ring);
                                }catch(const std::exception& e){
                                    ERROR("on_recv_view callback of the UDP server threw an exception: {}",e.what());
                                }catch(...){
                                    ERROR("on_recv_view callback of the UDP server threw an unexpected exception");
                                }
                            }
                        }
                    }
                    else
                    {
                        _recv_bad_result_count.fetch_add(1,std::memory_order_relaxed);
                        ERROR("received a malformed multishot recvmsg result and dropped it: index={}, fd={}",idx,get_socket(idx).get_fd());
                    }
                }
                }
            }
            if(_recvmsg_enobufs_streak){_recvmsg_enobufs_streak[idx].value.store(0,std::memory_order_relaxed);}
        }
        else if(res<0&&res!=-ECANCELED&&res!=-ENOENT&&res!=-ENOBUFS)
        {
            ERROR("multishot recvmsg failed: index={}, fd={}, error={}",idx,get_socket(idx).get_fd(),errno_text(res).c_str());
        }
        //缓冲回收必须无条件执行,绝不能放在"状态==Running"或"res>=0"的判断里
        //暂停期间内核照样会把provided buffer用掉,不归还的话缓冲环会被抽干:
        //之后每次recvmsg都立刻返回-ENOBUFS且没有MORE,resume()之后就变成"立刻失败->立刻重挂"的忙循环,该socket永久收不到包并且跑满一个核。
        //只有完成事件真的带回了缓冲(IORING_CQE_F_BUFFER)才归还,否则_provided_bid是无效值,归还它会让环里出现重复缓冲
        //归还后必须立刻清掉标志:同一个multishot任务的_has_provided_buffer会一直保留到下一次真实完成,
        //此后cleanup兜底按-ECANCELED强制收尾时,这条"已经归还过"的记录会被再归还一次,
        //环里出现重复缓冲,两个数据报会同时写进同一块内存
        if(task._has_provided_buffer){ring->recycle_recv_buffer(task._provided_bid);task._has_provided_buffer=false;}
        //没有IORING_CQE_F_MORE说明这一次multishot结束了(正常终止、出错或者缓冲用光)
        if(!more)
        {
            if(_recvmsg_inflight){_recvmsg_inflight[idx].value.store(false,std::memory_order_release);}
            if(_state.load(std::memory_order_acquire)!=ServerState::Running){return;}
            int streak=0;
            if(_recvmsg_enobufs_streak)
            {
                streak=_recvmsg_enobufs_streak[idx].value.fetch_add(1,std::memory_order_relaxed)+1;
            }
            //任何"立刻重挂又立刻失败"的持续错误都要退避,不只是-ENOBUFS:
            //例如缓冲参数非法导致每次都-EFAULT时,不退避就是每轮一条ERROR日志+一个100%CPU的忙循环(实测2线程1秒烧1.99秒CPU)
            const bool retryable_error=(res<0&&res!=-ECANCELED&&res!=-ENOENT);
            if(streak>2&&(res==-ENOBUFS||retryable_error))
            {
                //连续多次"立刻重挂又立刻失败":继续立刻重挂就是一个100%CPU的忙循环,退避1秒再试
                _recvmsg_enobufs_streak[idx].value.store(0,std::memory_order_relaxed);
                WARN("the multishot recvmsg of ring {} failed {} times in a row (last error={}, entries={}, recycled={}), retrying in 1 second",idx,streak,errno_text(res).c_str(),ring->recv_buffers_count(),static_cast<unsigned long long>(ring->recv_buffers_recycled()));
                auto self=this;
                if(ring->add_timer(TIMER_TICK,[self,idx]()
                   {
                       const ServerState st=self->_state.load(std::memory_order_acquire);
                       if(st==ServerState::Running||st==ServerState::Paused)
                       {
                           self->submit_recvmsg_multishot(idx);
                       }
                   })==0)
                {
                    ERROR("cannot arm the retry timer for the multishot recvmsg: index={}, that socket stops receiving",idx);
                }
                return;
            }
            submit_recvmsg_multishot(idx);
        }
    }
};
