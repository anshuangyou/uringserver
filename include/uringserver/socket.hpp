#pragma once
#include "log.hpp"
#include <atomic>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <stdexcept>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <cstdint>

class Socket
{
private:
    //保护"创建/关闭"这两类操作:它们必须串行。并发对同一个Socket调create_server会各自socket()再store,
    //后写的把前一个fd覆盖掉、前一个fd永远关不掉(实测两线程反复create+close泄漏了914个fd)
    std::mutex              _mgmt_mtx;
    std::atomic<int>        _fd{-1};
    std::atomic<uint16_t>   _port{0};           //对外报告的实际端口:port=0时由内核分配,create成功后回读
    uint16_t                _requested_port{0}; //调用方要的端口(0=让内核分配);每次create都用它,不会被上一次的回读结果锁住
    std::string             _ip{"0.0.0.0"};     //构造后不再改
    int                     _family{AF_INET};   //构造后不再改
    //配置项都用原子:set_*可能在别的线程调,而create_server在另一个线程读(TSan实测普通成员在这里是数据竞争)
    std::atomic<int>        _recv_buffer_size{0};
    std::atomic<int>        _send_buffer_size{0};
    std::atomic<int>        _backlog{SOMAXCONN};
    std::atomic_bool        _reuseaddr{true};
    std::atomic_bool        _reuseaddr_set{false};//调用方有没有显式设置过:没设置过时TCP默认开、UDP默认关
    std::atomic_bool        _reuseport{false};
    std::atomic_bool        _v6only{true};
public:
    Socket(const Socket&)=delete;
    Socket& operator=(const Socket&)=delete;
    explicit Socket(uint16_t port,const std::string& ip="0.0.0.0")
    :_port(port)
    ,_requested_port(port)
    ,_ip(ip)
    ,_family(ip.find(':')==std::string_view::npos?AF_INET:AF_INET6)
    {}
    Socket(Socket&& other)noexcept
    {
        std::scoped_lock lock(_mgmt_mtx,other._mgmt_mtx);//两边都锁上,scoped_lock自带死锁避免
        _fd.store(other._fd.exchange(-1),std::memory_order_relaxed);
        _port.store(other._port.exchange(0,std::memory_order_relaxed),std::memory_order_relaxed);//端口随fd一起转移:被搬空的Socket不能再报告一个已经不属于它的端口
        _requested_port=std::exchange(other._requested_port,0);
        _ip=std::move(other._ip);
        _family=std::exchange(other._family,AF_INET);
        _recv_buffer_size.store(other._recv_buffer_size.exchange(0,std::memory_order_relaxed),std::memory_order_relaxed);
        _send_buffer_size.store(other._send_buffer_size.exchange(0,std::memory_order_relaxed),std::memory_order_relaxed);
        _backlog.store(other._backlog.exchange(SOMAXCONN,std::memory_order_relaxed),std::memory_order_relaxed);
        _reuseaddr.store(other._reuseaddr.exchange(true,std::memory_order_relaxed),std::memory_order_relaxed);
        _reuseaddr_set.store(other._reuseaddr_set.exchange(false,std::memory_order_relaxed),std::memory_order_relaxed);
        _reuseport.store(other._reuseport.exchange(false,std::memory_order_relaxed),std::memory_order_relaxed);
        _v6only.store(other._v6only.exchange(true,std::memory_order_relaxed),std::memory_order_relaxed);
    }
    Socket& operator=(Socket&& other)noexcept
    {
        if(this!=&other)
        {
            std::scoped_lock lock(_mgmt_mtx,other._mgmt_mtx);
            close_locked();
            _fd.store(other._fd.exchange(-1),std::memory_order_relaxed);
            _port.store(other._port.exchange(0,std::memory_order_relaxed),std::memory_order_relaxed);//端口随fd一起转移:被搬空的Socket不能再报告一个已经不属于它的端口
            _requested_port=std::exchange(other._requested_port,0);
            _ip=std::move(other._ip);
            _family=std::exchange(other._family,AF_INET);
            _recv_buffer_size.store(other._recv_buffer_size.exchange(0,std::memory_order_relaxed),std::memory_order_relaxed);
            _send_buffer_size.store(other._send_buffer_size.exchange(0,std::memory_order_relaxed),std::memory_order_relaxed);
            _backlog.store(other._backlog.exchange(SOMAXCONN,std::memory_order_relaxed),std::memory_order_relaxed);
            _reuseaddr.store(other._reuseaddr.exchange(true,std::memory_order_relaxed),std::memory_order_relaxed);
            _reuseaddr_set.store(other._reuseaddr_set.exchange(false,std::memory_order_relaxed),std::memory_order_relaxed);
            _reuseport.store(other._reuseport.exchange(false,std::memory_order_relaxed),std::memory_order_relaxed);
            _v6only.store(other._v6only.exchange(true,std::memory_order_relaxed),std::memory_order_relaxed);
        }
        return *this;
    }
    virtual ~Socket(){close();}
public:
    //get_fd/is_open可以跨线程读(内部是原子),但拿到的fd号本身没有生命周期保证:
    //另一个线程close()之后,这个号可能马上被系统复用给别的socket,此时再拿它去read/write/setsockopt就会打到无关的socket上。
    //要么在同一个线程里使用,要么自己保证"close之后没人再用旧fd号"
    int get_fd()const noexcept{return _fd.load(std::memory_order_acquire);}
    //在持有管理锁的情况下把fd交给一段操作使用,与close()/create_*_server()互斥。
    //"先读fd号、再交给内核"的提交(accept/recv等)必须在锁内完成:锁外读完fd号之后它可能被close并被新连接复用,请求就打到无关的socket上
    template<typename F>
    auto with_fd(F&& op)->decltype(op(int{}))
    {
        std::lock_guard<std::mutex> lock(_mgmt_mtx);
        return op(_fd.load(std::memory_order_acquire));
    }
    uint16_t get_port()const noexcept{return _port.load(std::memory_order_acquire);}
    const std::string& get_ip()const noexcept{return _ip;}
    int get_family()const noexcept{return _family;}
    bool is_ipv6()const noexcept{return _family==AF_INET6;}
    bool is_open()const noexcept{return(_fd.load(std::memory_order_acquire)>=0);}
    //是否允许SO_REUSEADDR(必须在create_*_server之前调用)。
    //没显式设置过时:TCP默认开(不然重启时TIME_WAIT会让bind失败),UDP默认关(开了以后同机任何用户都能bind同一端口把流量分走)
    void set_reuseaddr(bool enable)noexcept{_reuseaddr.store(enable,std::memory_order_relaxed);_reuseaddr_set.store(true,std::memory_order_release);}//先存值再release旗子:弱序平台上create侧只要看到旗子为真,值就一定可见
    bool get_reuseaddr()const noexcept{return _reuseaddr.load(std::memory_order_relaxed);}
    bool reuseaddr_explicit()const noexcept{return _reuseaddr_set.load(std::memory_order_relaxed);}
    void set_reuseport(bool enable)noexcept{_reuseport.store(enable,std::memory_order_relaxed);}//是否允许SO_REUSEPORT(必须在create_*_server之前调用)
    bool get_reuseport()const noexcept{return _reuseport.load(std::memory_order_relaxed);}
    void set_v6only(bool enable)noexcept{_v6only.store(enable,std::memory_order_relaxed);}//是否只接受v6(必须在create_*_server之前调用)
    bool get_v6only()const noexcept{return _v6only.load(std::memory_order_relaxed);}
    void set_backlog(int backlog){if(backlog>0){_backlog.store(backlog,std::memory_order_relaxed);}}//设置listen的backlog(必须在create_tcp_server之前调用)
    int get_backlog()const noexcept{return _backlog.load(std::memory_order_relaxed);}
    //设置收发缓冲区大小(在create之前设好,或者在socket已创建时立即下发;size<=0表示不设置)
    //实际值会被内核的sysctl上限截断,可以用getsockopt回读
    void set_recv_buffer_size(int size)
    {
        _recv_buffer_size.store(size,std::memory_order_relaxed);
        //对存活socket下发要拿_mgmt_mtx:close()/create_*_server都持有它,不拿锁的话fd号可能在setsockopt之前被关闭复用,这一发就打到无关的socket上
        std::lock_guard<std::mutex> lock(_mgmt_mtx);
        const int fd=get_fd();
        if(fd>=0&&size>0&&setsockopt(fd,SOL_SOCKET,SO_RCVBUF,&size,sizeof(size))<0)
        {
            const int err=errno;
            WARN("cannot set SO_RCVBUF to {} bytes after the socket was created: {}",size,errno_text(err).c_str());
        }
    }
    void set_send_buffer_size(int size)
    {
        _send_buffer_size.store(size,std::memory_order_relaxed);
        //对存活socket下发要拿_mgmt_mtx:close()/create_*_server都持有它,不拿锁的话fd号可能在setsockopt之前被关闭复用,这一发就打到无关的socket上
        std::lock_guard<std::mutex> lock(_mgmt_mtx);
        const int fd=get_fd();
        if(fd>=0&&size>0&&setsockopt(fd,SOL_SOCKET,SO_SNDBUF,&size,sizeof(size))<0)
        {
            const int err=errno;
            WARN("cannot set SO_SNDBUF to {} bytes after the socket was created: {}",size,errno_text(err).c_str());
        }
    }
private:
    //调用方必须持有_mgmt_mtx
    void close_locked()noexcept
    {
        const int fd=_fd.exchange(-1,std::memory_order_acq_rel);
        if(fd>=0){::close(fd);}
    }
    //把_ip与_port填进sockaddr(成功返回结构体长度,IP非法返回0)
    socklen_t build_address(sockaddr_storage& storage)const noexcept
    {
        std::memset(&storage,0,sizeof(storage));
        if(_family==AF_INET6)
        {
            sockaddr_in6 addr{};
            addr.sin6_family=AF_INET6;
            addr.sin6_port=htons(_requested_port);
            if(inet_pton(AF_INET6,_ip.c_str(),&addr.sin6_addr)!=1){return 0;}
            std::memcpy(&storage,&addr,sizeof(addr));
            return sizeof(sockaddr_in6);
        }
        sockaddr_in addr{};
        addr.sin_family=AF_INET;
        addr.sin_port=htons(_requested_port);
        if(inet_pton(AF_INET,_ip.c_str(),&addr.sin_addr)!=1){return 0;}
        std::memcpy(&storage,&addr,sizeof(addr));
        return sizeof(sockaddr_in);
    }
    //TCP与UDP的建服流程只差socket类型与协议号,统一在这里做(调用方必须持有_mgmt_mtx)
    bool create_server(int sock_type,int protocol,bool block)
    {
        //创建socket
        close_locked();
        const int flags=SOCK_CLOEXEC|(block?0:SOCK_NONBLOCK);
        const int new_fd=socket(_family,sock_type|flags,protocol);
        if(new_fd<0)
        {
            //errno必须当场取:ERROR这一串实参里含getLog()(首次调用会惰性构造日志单例,内部syscall会污染errno)
            const int err=errno;
            ERROR("socket() failed: family={}, type={}, protocol={}, error={}",is_ipv6()?"AF_INET6":"AF_INET",sock_type==SOCK_STREAM?"SOCK_STREAM":"SOCK_DGRAM",protocol,errno_text(err).c_str());
            return false;
        }
        //注意_fd要到全部就绪之后才发布(见函数尾部):半路失败时fd会被直接关掉,
        //提前发布会让并发的get_fd()/is_open()看到一个随后就被关掉的半成品socket,fd号还可能已经复用给了别人
        //缓冲区大小:<=0表示不设置
        const int recv_size=_recv_buffer_size.load(std::memory_order_relaxed);
        if(recv_size>0&&setsockopt(new_fd,SOL_SOCKET,SO_RCVBUF,&recv_size,sizeof(recv_size))<0)
        {
            const int err=errno;
            WARN("cannot set SO_RCVBUF to {} bytes: {}",recv_size,errno_text(err).c_str());
        }
        const int send_size=_send_buffer_size.load(std::memory_order_relaxed);
        if(send_size>0&&setsockopt(new_fd,SOL_SOCKET,SO_SNDBUF,&send_size,sizeof(send_size))<0)
        {
            const int err=errno;
            WARN("cannot set SO_SNDBUF to {} bytes: {}",send_size,errno_text(err).c_str());
        }
        //IPV6_V6ONLY必须在bind之前设置
        if(_family==AF_INET6)
        {
            int v6only=_v6only.load(std::memory_order_relaxed)?1:0;
            if(setsockopt(new_fd,IPPROTO_IPV6,IPV6_V6ONLY,&v6only,sizeof(v6only))<0)
            {
                const int err=errno;
                WARN("cannot set IPV6_V6ONLY to {}: {}",v6only,errno_text(err).c_str());
            }
        }
        //IP/PORT复用
        int opt=1;
        //显式设置过就听调用方的;没设置过时TCP默认开、UDP默认关(见set_reuseaddr的说明)
        const bool reuseaddr=_reuseaddr_set.load(std::memory_order_acquire)?_reuseaddr.load(std::memory_order_relaxed):(sock_type==SOCK_STREAM);
        if(reuseaddr&&setsockopt(new_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt))<0)
        {
            const int err=errno;
            ERROR("cannot enable SO_REUSEADDR: {}",errno_text(err).c_str());
            ::close(new_fd);
            return false;
        }
        if(_reuseport.load(std::memory_order_relaxed)&&setsockopt(new_fd,SOL_SOCKET,SO_REUSEPORT,&opt,sizeof(opt))<0)
        {
            const int err=errno;
            ERROR("cannot enable SO_REUSEPORT: {}",errno_text(err).c_str());
            ::close(new_fd);
            return false;
        }
        //把_ip/_port填成sockaddr
        //解析失败必须当场报错:否则会绑到0.0.0.0,把本该只监听某个地址的服务暴露在所有网卡上
        sockaddr_storage storage{};
        const socklen_t len=build_address(storage);
        if(len==0)
        {
            ERROR("cannot parse the listen address '{}': it must be a numeric {} address",_ip.c_str(),is_ipv6()?"IPv6":"IPv4");
            ::close(new_fd);
            return false;
        }
        //绑定
        if(bind(new_fd,reinterpret_cast<sockaddr*>(&storage),len)<0)
        {
            const int err=errno;
            ERROR("cannot bind {}:{}: {}",_ip.c_str(),static_cast<unsigned>(_requested_port),errno_text(err).c_str());
            ::close(new_fd);
            return false;
        }
        //请求的是端口0时,真实端口由内核分配,这里回读它:_port是对外报告的真实端口,
        //_requested_port保持0,所以下次create仍然会让内核重新分配(不会被这次的分配结果锁住)
        if(_requested_port==0)
        {
            sockaddr_storage bound{};
            socklen_t bound_len=sizeof(bound);
            if(getsockname(new_fd,reinterpret_cast<sockaddr*>(&bound),&bound_len)==0)
            {
                const uint16_t real=(bound.ss_family==AF_INET6)?ntohs(reinterpret_cast<const sockaddr_in6*>(&bound)->sin6_port):ntohs(reinterpret_cast<const sockaddr_in*>(&bound)->sin_port);
                _port.store(real,std::memory_order_release);
            }
        }
        //监听
        const int backlog=_backlog.load(std::memory_order_relaxed);
        if(sock_type==SOCK_STREAM&&listen(new_fd,backlog)<0)
        {
            const int err=errno;
            ERROR("cannot listen on {}:{}: backlog={}, error={}",_ip.c_str(),static_cast<unsigned>(_port.load(std::memory_order_relaxed)),backlog,errno_text(err).c_str());
            ::close(new_fd);
            return false;
        }
        _fd.store(new_fd,std::memory_order_release);//到这里socket才真正可用,对外发布
        return true;
    }
public:
    [[nodiscard]] bool create_tcp_server(bool block=true)
    {
        std::lock_guard<std::mutex> lock(_mgmt_mtx);
        return create_server(SOCK_STREAM,IPPROTO_TCP,block);
    }
    [[nodiscard]] bool create_udp_server(bool block=true)
    {
        std::lock_guard<std::mutex> lock(_mgmt_mtx);
        return create_server(SOCK_DGRAM,0,block);
    }
    void close()noexcept
    {
        std::lock_guard<std::mutex> lock(_mgmt_mtx);
        close_locked();
    }
};
