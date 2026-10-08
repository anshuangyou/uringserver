#include "uringserver/log.hpp"
#include "uringserver/argparser.hpp"
#include "uringserver/tcpserver.hpp"
#include "uringserver/udpserver.hpp"
#include "uringserver/iouring.hpp"
#include <atomic>
#include <iostream>

class TcpEchoServer:public TcpServer
{
public:
    TcpEchoServer(uint16_t port,const std::string& ip="0.0.0.0"):TcpServer(port,ip){set_v6only(false);}
    ~TcpEchoServer(){(void)stop();}
protected:
    void on_connected(std::shared_ptr<Connection> conn)override
    {
        (void)conn;
    }
    void on_recv(std::shared_ptr<Connection> conn,std::shared_ptr<Buffer> buf)override
    {
        size_t n=buf->readable_size();
        if(!conn->send(buf->read_ptr(),n)){return;}
        conn->consume(n);
    }
    void on_close(std::shared_ptr<Connection> conn)
    {
        (void)conn;
    }
};

class UdpEchoServer:public UdpServer
{
public:
    UdpEchoServer(uint16_t port,const std::string& ip="0.0.0.0"):UdpServer(port,ip){set_v6only(false);}
    ~UdpEchoServer(){(void)stop();}
protected:
    void on_recv(std::shared_ptr<Buffer> buf,const sockaddr_in& src,IoUring* uring)override
    {
        (void)sendto(buf->read_ptr(),buf->readable_size(),src,uring);
    }
    void on_recv_v6(std::shared_ptr<Buffer> buf,const sockaddr_in6& src,IoUring* uring)override
    {
        (void)sendto(buf->read_ptr(),buf->readable_size(),src,uring);
    }
    void on_recv_view(const char* data,size_t len,const sockaddr_in& src,IoUring* uring)override
    {
        (void)sendto(data,len,src,uring);
    }
    void on_recv_view_v6(const char* data,size_t len,const sockaddr_in6& src,IoUring* uring)override
    {
        (void)sendto(data,len,src,uring);
    }   
};

int main(int argc,char* argv[])
{
    //获取参数
    ArgParser argparser;
    argparser.add_option("p","port","port",8080);
    argparser.add_option("","ip","ip","0.0.0.0");
    argparser.add_option("","mode","server type","tcp-echo",{"tcp-echo","udp-echo"});
    argparser.add_option("t","thread","thread num",2);
    if(!argparser.parse(argc,argv)){return 1;}
    std::string mode=argparser.get<std::string>("mode");
    uint16_t port=static_cast<uint16_t>(argparser.get<int>("port"));
    std::string ip=argparser.get<std::string>("ip");
    int thread_num=argparser.get<int>("thread");
    //初始化日志 
    Log::init({
        .path="./log/",
        .console_level=Log::Level::OFF
    });
    //初始化Echo服务器
    TcpEchoServer tcp_echo_server(port,ip);
    UdpEchoServer udp_echo_server(port,ip);
    //启动服务器
    if(mode=="tcp-echo")
    {
        (void)tcp_echo_server.start(thread_num);
    }
    else if(mode=="udp-echo")
    {
        (void)udp_echo_server.start(thread_num);
    }
    while(true){}
    return 0;
}