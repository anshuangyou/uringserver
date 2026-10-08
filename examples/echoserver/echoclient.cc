#include "uringserver/argparser.hpp"
#include <iostream>
#include <string>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>

//把地址格式化成可打印文本:IPv6用[addr]:port,IPv4用addr:port
std::string format_address(const sockaddr_storage& storage)
{
    char ip_buf[INET6_ADDRSTRLEN]{};
    if(storage.ss_family==AF_INET6)
    {
        const struct sockaddr_in6* addr=reinterpret_cast<const struct sockaddr_in6*>(&storage);
        if(inet_ntop(AF_INET6,&addr->sin6_addr,ip_buf,sizeof(ip_buf))==nullptr){return "?";}
        return "["+std::string(ip_buf)+"]:"+std::to_string(ntohs(addr->sin6_port));
    }
    const struct sockaddr_in* addr=reinterpret_cast<const struct sockaddr_in*>(&storage);
    if(inet_ntop(AF_INET,&addr->sin_addr,ip_buf,sizeof(ip_buf))==nullptr){return "?";}
    return std::string(ip_buf)+":"+std::to_string(ntohs(addr->sin_port));
}


int main(int argc,char* argv[])
{
    //获取参数
    ArgParser argparser;
    argparser.add_option("p","port","port",8080);
    argparser.add_option("","ip","server ip,IPv4 or IPv6 literal(e.g. 127.0.0.1 / ::1)","0.0.0.0");
    argparser.add_option("","mode","server type","tcp-echo",{"tcp-echo","udp-echo"});
    if(!argparser.parse(argc,argv)){return 1;}
    std::string mode=argparser.get<std::string>("mode");
    uint16_t port=static_cast<uint16_t>(argparser.get<int>("port"));
    std::string ip=argparser.get<std::string>("ip");
    //按目标地址选地址族,IPv4/IPv6共用后面的收发流程
    sockaddr_storage storage{};
    socklen_t storage_len=sizeof(struct sockaddr_in);
    std::memset(&storage,0,sizeof(storage));
    if(ip.find(':')!=std::string::npos)
    {
        struct sockaddr_in6 addr{};
        addr.sin6_family=AF_INET6;
        addr.sin6_port=htons(port);
        if(inet_pton(AF_INET6,ip.c_str(),&addr.sin6_addr)!=1){storage_len=0;}
        else{storage_len=sizeof(struct sockaddr_in6);}//v6的地址长度是28字节,用v4的16字节调connect/sendto会被内核以EINVAL拒绝
        std::memcpy(&storage,&addr,sizeof(addr));
    }
    else
    {
        struct sockaddr_in addr{};
        addr.sin_family=AF_INET;
        addr.sin_port=htons(port);
        if(inet_pton(AF_INET,ip.c_str(),&addr.sin_addr)!=1){storage_len=0;}
        else{storage_len=sizeof(struct sockaddr_in);}
        std::memcpy(&storage,&addr,sizeof(addr));
    }
    if(storage_len==0)
    {
        std::cerr<<"invalid ip address: "<<ip<<" (expect an IPv4 or IPv6 literal)"<<std::endl;
        return 1;
    }
    //启动echo
    if(mode=="tcp-echo")
    {
        //创建套接字
        int sockfd=socket(storage.ss_family,SOCK_STREAM,0);
        if(sockfd<0)
        {
            std::cerr<<"socket() failed: "<<std::string(std::strerror(errno))<<std::endl;
            return 1;
        }
        //发起连接
        if(connect(sockfd,reinterpret_cast<const struct sockaddr*>(&storage),storage_len)<0)
        {
            std::cerr<<"connect to "<<format_address(storage)<<" failed: "<<std::string(std::strerror(errno))<<std::endl;
            close(sockfd);
            return 1;
        }
        std::cout<<"connected to "<<format_address(storage)<<std::endl;
        //发送+接收
        std::string line;
        char buffer[1024];
        while(true)
        {
            //输入数据(读到EOF直接退出,不然只会空转)
            std::cout<<"send>>";
            if(!(std::cin>>line)){break;}
            //发送数据
            if(send(sockfd,line.c_str(),line.size(),0)<0)
            {
                std::cerr<<"send failed: "<<std::string(std::strerror(errno))<<std::endl;
                break;
            }
            //接收回显
            ssize_t n=recv(sockfd,buffer,sizeof(buffer)-1,0);
            if(n<0)
            {
                std::cerr<<"recv failed: "<<std::string(std::strerror(errno))<<std::endl;
                break;
            }
            if(n==0)
            {
                std::cout<<"connection closed"<<std::endl;
                break;
            }
            buffer[n]='\0';
            std::cout<<"recv:"<<buffer<<std::endl<<std::endl;
        }
        close(sockfd);
    }
    if(mode=="udp-echo")
    {
        //创建套接字
        int sockfd=socket(storage.ss_family,SOCK_DGRAM,0);
        if(sockfd<0)
        {
            std::cerr<<"socket() failed: "<<std::string(std::strerror(errno))<<std::endl;
            return 1;
        }
        std::cout<<"udp target "<<format_address(storage)<<std::endl;
        //发送+接收
        std::string line;
        char buffer[1024];
        while(true)
        {
            //输入数据(读到EOF直接退出)
            std::cout<<"send>>";
            if(!(std::cin>>line)){break;}
            //发送数据
            if(sendto(sockfd,line.c_str(),line.size(),0,reinterpret_cast<const struct sockaddr*>(&storage),storage_len)<0)
            {
                std::cerr<<"sendto failed: "<<std::string(std::strerror(errno))<<std::endl;
                break;
            }
            //接收回显:源地址单独存,不能覆盖server,否则后续报文会打到来包的地址上
            sockaddr_storage from{};
            socklen_t from_len=sizeof(from);
            ssize_t n=recvfrom(sockfd,buffer,sizeof(buffer)-1,0,reinterpret_cast<struct sockaddr*>(&from),&from_len);
            if(n<0)
            {
                std::cerr<<"recvfrom failed: "<<std::string(std::strerror(errno))<<std::endl;
                break;
            }
            buffer[n]='\0';
            std::cout<<"recv:"<<buffer<<std::endl<<std::endl;
        }
        close(sockfd);
    }
    return 0;
}

    