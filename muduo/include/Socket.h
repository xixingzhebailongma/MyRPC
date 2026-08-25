#pragma once
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include "InetAddress.h"

int createnonblocking();

class Socket{
    private:
    const int fd_;
    std::string ip_;
    uint16_t port_;

    public:
    Socket(int fd);
    ~Socket();

    int fd()const;
    std::string ip()const;
    uint16_t port() const ;
    void setipport(const std::string& ip,uint16_t port);

    void setreuseaddr(bool on);       // 设置SO_REUSEADDR选项，true-打开，false-关闭。
    void setreuseport(bool on);       // 设置SO_REUSEPORT选项。
    void settcpnodelay(bool on);     // 设置TCP_NODELAY选项。
    void setkeepalive(bool on);       // 设置SO_KEEPALIVE选项。

    void bind(const InetAddress& servaddr);
    void listen(int nn = 128);
    int accept(InetAddress & clientaddr);
};