#pragma once
#include "Acceptor.h"
#include "Channel.h"
#include "Connection.h"
#include "EventLoop.h"
#include "Socket.h"
#include "ThreadPool.h"
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>

class TcpServer{
    private:
    std::unique_ptr<EventLoop> mainloop_;// 主事件循环。 祼指针 普通指针 原始指针 std::unique_ptr
    std::vector<std::unique_ptr<EventLoop>>subloops_;// 存放从事件循环的容器
    Acceptor acceptor_;// 一个TcpServer只有一个Acceptor对象。
    int threadnum_; // 线程池的大小，即从事件循环的个数。
    ThreadPool threadpool_; //线程池
    std::mutex mmutex_; //保护conns_的互斥锁
    std::map<int,spConnection>conns_;
    std::unordered_map<std::string,std::set<int>>userIdToFds_;
    std::function<void(spConnection)>closeconnectioncb_;
    std::function<void(spConnection)>newconnectioncb_;
    std::function<void(spConnection)>errorconnectioncb_;
    std::function<void(spConnection,std::string& message)>onmessagecb_;
    std::function<void(spConnection)>sendcompletecb_;
    std::function<void(EventLoop*)>timeoutcb_;
    std::function<void(int)>removeconnectioncb_;
    double idletimeout_ = 0.0;      // 秒；0=禁用（默认不启用空闲检测）
    std::atomic_bool stopped_{false};

    public:
    TcpServer(const std::string& ip,const uint16_t port,int threadnum = 4);
    ~TcpServer();
    void start();
    void stop();
    void stopAccept();
    void newconnection(std::unique_ptr<Socket>clientsock);
    void closeconnection(spConnection conn);
    void errorconnection(spConnection conn);
    void onmessage(spConnection conn,std::string &message);
    void sendcomplete(spConnection conn);
    void epolltimeout(EventLoop* loop);

    void setnewconnectioncb(std::function<void(spConnection)>fn);
    void setcloseconnectioncb(std::function<void(spConnection)>fn);
    void seterrorconnectioncb(std::function<void(spConnection)>fn);
    void setonmessagecb(std::function<void(spConnection,std::string& message)>fn);
    void setsendcompletecb(std::function<void(spConnection)>fn);
    void settimeoutcb(std::function<void(EventLoop*)>fn);
    void setPeriodicTimer(double interval,std::function<void(EventLoop*)>fn);
    void removeconn(int fd);
    void setIdleTimeout(double seconds){idletimeout_ = seconds;}    //0=禁用
    void setremoveconnectioncb(std::function<void(int)>fn);

    spConnection getConnByFd(int fd);
};