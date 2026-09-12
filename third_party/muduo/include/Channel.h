#pragma once
#include <sys/epoll.h>
#include <functional>
#include "InetAddress.h"
#include "Socket.h"
#include <memory>

class EventLoop;

class Channel{
    private:
    int fd_ = -1;
    EventLoop* loop_;
    bool inepoll_ = false;
    uint32_t events_=0;
    uint32_t revents_ = 0;
    std::function<void()>readcallback_;
    std::function<void()>closecallback_;
    std::function<void()>errorcallback_;
    std::function<void()>writecallback_;
    // 绑定 owner（通常是 Connection）的弱引用：handleevent() 期间 lock 成强引用，
    // 防止回调链里 Connection 被析构后本 Channel 被 use-after-free（muduo 的 tie 机制）。
    std::weak_ptr<void> tie_;

    public:
    Channel(EventLoop* loop,int fd);
    ~Channel();

    int fd();
    void useet();
    bool enablereading();
    void disablereading();
    bool enablewriting();
    void disablewriting();
    void clearevent();
    void remove();
    void setinepoll(bool inepoll);
    void setrevents(uint32_t ev);
    bool inpoll();
    uint32_t events();
    uint32_t revents();

    void handleevent();

    // 绑定 owner 的 shared_ptr（生命周期守卫），须在 owner 已置于 shared_ptr 后调用。
    void tie(const std::shared_ptr<void>& obj) { tie_ = obj; }

    void setreadcallback(std::function<void()>fn);
    void setclosecallback(std::function<void()>fn);
    void seterrorcallback(std::function<void()>fn);
    void setwritecallback(std::function<void()>fn);
};