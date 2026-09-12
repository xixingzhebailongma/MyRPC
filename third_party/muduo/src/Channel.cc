#include "../include/Channel.h"
#include "../include/EventLoop.h"
#include "Logger.h"
Channel::Channel(EventLoop *loop, int fd) : fd_(fd), loop_(loop) {}

Channel::~Channel() {}

int Channel::fd() { return fd_; }

void Channel::useet() { events_ = events_ | EPOLLET; }

bool Channel::enablereading() {
  events_ |= EPOLLIN;
  if (!loop_->updatechannel(this)) {
    LOG_ERROR("Channel::enablereading() failed for fd %d.", fd_);
    return false;
  }
  return true;
}

void Channel::disablereading() {
  events_ &= ~EPOLLIN;
  if (!loop_->updatechannel(this)) {
    LOG_WARN("Channel::disablereading() failed for fd %d.", fd_);
  }
}

bool Channel::enablewriting() {
  events_ |= EPOLLOUT;
  if (!loop_->updatechannel(this)) {
    LOG_ERROR("Channel::enablewriting() failed for fd %d.", fd_);
    return false;
  }
  return true;
}

void Channel::disablewriting() {
  events_ &= ~EPOLLOUT;
  if (!loop_->updatechannel(this)) {
    LOG_WARN("Channel::disablewriting() failed for fd %d.", fd_);
  }
}

void Channel::clearevent() {
  events_ = 0;
  if (!loop_->updatechannel(this)) {
    LOG_WARN("Channel::disableall() failed for fd %d.", fd_);
  }
}

void Channel::remove() {
  clearevent();
  loop_->removechannel(this);
}

void Channel::setinepoll(bool inepoll) { inepoll_ = inepoll; }
void Channel::setrevents(uint32_t ev) { revents_ = ev; }

bool Channel::inpoll() { return inepoll_; }

uint32_t Channel::events() { return events_; }

uint32_t Channel::revents() { return revents_; }

void Channel::handleevent()
{
    // 生命周期守卫：持有 owner（Connection）的强引用直到本函数结束。
    // onmessage 读到对端关闭会走 closecallback() → 释放最后一个 shared_ptr，
    // 若没有此守卫，本 Channel 会在回调返回后成为悬垂指针（heap-use-after-free）。
    std::shared_ptr<void> guard = tie_.lock();

    if (revents_ & EPOLLERR)
        errorcallback_();

    if (revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP))
        readcallback_();

    if (revents_ & EPOLLOUT)
        writecallback_();

    if ((revents_ & EPOLLHUP) && !(revents_ & EPOLLIN))
        closecallback_();
}

void Channel::setreadcallback(std::function<void()> fn) { readcallback_ = fn; }

void Channel::setclosecallback(std::function<void()> fn) {
  closecallback_ = fn;
}

void Channel::seterrorcallback(std::function<void()> fn) {
  errorcallback_ = fn;
}

void Channel::setwritecallback(std::function<void()> fn) {
  writecallback_ = fn;
}
