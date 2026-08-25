#include "../include/Channel.h"
#include "Logger.h"
#include "../include/EventLoop.h"
Channel::Channel(EventLoop *loop, int fd) :  fd_(fd),loop_(loop) {}

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

void Channel::remove(){
    clearevent();
    loop_->removechannel(this);
}

void Channel::setinepoll(bool inepoll){
    inepoll_ = inepoll;
}
void Channel::setrevents(uint32_t ev){
    revents_ = ev;
}

bool Channel::inpoll(){
    return inepoll_;
}

uint32_t Channel::events(){
    return events_;
}

uint32_t Channel::revents(){
    return revents_;
}

void Channel::handleevent(){
    if(revents_&EPOLLRDHUP){
        closecallback_();
    }else if(revents_&(EPOLLIN|EPOLLPRI)){
        readcallback_();
    }else if(revents_&EPOLLOUT){
        writecallback_();
    }else{
        errorcallback_();
    }
}

void Channel::setreadcallback(std::function<void()>fn){
    readcallback_ = fn;
}

void Channel::setclosecallback(std::function<void()>fn){
    closecallback_ = fn;
}

void Channel::seterrorcallback(std::function<void ()> fn){
    errorcallback_ = fn;
}

void Channel::setwritecallback(std::function<void ()> fn){
    writecallback_ = fn;
}
