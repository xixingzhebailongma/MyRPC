#include "../include/EventLoop.h"
#include "../include/TimerQueue.h"
#include "Logger.h"
#include <sys/eventfd.h>

EventLoop::EventLoop(bool mainloop)
    : ep_(new Epoll), timerqueue_(new TimerQueue(this)), threadid_(0),
      wakeupfd_(eventfd(0, EFD_NONBLOCK)),
      wakechannel_(new Channel(this, wakeupfd_)), mainloop_(mainloop),
      stop_(false) {
  wakechannel_->setreadcallback(std::bind(&EventLoop::handlewakeup, this));
  if (!wakechannel_->enablereading()) {
    LOG_ERROR("EventLoop: failed to register wakeup channel (fd %d). EventLoop "
              "cannot function.",
              wakeupfd_);
    stop_ = true;
  }
}

EventLoop::~EventLoop() {
  stop();
  ::close(wakeupfd_);
}

void EventLoop::run() {
  threadid_ = syscall(SYS_gettid);

  while (stop_ == false) {
    std::vector<Channel *> channels = ep_->loop(10 * 1000);

    if (channels.size() == 0) {
      if (epolltimeoutcallback_)
        epolltimeoutcallback_(this);
    } else {
      for (auto &ch : channels) {
        ch->handleevent();
      }
    }
  }
}

void EventLoop::stop() {
  stop_ = true;
  wakeup();
}

bool EventLoop::updatechannel(Channel *ch) { return ep_->updatechannel(ch); }

void EventLoop::removechannel(Channel *ch) { ep_->removechannel(ch); }

void EventLoop::setepolltimeoutcallback(std::function<void(EventLoop *)> fn) {
  epolltimeoutcallback_ = fn;
}

bool EventLoop::isinloopthread() { return threadid_ == syscall(SYS_gettid); }

void EventLoop::queueinloop(std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> gd(mutex_);
    taskqueue_.push(fn);
  }
  wakeup();
}

void EventLoop::wakeup() {
  uint64_t val = 1;
  write(wakeupfd_, &val, sizeof(val));
}

void EventLoop::handlewakeup() {
  uint64_t val;
  read(wakeupfd_, &val, sizeof(val));

  std::vector<std::function<void()>> functors;
  {
    std::lock_guard<std::mutex> gd(mutex_);
    functors.reserve(taskqueue_.size());
    while (!taskqueue_.empty()) {
      functors.push_back(std::move(taskqueue_.front()));
      taskqueue_.pop();
    }
  }

  for (auto &fn : functors)
    fn();
}

void EventLoop::newconnection(spConnection conn) {
  std::lock_guard<std::mutex> gd(mmutex_);
  conns_[conn->fd()] = conn;
}

TimerId EventLoop::runAfter(double delay, std::function<void()> cb) {
  return timerqueue_->runAfter(delay, std::move(cb));
}
TimerId EventLoop::runEvery(double interval, std::function<void()> cb) {
  return timerqueue_->runEvery(interval, std::move(cb));
}
TimerId EventLoop::runAt(Timestamp when, std::function<void()> cb) {
  return timerqueue_->runAt(when, std::move(cb));
}
void EventLoop::cancel(TimerId id) { timerqueue_->cancel(id); }
void EventLoop::removeconnection(int fd) {
  std::lock_guard<std::mutex> gd(mmutex_);
  conns_.erase(fd);
}