#include "../include/Epoll.h"
#include "Logger.h"
#include <cstring>
#include <sys/epoll.h>

Epoll::Epoll() {
  if ((epollfd_ = epoll_create1(0)) == -1) {
    LOG_ERROR("epoll_create() failed: %s(%d).", strerror(errno), errno, "");
    exit(-1);
  }
}

Epoll::~Epoll() { close(epollfd_); }

bool Epoll::updatechannel(Channel *ch) {
  epoll_event ev;
  ev.data.ptr = ch;
  ev.events = ch->events();

  if (ch->inpoll()) {
    if (epoll_ctl(epollfd_, EPOLL_CTL_MOD, ch->fd(), &ev) == -1) {
      LOG_ERROR("epoll_ctl(EPOLL_CTL_MOD) failed: %s.", strerror(errno));
      return false;
    }
  } else {
    if (epoll_ctl(epollfd_, EPOLL_CTL_ADD, ch->fd(), &ev) == -1) {
      LOG_ERROR("epoll_ctl(EPOLL_CTL_ADD) failed: %s.", strerror(errno));
      return false;
    }
    ch->setinepoll(true);
  }
  return true;
}

void Epoll::removechannel(Channel *ch) {
  if (ch->inpoll()) {
    if (epoll_ctl(epollfd_, EPOLL_CTL_DEL, ch->fd(), 0) == -1) {
      LOG_ERROR("epoll_ctl(EPOLL_CTL_DEL) failed: %s.", strerror(errno));
    }
  }
}

std::vector<Channel *> Epoll::loop(int timeout) {
  std::vector<Channel *> channels;

  bzero(events_, sizeof(events_));
  int infds = epoll_wait(epollfd_, events_, MaxEvents, timeout);
  if (infds < 0) {
    // EBADF ：epfd不是一个有效的描述符。
    // EFAULT ：参数events指向的内存区域不可写。
    // EINVAL ：epfd不是一个epoll文件描述符，或者参数maxevents小于等于0。
    // EINTR
    // ：阻塞过程中被信号中断，epoll_pwait()可以避免，或者错误处理中，解析error后重新调用epoll_wait()。
    // 在Reactor模型中，不建议使用信号，因为信号处理起来很麻烦，没有必要。------
    // 陈硕

    if (errno == EINTR) {
      // EINTR：被信号中断，重试即可。
      return channels; // 返回空vector，EventLoop::run()会再次调用loop()
    }
    LOG_ERROR("epoll_wait() failed: %s.", strerror(errno));
    return channels;
  }
  if (infds == 0) {
    return channels;
  }
  for (int ii = 0; ii < infds; ++ii) {
    Channel *ch = (Channel *)events_[ii].data.ptr;
    ch->setrevents(events_[ii].events);
    channels.push_back(ch);
  }
  return channels;
}