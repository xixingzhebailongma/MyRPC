#pragma once
#include "Channel.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <vector>

class Channel;

class Epoll {
private:
  static const int MaxEvents = 100;
  int epollfd_ = -1;
  epoll_event events_[MaxEvents];

public:
  Epoll();
  ~Epoll();

  bool updatechannel(Channel *ch);
  void removechannel(Channel *ch);
  std::vector<Channel *> loop(int timout = -1);
};
