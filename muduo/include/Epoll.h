#pragma once
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <strings.h>
#include <string.h>
#include <sys/epoll.h>
#include <vector>
#include <unistd.h>
#include "Channel.h"

class Channel;

class Epoll{
private:
static const int MaxEvents = 100;
int epollfd_ = -1;
epoll_event events_[MaxEvents];
public:
Epoll();
~Epoll();

bool updatechannel(Channel* ch);
void removechannel(Channel* ch);
std::vector<Channel*>loop(int timout = -1);
};
