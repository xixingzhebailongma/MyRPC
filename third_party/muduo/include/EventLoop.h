#pragma once
#include "Connection.h"
#include "Epoll.h"
#include <atomic>
#include <functional>
#include <map>
#include <memory.h>
#include <mutex>
#include <queue>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include "Timer.h"
#include <memory>

class Channel;
class Epoll;
class Connection;
class TimerQueue;
using spConnection = std::shared_ptr<Connection>;

class EventLoop {
private:
  std::unique_ptr<Epoll> ep_;
  std::unique_ptr<TimerQueue> timerqueue_;  // 在 ep_ 之后：析构时先于 ep_（逆向析构）
  pid_t threadid_;
  std::queue<std::function<void()>> taskqueue_;
  std::mutex mutex_;    // 保护 taskqueue_ 的互斥锁
  std::mutex mmutex_; // 保护conns_的互斥锁。
  int wakeupfd_;
  std::unique_ptr<Channel> wakechannel_;
  bool mainloop_;
  std::map<int, spConnection> conns_;
  std::atomic_bool stop_;
  std::function<void(EventLoop *)> epolltimeoutcallback_;

public:
  EventLoop(bool mainloop);
  ~EventLoop();

  void run();
  void stop();

  bool updatechannel(Channel *ch);
  void removechannel(Channel *ch);

  void setepolltimeoutcallback(std::function<void(EventLoop *)> fn);
  bool isinloopthread();
  void queueinloop(std::function<void()> fn);
  void wakeup();
  void handlewakeup();
  void newconnection(spConnection conn);

  //定时器API:仅限loop 线程调用（TimerQueue 操作 std::set 无锁，与 muduo 一致）
  TimerId runAfter(double dalay,std::function<void()>cb);
  TimerId runEvery(double interval,std::function<void()>cb);
  TimerId runAt(Timestamp when,std::function<void()>cb);
  void cancel(TimerId id);
  void removeconnection(int fd);  // 从 conns_ 删除，修复泄漏
};