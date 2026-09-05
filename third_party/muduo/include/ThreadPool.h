#pragma once
#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>
#include <vector>

class ThreadPool {
private:
  std::vector<std::thread> threads_;            // 线程池中的线程
  std::queue<std::function<void()>> taskqueue_; // 任务队列（有界）
  std::mutex mutex_;                            // 队列同步锁
  std::condition_variable notEmpty_; // worker：等「队列非空」
  std::condition_variable notFull_;  // 生产者：等「队列有空位」
  std::atomic_bool stop_;
  const size_t maxQueueSize_;    // 队列容量上限（背压）
  const std::string threadtype_; // "IO" / "WORK" / "GATEWAY"
public:
  // maxQueueSize：队列容量上限，满时 addtask 阻塞（背压），默认 8192
  ThreadPool(size_t threadnum, const std::string &threadtype,
             size_t maxQueueSize = 8192);

  void addtask(std::function<void()> task); // 满时阻塞；stop 后丢弃并返回
  size_t size();                            // 线程数（保持原语义）
  size_t pendingTasks(); // 当前排队任务数（观测/测试用）
  void stop();
  ~ThreadPool();
};