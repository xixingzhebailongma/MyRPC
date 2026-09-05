#include "../include/ThreadPool.h"
#include "Logger.h"

ThreadPool::ThreadPool(size_t threadnum, const std::string &threadtype,
                       size_t maxQueueSize)
    : stop_(false), maxQueueSize_(maxQueueSize == 0 ? 1 : maxQueueSize),
      threadtype_(threadtype) {
  for (size_t ii = 0; ii < threadnum; ++ii) {
    threads_.emplace_back([this] {
      LOG_INFO("create %s thread(%ld).", threadtype_.c_str(),
               syscall(SYS_gettid));
      while (true) {
        std::function<void()> task;
        {
          std::unique_lock<std::mutex> lock(this->mutex_);
          this->notEmpty_.wait(lock, [this] {
            return (this->stop_ == true) || (this->taskqueue_.empty() == false);
          });
          if ((this->stop_ == true) && (this->taskqueue_.empty() == true))
            return; // stop 且已排空 → 退出
          task = std::move(this->taskqueue_.front());
          this->taskqueue_.pop();
        }
        this->notFull_.notify_one(); // 有空位了，唤醒一个被阻塞的生产者
        task();
      }
    });
  }
}

void ThreadPool::addtask(std::function<void()> task) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    notFull_.wait(lock, [this] {
      return (this->stop_ == true) ||
             (this->taskqueue_.size() < this->maxQueueSize_);
    });
    if (this->stop_ == true)
      return; // 已停止：丢弃（正常关闭流程里 IO 线程先 join，通常走不到这里）
    taskqueue_.push(std::move(task));
  }
  notEmpty_.notify_one();
}

void ThreadPool::stop() {
  if (stop_)
    return;
  stop_ = true;
  notEmpty_.notify_all(); // 唤醒所有 worker 去排空
  notFull_.notify_all(); // 唤醒所有阻塞中的生产者，让它们看到 stop_ 后返回
  for (std::thread &th : threads_) {
    if (th.joinable())
      th.join();
  }
}

ThreadPool::~ThreadPool() { stop(); }

size_t ThreadPool::size() { return threads_.size(); }

size_t ThreadPool::pendingTasks() {
  std::lock_guard<std::mutex> lock(mutex_);
  return taskqueue_.size();
}