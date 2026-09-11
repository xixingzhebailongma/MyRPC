// 学习参考：双缓冲简化版 AsyncLogger 实现（不参与构建，见同目录 README.md）
// 原粘贴文件名是 asynclogger.h，实为实现，这里按约定改为 .cc
#include "AsyncLogger.h"
#include <chrono>

AsyncLogger &AsyncLogger::instance() {
  static AsyncLogger inst;
  return inst;
}

void AsyncLogger::start(const std::string &filePath) {
  if (running_.load())
    return;
  file_.open(filePath, std::ios::app);
  running_.store(true);
  thread_ = std::make_unique<std::thread>(&AsyncLogger::threadFunc, this);
}

void AsyncLogger::append(const char *data, size_t len) {
  std::lock_guard<std::mutex> lock(mutex_);
  curBuffer_.append(data, len);
  if (curBuffer_.size() >= kMaxBufferSize) {
    filledBuffers_.push_back(std::move(curBuffer_));
    curBuffer_.clear();
    cond_.notify_one(); // 只有写满时才唤醒后台线程
  }
}

void AsyncLogger::threadFunc() {
  while (true) {
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait_for(lock, std::chrono::milliseconds(kFlushIntervalMs), [this] {
      return !filledBuffers_.empty() || !running_.load();
    });
    if (!running_.load())
      break;

    // 把待写数据交换出来，尽快释放锁，落盘在锁外进行
    std::vector<std::string> buffersToWrite;
    buffersToWrite.swap(filledBuffers_);
    if (!curBuffer_.empty()) {
      buffersToWrite.push_back(std::move(curBuffer_));
      curBuffer_.clear();
    }

    lock.unlock();

    for (auto &buf : buffersToWrite) {
      file_ << buf;
    }
    file_.flush();
  }

  // 收尾：退出前把残留缓冲全部落盘
  std::lock_guard<std::mutex> lock(mutex_);
  if (!curBuffer_.empty()) {
    file_ << curBuffer_;
    curBuffer_.clear();
  }
  for (auto &buf : filledBuffers_) {
    file_ << buf;
  }
  filledBuffers_.clear();
  file_.flush();
  file_.close();
}

void AsyncLogger::stop() {
  running_.store(false);
  cond_.notify_one();
  if (thread_ && thread_->joinable()) {
    thread_->join();
  }
}
