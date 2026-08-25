#include "AsyncLogger.h"
#include <chrono>
#include <cstdint>
#include <immintrin.h> // _mm_pause()
#include <iostream>    // std::cerr

namespace {
// 计算自 since 以来经过的微秒数
uint64_t elapsedUs(std::chrono::steady_clock::time_point since) {
  return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::steady_clock::now() - since)
      .count();
}
} // namespace

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
  std::string line(data, len); // 一次拷贝，与旧版 curBuffer_.append 等价
  if (enqueue(std::move(line))) {
    consumerCv_.notify_one(); // 不持锁 notify，唤醒消费者降低落盘延迟
  }
}

bool AsyncLogger::enqueue(std::string &&line) {
  // 第 1 层：忙等自旋，吸收瞬时突发（生产者互相抢占、消费者短暂满载）
  for (size_t spin = 0; spin < kMaxSpin; ++spin) {
    if (queue_.tryEnqueue(std::move(line)))
      return true;
    _mm_pause(); // 让出流水线，减少自旋功耗/缓存争用
  }

  // 第 2 层：持续满载，说明磁盘 IO 确实跟不上 → 限时阻塞，避免无限自旋
  auto t0 = std::chrono::steady_clock::now();
  for (;;) {
    std::unique_lock<std::mutex> lk(overflowMutex_);
    bool ok =
        overflowCv_.wait_for(lk, std::chrono::milliseconds(kBlockTimeoutMs),
                             [this] { return !queue_.full(); });
    if (ok && queue_.tryEnqueue(std::move(line))) {
      recordFullWait(elapsedUs(t0)); // 记下这一整段阻塞等待
      return true;
    }
    if (!ok) { // 超时仍满 → 兜底
      onQueueOverflow();
      if (kDropOnOverflow) { // 运维兜底开关：true 则丢弃
        recordFullWait(elapsedUs(t0));
        ++dropped_;
        return false;
      }
      // 默认不丢：t0 不重置，回循环继续等，等待时间累计
    }
  }
}

void AsyncLogger::onQueueOverflow() {
  if (!overflowWarned_.exchange(true, std::memory_order_relaxed)) {
    // 只写 stderr，绝不走 append()，否则队列仍满会递归阻塞
    std::cerr << "[AsyncLogger] WARNING: log queue full >" << kBlockTimeoutMs
              << "ms, disk writer falling behind";
    if (kDropOnOverflow)
      std::cerr << " (dropping logs)";
    std::cerr << std::endl;
  }
}

void AsyncLogger::recordFullWait(uint64_t waitUs) {
  ++fullWaitCount_;           // atomic fetch_add
  fullWaitTotalUs_ += waitUs; // atomic fetch_add
  uint64_t prev = fullWaitMaxUs_.load(std::memory_order_relaxed);
  while (waitUs > prev && !fullWaitMaxUs_.compare_exchange_weak(
                              prev, waitUs, std::memory_order_relaxed))
    ;
  ++fullWaitHist_[histBucket(waitUs)];
}

size_t AsyncLogger::histBucket(uint64_t us) {
  // 对数分桶（微秒）：[0]<1, [1]1-4, [2]4-16, [3]16-64,
  //                  [4]64-256, [5]256-1024, [6]1024-4096, [7]>=4096
  size_t b = 0;
  uint64_t hi = 1;
  while (b + 1 < kHistBuckets && us >= hi) {
    ++b;
    hi <<= 2; // 每桶上界 ×4
  }
  return b;
}

void AsyncLogger::threadFunc() {
  std::string line;
  while (running_.load(std::memory_order_acquire)) {
    size_t n = 0;
    while (n < kMaxBatch && queue_.tryDequeue(line)) { // 批量出队写盘
      file_ << line;
      ++n;
    }
    if (n > 0) {
      file_.flush();
      overflowCv_.notify_all(); // 腾出空间，唤醒阻塞的生产者
      overflowWarned_.store(false,
                            std::memory_order_relaxed); // 恢复后可再次告警
    }
    std::unique_lock<std::mutex> lk(consumerMutex_);
    consumerCv_.wait_for(
        lk, std::chrono::milliseconds(kFlushIntervalMs), [this] {
          return !queue_.empty() || !running_.load(std::memory_order_acquire);
        });
  }
  // 收尾：drain 剩余 + flush + close（修复原来不可达的 final-flush）
  while (queue_.tryDequeue(line))
    file_ << line;
  file_.flush();
  file_.close();
}

void AsyncLogger::stop() {
  if (!running_.exchange(false))
    return;                 // 幂等：重复 stop 无副作用
  consumerCv_.notify_all(); // 唤醒消费者退出循环
  overflowCv_.notify_all(); // 唤醒可能阻塞中的生产者
  if (thread_ && thread_->joinable())
    thread_->join();
}

// ---- 监控指标 getter ----
uint64_t AsyncLogger::logsDropped() const {
  return dropped_.load(std::memory_order_relaxed);
}
uint64_t AsyncLogger::queueFullWaitCount() const {
  return fullWaitCount_.load(std::memory_order_relaxed);
}
uint64_t AsyncLogger::queueFullWaitTotalUs() const {
  return fullWaitTotalUs_.load(std::memory_order_relaxed);
}
uint64_t AsyncLogger::queueFullWaitMaxUs() const {
  return fullWaitMaxUs_.load(std::memory_order_relaxed);
}
std::array<uint64_t, AsyncLogger::kHistBuckets>
AsyncLogger::queueFullWaitHist() const {
  std::array<uint64_t, kHistBuckets> snap{};
  for (size_t i = 0; i < kHistBuckets; ++i)
    snap[i] = fullWaitHist_[i].load(std::memory_order_relaxed);
  return snap;
}