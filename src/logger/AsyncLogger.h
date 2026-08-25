#pragma once
#include "MpscQueue.h"
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

class AsyncLogger {
public:
  // 常量公开，方便测试里引用
  static constexpr size_t kQueueCapacity = 1u << 13; // 8192 条（2 的幂）
  static constexpr size_t kMaxSpin = 1000;           // 忙等自旋上限
  static constexpr int kBlockTimeoutMs = 10;         // 满时阻塞等待上限
  static constexpr int kFlushIntervalMs =
      3000; // 保留原值：无数据时每 3s 也 flush 一次
  static constexpr size_t kHistBuckets = 8; // 直方图桶数
  static constexpr size_t kMaxBatch = 1024; // 消费者每次 drain 最多写多少条
  static constexpr bool kDropOnOverflow =
      false; // 兜底开关：true 则满载超时后丢弃

  static AsyncLogger &instance();
  void start(const std::string &filePath);
  void stop();
  void append(const char *data, size_t len);

  // 监控指标 getter（供运维/压测读取）
  uint64_t logsDropped() const;
  uint64_t queueFullWaitCount() const;
  uint64_t queueFullWaitTotalUs() const;
  uint64_t queueFullWaitMaxUs() const;
  std::array<uint64_t, kHistBuckets> queueFullWaitHist() const;

private:
  void threadFunc();
  bool enqueue(std::string &&line); // 分层背压核心：自旋 -> 阻塞 -> 兜底
  void onQueueOverflow(); // 满载告警/丢弃（只写 stderr，禁止走 append）
  void recordFullWait(uint64_t waitUs); // 记录一次阻塞等待耗时
  static size_t histBucket(uint64_t us); // 把耗时 us 映射到直方图桶下标

  BoundedMpscQueue<std::string> queue_{kQueueCapacity};

  std::mutex consumerMutex_;           // 仅消费者用于 wait 谓词
  std::condition_variable consumerCv_; // 消费者休眠/唤醒 + 3s 超时
  std::mutex overflowMutex_;           // 生产者阻塞路径
  std::condition_variable
      overflowCv_; // 满时生产者等待 / 消费者腾位后 notify_all

  std::ofstream file_;
  std::unique_ptr<std::thread> thread_;
  std::atomic<bool> running_{false};

  // 指标（全部 atomic，生产者热路径只需 relaxed 加减，无锁）
  std::atomic<uint64_t> dropped_{0};         // 丢弃的日志条数
  std::atomic<uint64_t> fullWaitCount_{0};   // 发生过阻塞等待的次数
  std::atomic<uint64_t> fullWaitTotalUs_{0}; // 阻塞等待总耗时（微秒）
  std::atomic<uint64_t> fullWaitMaxUs_{0};   // 单次阻塞最大耗时
  std::array<std::atomic<uint64_t>, kHistBuckets>
      fullWaitHist_{};                      // 对数分桶直方图
  std::atomic<bool> overflowWarned_{false}; // 告警去重标志（只告警一次）
};