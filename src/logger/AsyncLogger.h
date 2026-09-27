#pragma once
#include "LogLevel.h"
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

// 异步队列元素：携带等级，供 AsyncLogger 在过载时按等级优先丢弃
struct LogEntry {
  LogLevel level = LogLevel::INFO;
  std::string msg;
  // BoundedMpscQueue::tryDequeue 会对元素调用 clear() 释放堆内存（原 T 是 std::string）
  void clear() { msg.clear(); }
};

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

  // 预留水位：低等级日志只在 size() < kLowPriorityLimit 时入队，
  // 为高等级（WARN/ERROR）腾出 kReservedSlots 个槽位，使其过载时也几乎无需阻塞
  static constexpr size_t kReservedSlots = 1u << 11; // 2048（25%）
  static constexpr size_t kLowPriorityLimit = kQueueCapacity - kReservedSlots;

  static AsyncLogger &instance();
  void start(const std::string &filePath);
  void stop();
  void append(LogLevel level, const char *data, size_t len);

  // 丢弃策略运行时可配置：true=满载超时后丢弃（默认，保证业务线程不被拖死）
  void setDropOnOverflow(bool v) {
    dropOnOverflow_.store(v, std::memory_order_relaxed);
  }
  bool dropOnOverflow() const {
    return dropOnOverflow_.load(std::memory_order_relaxed);
  }

  // 保底等级：>= 此等级的日志永不丢弃（满载时阻塞等待腾位），默认 WARN
  void setMinGuaranteedLevel(LogLevel v) {
    minGuaranteedLevel_.store(v, std::memory_order_relaxed);
  }
  LogLevel minGuaranteedLevel() const {
    return minGuaranteedLevel_.load(std::memory_order_relaxed);
  }

  // 纯函数：满载兜底时某条日志是否应被丢弃（供单测复用）。
  // 规则：全局允许丢弃 且 等级低于保底等级 才会丢；高等级永不丢。
  static bool shouldDrop(LogLevel level, LogLevel minGuaranteed,
                         bool dropOnOverflow) {
    return dropOnOverflow && level < minGuaranteed;
  }

  // 监控指标 getter（供运维/压测读取）
  uint64_t logsDropped() const;
  std::array<uint64_t, 4> logsDroppedByLevel() const; // [DEBUG, INFO, WARN, ERROR]
  uint64_t queueFullWaitCount() const;
  uint64_t queueFullWaitTotalUs() const;
  uint64_t queueFullWaitMaxUs() const;
  std::array<uint64_t, kHistBuckets> queueFullWaitHist() const;

private:
  void threadFunc();
  bool enqueue(LogLevel level, std::string &&line); // 分层背压核心：自旋 -> 阻塞 -> 兜底
  bool hasRoom(LogLevel level) const; // 该等级当前是否可入队（水位/满判定）
  void onQueueOverflow(); // 满载告警/丢弃（只写 stderr，禁止走 append）
  void recordFullWait(uint64_t waitUs); // 记录一次阻塞等待耗时
  static size_t histBucket(uint64_t us); // 把耗时 us 映射到直方图桶下标

  BoundedMpscQueue<LogEntry> queue_{kQueueCapacity};

  std::mutex consumerMutex_;           // 仅消费者用于 wait 谓词
  std::condition_variable consumerCv_; // 消费者休眠/唤醒 + 3s 超时
  std::mutex overflowMutex_;           // 生产者阻塞路径
  std::condition_variable
      overflowCv_; // 满时生产者等待 / 消费者腾位后 notify_all

  std::ofstream file_;
  std::unique_ptr<std::thread> thread_;
  std::atomic<bool> running_{false};
  std::atomic<bool> dropOnOverflow_{true}; // 丢弃策略（运行时可切换）
  std::atomic<LogLevel> minGuaranteedLevel_{LogLevel::WARN}; // 保底等级

  // 指标（全部 atomic，生产者热路径只需 relaxed 加减，无锁）
  std::atomic<uint64_t> dropped_{0};         // 丢弃的日志条数（总量）
  std::array<std::atomic<uint64_t>, 4>
      droppedByLevel_{};                    // 按等级分桶的丢弃计数
  std::atomic<uint64_t> fullWaitCount_{0};   // 发生过阻塞等待的次数
  std::atomic<uint64_t> fullWaitTotalUs_{0}; // 阻塞等待总耗时（微秒）
  std::atomic<uint64_t> fullWaitMaxUs_{0};   // 单次阻塞最大耗时
  std::array<std::atomic<uint64_t>, kHistBuckets>
      fullWaitHist_{};                      // 对数分桶直方图
  std::atomic<bool> overflowWarned_{false}; // 告警去重标志（只告警一次）
};
