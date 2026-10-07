#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>

// Snowflake 发号器配置（沿用项目 POD 结构体带默认值风格）。
struct SnowflakeConfig {
  int spin_threshold_ms = 5;    // 微小回拨：纯自旋上限（超过则转小步睡眠）
  int severe_threshold_ms = 500; // 严重回拨：总等待预算 / 快速失败阈值
  int warn_threshold_ms = 100;   // 单次回拨幅度超过此值即告警
};

// 自包含的 Snowflake ID 生成器，只负责「位布局 + 逻辑时钟 + 回拨策略 + 持久化钩子」。
// 物理时钟、单调时钟、睡眠与持久化均可注入，便于单测确定性覆盖回拨场景。
class SnowflakeIdGenerator {
public:
  using NowFn = std::function<uint64_t()>;         // 墙上时钟毫秒（默认 system_clock）
  using SteadyFn = std::function<uint64_t()>;      // 单调时钟毫秒（默认 steady_clock）
  using SleepFn = std::function<void(uint64_t ms)>; // 睡眠（默认 this_thread::sleep_for）
  using PersistFn = std::function<bool(uint64_t ts)>; // 持久化时间戳（默认 no-op 返回 true）

  explicit SnowflakeIdGenerator(uint64_t worker_id,
                                SnowflakeConfig cfg = SnowflakeConfig{});

  void SetClock(NowFn now, SteadyFn steady, SleepFn sleep);
  void SetPersistFn(PersistFn fn);

  // 生成一个 Snowflake ID（十进制字符串）。严重回拨 / 持久化失败 / 故障期返回空串。
  std::string Next();

  bool InFault() const { return fault_.load(std::memory_order_relaxed); }

  uint64_t LastTimestampMs() const;
  // 启动 seed 逻辑时钟：设 last_timestamp_ms_ = ts、sequence_ = 0、needs_persist_ = true。
  void SetLastTimestampMs(uint64_t ts);

  uint64_t WorkerId() const { return worker_id_; }

private:
  // [1 位符号=0][41 位毫秒时间戳][10 位 worker_id][12 位毫秒内序列]
  static constexpr uint64_t kEpochMs =
      1577836800000ULL; // 2020-01-01 00:00:00 UTC
  static constexpr uint64_t kWorkerIdBits = 10;
  static constexpr uint64_t kSequenceBits = 12;
  static constexpr uint64_t kMaxWorkerId = (1ULL << kWorkerIdBits) - 1;  // 1023
  static constexpr uint64_t kSequenceMask = (1ULL << kSequenceBits) - 1; // 4095
  static constexpr uint64_t kWorkerIdShift = kSequenceBits;              // 12
  static constexpr uint64_t kTimestampShift =
      kWorkerIdBits + kSequenceBits; // 22

  static uint64_t DefaultNowMs();
  static uint64_t DefaultSteadyMs();
  static void DefaultSleep(uint64_t ms);

  void MaybeWarnRollback(uint64_t rollback, uint64_t now);

  uint64_t worker_id_;
  SnowflakeConfig cfg_;

  NowFn now_fn_;
  SteadyFn steady_fn_;
  SleepFn sleep_fn_;
  PersistFn persist_fn_;

  mutable std::mutex id_mutex_; // 保护 last_timestamp_ms_ / sequence_ / 回拨统计
  uint64_t last_timestamp_ms_{0};
  uint64_t sequence_{0};
  bool needs_persist_{false}; // SetLastTimestampMs 后置位，保证首个毫秒也落盘
  std::atomic<bool> fault_{false};

  // 回拨频率统计（窗口内计数，用于去抖告警）
  uint64_t last_rollback_at_ms_{0};
  int rollback_count_{0};
};
