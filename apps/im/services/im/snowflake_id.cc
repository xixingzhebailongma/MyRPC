#include "snowflake_id.h"
#include "Logger.h"
#include <algorithm>
#include <chrono>
#include <thread>

SnowflakeIdGenerator::SnowflakeIdGenerator(uint64_t worker_id, SnowflakeConfig cfg)
    : worker_id_(worker_id & kMaxWorkerId), cfg_(cfg),
      now_fn_(&SnowflakeIdGenerator::DefaultNowMs),
      steady_fn_(&SnowflakeIdGenerator::DefaultSteadyMs),
      sleep_fn_(&SnowflakeIdGenerator::DefaultSleep),
      persist_fn_([](uint64_t) { return true; }) {}

void SnowflakeIdGenerator::SetClock(NowFn now, SteadyFn steady, SleepFn sleep) {
  now_fn_ = std::move(now);
  steady_fn_ = std::move(steady);
  sleep_fn_ = std::move(sleep);
}

void SnowflakeIdGenerator::SetPersistFn(PersistFn fn) {
  persist_fn_ = std::move(fn);
}

uint64_t SnowflakeIdGenerator::DefaultNowMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

uint64_t SnowflakeIdGenerator::DefaultSteadyMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

void SnowflakeIdGenerator::DefaultSleep(uint64_t ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

uint64_t SnowflakeIdGenerator::LastTimestampMs() const {
  std::lock_guard<std::mutex> lock(id_mutex_);
  return last_timestamp_ms_;
}

void SnowflakeIdGenerator::SetLastTimestampMs(uint64_t ts) {
  std::lock_guard<std::mutex> lock(id_mutex_);
  last_timestamp_ms_ = ts;
  sequence_ = 0;
  needs_persist_ = true;
}

void SnowflakeIdGenerator::MaybeWarnRollback(uint64_t rollback, uint64_t now) {
  // 60s 窗口内累计回拨次数；墙上时钟倒退或超窗则重新计数
  if (now < last_rollback_at_ms_ || now - last_rollback_at_ms_ > 60000) {
    rollback_count_ = 0;
  }
  last_rollback_at_ms_ = now;
  ++rollback_count_;
  if (rollback > static_cast<uint64_t>(cfg_.warn_threshold_ms) ||
      rollback_count_ >= 3) {
    LOG_WARN(
        "Snowflake: clock moved backwards by %llums (count %d in window), worker %llu",
        static_cast<unsigned long long>(rollback), rollback_count_,
        static_cast<unsigned long long>(worker_id_));
    rollback_count_ = 0; // 复位，避免持续刷屏
  }
}

std::string SnowflakeIdGenerator::Next() {
  std::lock_guard<std::mutex> lock(id_mutex_);
  uint64_t now = now_fn_();

  // 故障粘性标志：仍落后 → 快速失败（不重新自旋）；已追平 → 自动恢复
  if (fault_.load(std::memory_order_relaxed)) {
    if (now < last_timestamp_ms_) {
      return "";
    }
    fault_.store(false, std::memory_order_relaxed);
  }

  // 时钟回拨：有界等待（deadline 用 steady_clock，不用 system_clock）
  if (now < last_timestamp_ms_) {
    uint64_t rollback = last_timestamp_ms_ - now;
    MaybeWarnRollback(rollback, now);

    uint64_t deadline = steady_fn_() + cfg_.severe_threshold_ms;
    if (rollback <= static_cast<uint64_t>(cfg_.spin_threshold_ms)) {
      // 微小回拨：有界自旋，等墙上时钟追上
      while (now < last_timestamp_ms_ && steady_fn_() < deadline) {
        now = now_fn_();
      }
    } else {
      // 中度回拨：小步睡眠等待，直到追平或超严重阈值
      while (now < last_timestamp_ms_ && steady_fn_() < deadline) {
        sleep_fn_(1);
        now = now_fn_();
      }
    }
    if (now < last_timestamp_ms_) {
      fault_.store(true, std::memory_order_relaxed);
      LOG_ERROR(
          "Snowflake: clock rollback of %llums exceeds %dms, worker %llu — refusing to issue id",
          static_cast<unsigned long long>(rollback), cfg_.severe_threshold_ms,
          static_cast<unsigned long long>(worker_id_));
      return "";
    }
  }

  // 是否进入新毫秒（时间戳推进，或 SetLastTimestampMs 后首个毫秒）
  bool new_ms = (now != last_timestamp_ms_) || needs_persist_;

  if (!new_ms) {
    // 同一毫秒：序列自增，不写持久化
    sequence_ = (sequence_ + 1) & kSequenceMask;
    if (sequence_ == 0) {
      // 序列耗尽（>4096/ms，实际不可达）：等待下一毫秒，走新毫秒持久化路径
      uint64_t deadline = steady_fn_() + cfg_.severe_threshold_ms;
      do {
        sleep_fn_(1);
        now = now_fn_();
      } while (now <= last_timestamp_ms_ && steady_fn_() < deadline);
      if (now <= last_timestamp_ms_) {
        fault_.store(true, std::memory_order_relaxed);
        LOG_ERROR("Snowflake: sequence exhausted and clock stuck, worker %llu",
                  static_cast<unsigned long long>(worker_id_));
        return "";
      }
      new_ms = true;
    }
  }

  if (new_ms) {
    // 新毫秒：先同步持久化时间戳，成功后才发号
    if (!persist_fn_(now)) {
      fault_.store(true, std::memory_order_relaxed);
      LOG_ERROR("Snowflake: failed to persist last_ts, worker %llu",
                static_cast<unsigned long long>(worker_id_));
      return "";
    }
    needs_persist_ = false;
    last_timestamp_ms_ = now;
    sequence_ = 0;
  }

  uint64_t id = ((now - kEpochMs) << kTimestampShift) |
                ((worker_id_ & kMaxWorkerId) << kWorkerIdShift) | sequence_;
  return std::to_string(id);
}
