#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

using Timestamp = std::chrono::steady_clock::time_point;

inline Timestamp addTime(Timestamp t, double seconds) {
  return t + std::chrono::milliseconds(static_cast<int64_t>(seconds * 1000.0));
}

class Timer;
class TimerId {
public:
  TimerId() : timer_(nullptr), sequence_(0) {}
  TimerId(Timer *t, int64_t seq) : timer_(t), sequence_(seq) {}
  friend class TimerQueue;

private:
  Timer *timer_;
  int64_t sequence_;
};

class Timer {
public:
  using TimerCallback = std::function<void()>;
  Timer(TimerCallback cb, Timestamp when, double interval)
      : callback_(std::move(cb)), expiration_(when), interval_(interval),
        repeat_(interval > 0.0), sequence_(++s_numCreated_) {}

  void run() const { callback_(); }
  Timestamp expiration() const { return expiration_; }
  bool repeat() const { return repeat_; }
  int64_t sequence() const { return sequence_; }
  void restart(Timestamp now) {
    if (repeat_)
      expiration_ = addTime(now, interval_);
  }
  static int64_t numCreated() { return s_numCreated_; }

private:
  const TimerCallback callback_;
  Timestamp expiration_;
  const double interval_;
  const bool repeat_;
  const int64_t sequence_;
  static std::atomic<int64_t> s_numCreated_;
};