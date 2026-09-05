#include "../include/TimerQueue.h"
#include "Logger.h"
#include "Timer.h"
#include <algorithm>
#include <assert.h>
#include <chrono>
#include <cstdint>
#include <string.h>
#include <sys/timerfd.h>
#include <unistd.h>
std::atomic<int64_t> Timer::s_numCreated_{0};
namespace {
// 1) 创建 timerfd：CLOCK_MONOTONIC + 非阻塞 + close-on-exec
int createTimerfd() {
  int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (fd < 0)
    LOG_ERROR("TimerQueue: timerfd_create failed");
  return fd;
}
// 2) 【关键】把「到 when 还有多久」换算成 timespec。
//    因为 Timestamp 是 steady_clock::time_point，epoch 未定义，绝不能读
//    time_since_epoch() 的绝对值；只能算差值 (when - now)。
struct timespec howMuchTimeFromNow(Timestamp when) {
  int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(
                   when - std::chrono::steady_clock::now())
                   .count();
  if (us < 100)
    us = 100; // 下限 clamp 到 100µs，防忙等
  struct timespec ts;
  ts.tv_sec = static_cast<time_t>(us / 1000000);
  ts.tv_nsec = static_cast<long>((us % 1000000) * 1000);
  return ts;
}

// 3) 重设 timerfd 到最早到期时间（it_interval={0,0}，手动重装）
void resetTimerfd(int timerfd, Timestamp expiration) {
  struct itimerspec newValue, oldValue;
  bzero(&newValue, sizeof(newValue));
  bzero(&oldValue, sizeof(oldValue));
  newValue.it_value = howMuchTimeFromNow(expiration); // it_interval 保持 0
  if (::timerfd_settime(timerfd, 0, &newValue, &oldValue) < 0)
    LOG_ERROR("TimerQueue: timerfd_settime failed");
}

// 4) 读 8 字节的过期次数并排空（电平触发，read 后 fd 不再可读）
void readTimerfd(int timerfd) {
  uint64_t howmany;
  ssize_t n = ::read(timerfd, &howmany, sizeof(howmany));
  if (n != sizeof(howmany))
    LOG_ERROR("TimerQueue: read %zd bytes instead of 8", n);
}

} // namespace

TimerQueue::TimerQueue(EventLoop *loop)
    : loop_(loop), timerfd_(createTimerfd()), timerfdchannel_(loop, timerfd_),
      callingExpiredTimers_(false) {
  timerfdchannel_.setreadcallback(std::bind(&TimerQueue::handleRead, this));
  timerfdchannel_.enablereading(); // 电平触发（默认），【不调用 useet()】
}

TimerQueue::~TimerQueue() {
  timerfdchannel_
      .remove(); // 内部已是 clearevent()+removechannel()，把 fd 从 epoll 摘除
  ::close(timerfd_);
  for (auto &e : timers_) // 兜底：正常应已为空，残留则逐个 delete
    delete e.timer;
}

TimerId TimerQueue::addTimer(TimerCallback cb, Timestamp when,
                             double interval) {
  Timer *timer = new Timer(std::move(cb), when, interval);
  bool earliestChanged = insert(timer);
  if (earliestChanged) //新定时器比原最早项更早-》重设timerfd
    resetTimerfd(timerfd_, timer->expiration());
  return TimerId(timer, timer->sequence());
}

TimerId TimerQueue::runAfter(double delay, TimerCallback cb) {
  return addTimer(std::move(cb),
                  addTime(std::chrono::steady_clock::now(), delay), 0.0);
}

TimerId TimerQueue::runEvery(double interval, TimerCallback cb) {
  return addTimer(std::move(cb),
                  addTime(std::chrono::steady_clock::now(), interval),
                  interval);
}

TimerId TimerQueue::runAt(Timestamp when, TimerCallback cb) {
  return addTimer(std::move(cb), when, 0.0);
}

bool TimerQueue::insert(Timer *timer) {
  bool earliestChanged = false;
  Timestamp when = timer->expiration();
  if (timers_.empty() || when < timers_.begin()->expiration)
    earliestChanged = true;
  auto r1 = timers_.insert(Entry{when, timer->sequence(), timer});
  assert(r1.second);
  (void)r1;
  auto r2 = activeTimers_.insert(ActiveTimer(timer, timer->sequence()));
  assert(r2.second);
  (void)r2;

  return earliestChanged; // 供 addTimer 判断是否要重设 timerfd
}

std::vector<TimerQueue::Entry> TimerQueue::getExpired(Timestamp now) {
  std::vector<Entry> expired;
  Entry sentry{
      now, INT64_MAX,
      nullptr}; // 哨兵：比较器只碰 expiration/sequence，不解引用 nullptr
  TimerList::iterator end = timers_.lower_bound(sentry);
  std::copy(timers_.begin(), end, std::back_inserter(expired));
  timers_.erase(timers_.begin(), end); // 先移出集合
  for (const Entry &e : expired) {     // 再同步从 activeTimers_ 删除
    size_t n = activeTimers_.erase(ActiveTimer(e.timer, e.sequence));
    assert(n == 1);
    (void)n;
  }
  return expired;
}

void TimerQueue::handleRead() {
  Timestamp now(std::chrono::steady_clock::now());
  readTimerfd(timerfd_);
  std::vector<Entry> expired = getExpired(now);
  callingExpiredTimers_ = true;
  cancelingTimers_.clear(); // 清空上一轮记录
  for (const Entry &e : expired) {
    e.timer->run(); // 回调内可能 cancel（含取消自身）
  }
  callingExpiredTimers_ = false;
  reset(expired, now);
}

void TimerQueue::reset(const std::vector<Entry> &expired, Timestamp now) {
  for (const Entry &e : expired) {
    ActiveTimer key(e.timer, e.sequence);
    if (e.timer->repeat() &&
        cancelingTimers_.find(key) == cancelingTimers_.end()) {
      e.timer->restart(
          now); //// 推进 expiration（repeat_ 为真才生效，见 Timer.h:37）
      insert(e.timer); // 重新入集合
    } else {
      delete e.timer; // 一次性已触发 / 或回调内被取消
    }
  }
  if (!timers_.empty()) //还有未到期项 → 重设到新最早项
    resetTimerfd(timerfd_, timers_.begin()->expiration);
}

void TimerQueue::cancel(TimerId id) {
  // 仅限 loop 线程调用（文档注明）；id 来自 runAfter/runEvery/runAt
  ActiveTimer key(id.timer_, id.sequence_);
  auto it = activeTimers_.find(key); // std::set<pair>，无需自定义 hash
  if (it != activeTimers_.end()) {
    size_t n =
        timers_.erase(Entry{id.timer_->expiration(), id.sequence_, id.timer_});
    assert(n == 1);
    (void)n;
    delete id.timer_;
    activeTimers_.erase(it);
  } else if (callingExpiredTimers_) {
    // 找不到 = 该定时器已到期、正被回调执行（已移出两个集合）。
    // 这是「在回调里取消自身」→ 记入 cancelingTimers_，由 reset 兜底 delete。
    cancelingTimers_.insert(key);
  }
  assert(timers_.size() == activeTimers_.size()); // 不变量校验
}