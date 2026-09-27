// 日志背压分级丢弃：单测 shouldDrop 纯函数 + LogEntry 队列往返 + 保底等级配置
#include "AsyncLogger.h"
#include "LogLevel.h"
#include "MpscQueue.h"
#include <cassert>
#include <iostream>
#include <string>

using L = LogLevel;

int main() {
  // 1. shouldDrop 纯函数：>= 保底等级的日志永不丢；低等级仅在全局允许丢弃时丢
  assert(!AsyncLogger::shouldDrop(L::WARN, L::WARN, true));
  assert(!AsyncLogger::shouldDrop(L::ERROR, L::WARN, true));
  assert(AsyncLogger::shouldDrop(L::INFO, L::WARN, true));
  assert(AsyncLogger::shouldDrop(L::DEBUG, L::WARN, true));
  // 全局禁止丢弃时，任何等级都不丢
  assert(!AsyncLogger::shouldDrop(L::INFO, L::WARN, false));
  assert(!AsyncLogger::shouldDrop(L::DEBUG, L::WARN, false));
  // 保底等级上调到 ERROR 后，WARN 变为可丢
  assert(AsyncLogger::shouldDrop(L::WARN, L::ERROR, true));
  assert(!AsyncLogger::shouldDrop(L::ERROR, L::ERROR, true));

  // 2. 保底等级 setter/getter 往返
  AsyncLogger &log = AsyncLogger::instance();
  log.setMinGuaranteedLevel(L::ERROR);
  assert(log.minGuaranteedLevel() == L::ERROR);
  log.setMinGuaranteedLevel(L::WARN);
  assert(log.minGuaranteedLevel() == L::WARN);

  // 3. LogEntry 在 BoundedMpscQueue 中 level+msg 往返且保持 FIFO
  BoundedMpscQueue<LogEntry> q(4);
  assert(q.tryEnqueue(LogEntry{L::ERROR, "e1"}));
  assert(q.tryEnqueue(LogEntry{L::DEBUG, "d1"}));
  assert(q.tryEnqueue(LogEntry{L::WARN, "w1"}));
  assert(q.tryEnqueue(LogEntry{L::INFO, "i1"}));
  assert(!q.tryEnqueue(LogEntry{L::INFO, "overflow"})); // 容量 4，满则失败

  LogEntry out;
  assert(q.tryDequeue(out));
  assert(out.level == L::ERROR && out.msg == "e1");
  assert(q.tryDequeue(out));
  assert(out.level == L::DEBUG && out.msg == "d1");
  assert(q.tryDequeue(out));
  assert(out.level == L::WARN && out.msg == "w1");
  assert(q.tryDequeue(out));
  assert(out.level == L::INFO && out.msg == "i1");
  assert(!q.tryDequeue(out)); // 空

  std::cout << "test_logger: all assertions passed" << std::endl;
  return 0;
}
