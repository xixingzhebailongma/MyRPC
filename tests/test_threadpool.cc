#include "ThreadPool.h"
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

static int envInt(const char *name, int fallback) {
  const char *v = std::getenv(name);
  return v ? std::atoi(v) : fallback;
}

int main() {
  const int kThreads = envInt("TPTEST_THREADS", 4);
  const int kTasks = envInt("TPTEST_TASKS", 20000);
  const size_t kCap = (size_t)envInt("TPTEST_CAP", 64);

  ThreadPool pool(kThreads, "TEST", kCap);

  std::atomic<int> done{0};
  std::atomic<bool> exceeded{false};

  std::vector<std::thread> pros;
  for (int t = 0; t < kThreads; ++t) {
    pros.emplace_back([&] {
      for (int i = 0; i < kTasks; ++i) {
        pool.addtask([&done] { done.fetch_add(1, std::memory_order_relaxed); });
        // 采样：addtask 返回后队列里排队数。若 addtask 不阻塞，这里会突破 kCap
        if (pool.pendingTasks() > kCap)
          exceeded.store(true, std::memory_order_relaxed);
      }
    });
  }
  for (auto &th : pros)
    th.join();
  pool.stop(); // 排空 + join

  const int total = kThreads * kTasks;
  std::cout << "done=" << done.load() << " expected=" << total << "\n";
  if (done.load() != total) {
    std::cerr << "FAIL: task count mismatch\n";
    return 1;
  }
  if (exceeded.load()) {
    std::cerr << "FAIL: queue exceeded cap " << kCap << "\n";
    return 1;
  }
  std::cout << "OK: bounded queue + backpressure correct\n";
  return 0;
}