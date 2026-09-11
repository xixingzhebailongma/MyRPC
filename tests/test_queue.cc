#include "MpscQueue.h"
#include <atomic>
#include <cassert>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main() {
  BoundedMpscQueue<std::string> q(8192);

  // 4 个生产者各塞 1000 条
  std::vector<std::thread> pros;
  for (int t = 0; t < 4; ++t) {
    pros.emplace_back([&q, t] {
      for (int i = 0; i < 1000; ++i)
        while (!q.tryEnqueue(std::string("thread-") + std::to_string(t) +
                             "-msg-" + std::to_string(i))) {
        }
    });
  }

  // 单消费者全部取走并计数
  // 退出条件必须是「生产者全部结束 且 队列空」，否则生产者还在塞时队列会
  // 短暂为空，消费者提前退出导致计数不足（flaky）。
  std::atomic<bool> producers_done{false};
  std::thread con([&q, &producers_done] {
    std::string s;
    int cnt = 0;
    while (!producers_done.load() || !q.empty()) {
      while (q.tryDequeue(s))
        ++cnt;
      std::this_thread::yield();
    }
    std::cout << "consumed: " << cnt << "\n";
    assert(cnt == 4000);
  });

  for (auto &t : pros)
    t.join();
  producers_done.store(true);
  con.join();
  std::cout << "OK: queue is correct\n";
  return 0;
}