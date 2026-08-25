#include "MpscQueue.h"
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
  std::thread con([&q] {
    std::string s;
    int cnt = 0;
    for (;;) {
      while (q.tryDequeue(s))
        ++cnt;
      if (q.empty())
        break;
    }
    std::cout << "consumed: " << cnt << "\n";
    assert(cnt == 4000);
  });

  for (auto &t : pros)
    t.join();
  con.join();
  std::cout << "OK: queue is correct\n";
  return 0;
}