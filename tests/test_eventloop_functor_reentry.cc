#include "EventLoop.h"
#include <cassert>
#include <cstdio>

int main() {
  EventLoop loop(false);

  bool ran1 = false, ran2 = false;
  loop.queueinloop([&] {
    ran1 = true;
    // 在 functor 内再次 queueinloop —— 旧实现此处死锁
    loop.queueinloop([&] { ran2 = true; });
  });

  loop.handlewakeup(); // 执行 fn1；fn1 重入 queueinloop 不再死锁
  assert(ran1);
  assert(!ran2); // fn2 尚未执行，仍在队列中

  loop.handlewakeup(); // 执行 fn2
  assert(ran2);

  std::printf("all tests passed\n");
  return 0;
}