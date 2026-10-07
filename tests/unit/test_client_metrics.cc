// 客户端计数器单调累加器 + try_lock 渲染与并发注销的沉淀不丢。
// - 基础：注册→累加→注销（沉淀）→再注册→再累加，断言总数只增不减、不重置、不重复。
// - 并发：render 持 client_mu_ 期间另一线程注销对象，释放锁后再 render，断言
//   沉淀进累加器、不永久漏计。
#include "metrics_registry.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>

static int fail(const char *msg) {
  std::fprintf(stderr, "FAILED: %s\n", msg);
  return 1;
}

int main() {
  auto &reg = MetricsRegistry::instance();

  // ===== 基础：单调累加器 =====
  {
    std::atomic<uint64_t> a_total{0};
    uint64_t tokA = reg.registerClient(
        MetricsRegistry::ClientKind::kSync, [&a_total]() {
          ClientCounters c;
          c.total = a_total.load(std::memory_order_relaxed);
          return c;
        });
    a_total.store(5);
    if (reg.clientSnapshot().total != 5)
      return fail("live client A total != 5");

    reg.unregisterClient(tokA); // 沉淀 5
    if (reg.clientSnapshot().total != 5)
      return fail("accumulator after unregister != 5");

    std::atomic<uint64_t> b_total{0};
    uint64_t tokB = reg.registerClient(
        MetricsRegistry::ClientKind::kSync, [&b_total]() {
          ClientCounters c;
          c.total = b_total.load(std::memory_order_relaxed);
          return c;
        });
    b_total.store(3);
    if (reg.clientSnapshot().total != 8)
      return fail("total != 5 + 3 (reset or double-count)");

    reg.unregisterClient(tokB); // 沉淀 3 → 累加器 = 8
    if (reg.clientSnapshot().total != 8)
      return fail("total after second unregister != 8");
  }

  // ===== 并发：render 持 client_mu_ 期间注销对象 → 释放后再 render 不丢 =====
  {
    // 累加器当前 = 8。再注册对象 C（累加到 7），并发注销它。
    std::atomic<uint64_t> c_total{0};
    std::atomic<bool> reader_entered{false};
    std::atomic<bool> release_reader{false};
    uint64_t tokC = reg.registerClient(
        MetricsRegistry::ClientKind::kSync, [&]() {
          // 第一次被 render 读到就阻塞，模拟「render 正持 client_mu_」。
          reader_entered.store(true);
          while (!release_reader.load())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          ClientCounters c;
          c.total = c_total.load(std::memory_order_relaxed);
          return c;
        });
    c_total.store(7);

    std::string rendered;
    std::thread renderer([&] { rendered = reg.renderPrometheus(); });

    // 等 render 已经进入 reader（即已持锁）。
    while (!reader_entered.load())
      std::this_thread::sleep_for(std::chrono::milliseconds(1));

    // 注销会阻塞在 client_mu_ 上，直到 render 释放。
    std::thread unreg([&] { reg.unregisterClient(tokC); });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    release_reader.store(true); // 放行 reader → render 释放锁 → unreg 完成沉淀
    renderer.join();
    unreg.join();

    // 最终值 = 8(旧沉淀) + 7(C 沉淀) = 15；若漏计会得 8。
    if (reg.clientSnapshot().total != 15)
      return fail("commit lost after concurrent render/unregister");
  }

  std::printf("PASSED\n");
  return 0;
}
