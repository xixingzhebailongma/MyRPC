#include "idempotency_lru.h"
#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main() {
  // 1) 基本：claim -> execute, complete -> replay
  {
    IdempotencyLru store;
    std::string body;
    IdemLease lease;
    assert(store.claim("k1", &body, &lease) == IdemResult::kExecute);
    assert(store.complete("k1", lease, "resp1"));
    body.clear();
    IdemLease lease2;
    assert(store.claim("k1", &body, &lease2) == IdemResult::kReplay);
    assert(body == "resp1");
  }

  // 2) in-flight：未 complete 时重复 claim 返回 kInFlight，abort 后可重新
  // execute
  {
    IdempotencyLru store;
    std::string body;
    IdemLease lease;
    assert(store.claim("k2", &body, &lease) == IdemResult::kExecute);
    IdemLease lease2;
    assert(store.claim("k2", &body, &lease2) == IdemResult::kInFlight);
    store.abort("k2", lease);
    assert(store.claim("k2", &body, &lease2) == IdemResult::kExecute);
  }

  // 3) TTL 过期：ttl_ms=1，complete 后 sleep，再 claim 返回 kExecute
  {
    IdempotencyLru store(/*max_entries=*/10, /*ttl_ms=*/1);
    std::string body;
    IdemLease lease;
    assert(store.claim("k3", &body, &lease) == IdemResult::kExecute);
    store.complete("k3", lease, "resp3");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    IdemLease lease2;
    assert(store.claim("k3", &body, &lease2) == IdemResult::kExecute);
  }

  // 4) 淘汰：max_entries=2，插 3 个 key，第一个被淘汰
  {
    IdempotencyLru store(/*max_entries=*/2);
    std::string body;
    IdemLease lease;
    assert(store.claim("a", &body, &lease) == IdemResult::kExecute);
    store.complete("a", lease, "ra");
    IdemLease lb;
    assert(store.claim("b", &body, &lb) == IdemResult::kExecute);
    store.complete("b", lb, "rb");
    IdemLease lc;
    assert(store.claim("c", &body, &lc) == IdemResult::kExecute);
    store.complete("c", lc, "rc");
    body.clear();
    IdemLease la;
    assert(store.claim("a", &body, &la) == IdemResult::kExecute); // a 已被淘汰
  }

  // 5) fencing：慢 worker 的旧 token 完成被丢弃
  {
    IdempotencyLru store(/*max_entries=*/10, /*ttl_ms=*/1000,
                         /*in_flight_ttl_ms=*/1); // in-flight 1ms，极易过期
    std::string body;
    IdemLease lease1;
    assert(store.claim("k5", &body, &lease1) == IdemResult::kExecute);
    std::this_thread::sleep_for(
        std::chrono::milliseconds(10)); // 让 lease1 过期
    IdemLease lease2;
    assert(store.claim("k5", &body, &lease2) == IdemResult::kExecute); // 被抢占
    // 旧 worker 迟到完成：token 不匹配，应被丢弃（返回 false），且不覆盖新结果
    assert(!store.complete("k5", lease1, "stale"));
    assert(store.complete("k5", lease2, "fresh"));
    IdemLease lease3;
    body.clear();
    assert(store.claim("k5", &body, &lease3) == IdemResult::kReplay);
    assert(body == "fresh"); // 缓存的是新结果，而非 stale
  }

  // 6) renew：续租后 in-flight 不因 TTL 过期
  {
    IdempotencyLru store(/*max_entries=*/10, /*ttl_ms=*/1000,
                         /*in_flight_ttl_ms=*/20);
    std::string body;
    IdemLease lease;
    assert(store.claim("k6", &body, &lease) == IdemResult::kExecute);
    for (int i = 0; i < 20; ++i) { // 每 5ms 续租，共 100ms，远超 20ms 租约
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      assert(store.renew("k6", lease));
    }
    IdemLease lease2;
    assert(store.claim("k6", &body, &lease2) ==
           IdemResult::kInFlight); // 仍持有
  }

  std::cout << "test_idempotency_lru: all passed" << std::endl;
  return 0;
}