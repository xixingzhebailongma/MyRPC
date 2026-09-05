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
    assert(store.claim("k1", &body) == IdemResult::kExecute);
    store.complete("k1", "resp1");
    body.clear();
    assert(store.claim("k1", &body) == IdemResult::kReplay);
    assert(body == "resp1");
  }

  // 2) in-flight：未 complete 时重复 claim 返回 kInFlight，abort 后可重新
  // execute
  {
    IdempotencyLru store;
    std::string body;
    assert(store.claim("k2", &body) == IdemResult::kExecute);
    assert(store.claim("k2", &body) == IdemResult::kInFlight);
    store.abort("k2");
    assert(store.claim("k2", &body) == IdemResult::kExecute);
  }

  // 3) TTL 过期：ttl_ms=1，complete 后 sleep，再 claim 返回 kExecute
  {
    IdempotencyLru store(/*max_entries=*/10, /*ttl_ms=*/1);
    std::string body;
    assert(store.claim("k3", &body) == IdemResult::kExecute);
    store.complete("k3", "resp3");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    assert(store.claim("k3", &body) == IdemResult::kExecute);
  }

  // 4) 淘汰：max_entries=2，插 3 个 key，第一个被淘汰
  {
    IdempotencyLru store(/*max_entries=*/2);
    std::string body;
    assert(store.claim("a", &body) == IdemResult::kExecute);
    store.complete("a", "ra");
    assert(store.claim("b", &body) == IdemResult::kExecute);
    store.complete("b", "rb");
    assert(store.claim("c", &body) == IdemResult::kExecute);
    store.complete("c", "rc");
    body.clear();
    assert(store.claim("a", &body) == IdemResult::kExecute); // a 已被淘汰
  }

  std::cout << "test_idempotency_lru: all passed" << std::endl;
  return 0;
}