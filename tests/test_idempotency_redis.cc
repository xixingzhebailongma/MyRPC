#include "idempotency_redis.h"
#include <cassert>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>

// 每次运行用高精度时钟计数做 key 前缀，避免上一次运行残留的 key 串扰
static std::string keyPrefix() {
  static const std::string prefix =
      "test:idem:" +
      std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count()) +
      ":";
  return prefix;
}

int main() {
  // 基础连通性：连不上 redis-server 就跳过（测试机没 Redis 也不判失败）
  IdempotencyRedis probe("tester");
  if (!probe.connect("127.0.0.1", 6379)) {
    std::cout << "test_idempotency_redis: SKIP (no redis-server on "
                 "127.0.0.1:6379)"
              << std::endl;
    return 0;
  }

  const std::string p = keyPrefix();

  // 1) claim -> complete -> replay
  {
    IdempotencyRedis store("tester", 5000, 5000);
    assert(store.connect("127.0.0.1", 6379));
    std::string key = p + "basic";
    std::string body;
    IdemLease lease;
    assert(store.claim(key, &body, &lease) == IdemResult::kExecute);
    assert(lease.token != 0);
    assert(store.complete(key, lease, "resp1"));
    IdemLease l2;
    assert(store.claim(key, &body, &l2) == IdemResult::kReplay);
    assert(body == "resp1");
  }

  // 2) in-flight + abort 后可重新 execute
  {
    IdempotencyRedis store("tester", 5000, 5000);
    assert(store.connect("127.0.0.1", 6379));
    std::string key = p + "inflight";
    std::string body;
    IdemLease lease;
    assert(store.claim(key, &body, &lease) == IdemResult::kExecute);
    IdemLease l2;
    assert(store.claim(key, &body, &l2) == IdemResult::kInFlight);
    store.abort(key, lease);
    IdemLease l3;
    assert(store.claim(key, &body, &l3) == IdemResult::kExecute);
  }

  // 3) 二进制响应体（含 \0 与高位字节）round-trip，验证值编码二进制安全
  {
    IdempotencyRedis store("tester", 5000, 5000);
    assert(store.connect("127.0.0.1", 6379));
    std::string key = p + "binary";
    std::string bin;
    bin.push_back('A');
    bin.push_back('\0');
    bin.push_back(char(0xff));
    bin.push_back('Z');
    std::string body;
    IdemLease lease;
    assert(store.claim(key, &body, &lease) == IdemResult::kExecute);
    assert(store.complete(key, lease, bin));
    IdemLease l2;
    body.clear();
    assert(store.claim(key, &body, &l2) == IdemResult::kReplay);
    assert(body == bin);
  }

  // 4) fencing：租约过期被抢占后，旧 worker 迟到完成被丢弃
  {
    IdempotencyRedis store("tester", /*lease_ttl_ms=*/50,
                           /*result_ttl_ms=*/5000);
    assert(store.connect("127.0.0.1", 6379));
    std::string key = p + "fence";
    std::string body;
    IdemLease lease1;
    assert(store.claim(key, &body, &lease1) == IdemResult::kExecute);
    std::this_thread::sleep_for(
        std::chrono::milliseconds(100)); // 让 lease1 过期
    IdemLease lease2;
    assert(store.claim(key, &body, &lease2) == IdemResult::kExecute); // 被抢占
    assert(!store.complete(key, lease1, "stale")); // 旧 token 丢弃
    assert(store.complete(key, lease2, "fresh"));
    IdemLease l3;
    body.clear();
    assert(store.claim(key, &body, &l3) == IdemResult::kReplay);
    assert(body == "fresh"); // 缓存的是新结果，而非 stale
  }

  // 5) renew：续租后 in-flight 不因短租约过期
  {
    IdempotencyRedis store("tester", /*lease_ttl_ms=*/50,
                           /*result_ttl_ms=*/5000);
    assert(store.connect("127.0.0.1", 6379));
    std::string key = p + "renew";
    std::string body;
    IdemLease lease;
    assert(store.claim(key, &body, &lease) == IdemResult::kExecute);
    for (int i = 0; i < 10; ++i) { // 每 20ms 续租，共 200ms，远超 50ms 租约
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      assert(store.renew(key, lease));
    }
    IdemLease l2;
    assert(store.claim(key, &body, &l2) == IdemResult::kInFlight); // 仍持有
    assert(store.complete(key, lease, "done-after-renew"));
  }

  std::cout << "test_idempotency_redis: all passed" << std::endl;
  return 0;
}