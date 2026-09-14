#include "load_balancer.h"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_set>

int main() {
  ConsistentHashBalancer balancer(150);
  balancer.rebuild({"A", "B", "C", "D"});
  const size_t N = 4;

  // 1) 空 exclude == 首选
  for (int i = 0; i < 1000; ++i) {
    std::string key = "user_" + std::to_string(i);
    size_t primary = balancer.select(N, key);
    assert(balancer.select(N, key, std::unordered_set<size_t>{}) == primary);
  }

  // 2) 排除首选后，结果必不是该节点
  for (int i = 0; i < 1000; ++i) {
    std::string key = "user_" + std::to_string(i);
    size_t primary = balancer.select(N, key);
    assert(balancer.select(N, key, std::unordered_set<size_t>{primary}) !=
           primary);
  }

  // 3) 只剩一个候选时返回它
  for (int i = 0; i < 100; ++i) {
    std::string key = "k_" + std::to_string(i);
    assert(balancer.select(N, key, std::unordered_set<size_t>{0, 1, 2}) == 3);
  }

  // 4) 故障节点(下标 0)的 key 应打散到其余节点，而非集中到同一台
  std::unordered_set<size_t> distinct;
  for (int i = 0; i < 1000; ++i) {
    std::string key = "user_" + std::to_string(i);
    if (balancer.select(N, key) == 0) { // 只统计原本哈希到 0 的 key
      distinct.insert(balancer.select(N, key, std::unordered_set<size_t>{0}));
    }
  }
  assert(distinct.size() >= 2); // 修复前 candidates.front() 恒返回 1

  // ===== RoundRobinBalancer =====
  {
    RoundRobinBalancer rr;
    rr.rebuild({"A", "B", "C"});
    const size_t M = 3;
    // 顺序轮转：0,1,2,0,1,2...
    assert(rr.select(M) == 0);
    assert(rr.select(M) == 1);
    assert(rr.select(M) == 2);
    assert(rr.select(M) == 0);
    assert(rr.select(M) == 1);
    assert(rr.select(M) == 2);
    // key 版本回退到无 key 轮转（轮询不关心 key）。
    // 注意：RoundRobinBalancer::select(size_t) 用 override 隐藏了基类
    // 带 key 的重载，故须经 ILoadBalancer& 调用才能命中基类回退逻辑。
    ILoadBalancer &rrIface = rr;
    assert(rrIface.select(M, "whatever") == 0);
    assert(rrIface.select(M, "whatever") == 1);
    // rebuild 重置计数器
    rr.rebuild({"A", "B"});
    assert(rr.select(2) == 0);
    assert(rr.select(2) == 1);
    assert(rr.select(2) == 0);
    // select(0) 安全返回 0
    assert(rr.select(0) == 0);
  }

  // ===== RandomBalancer =====
  {
    RandomBalancer rnd;
    const size_t M = 4;
    // 值域合法 + 均匀分布（4 节点 8000 次，每节点占比落在 [0.18, 0.32]）
    size_t cnt[4] = {0, 0, 0, 0};
    const int kSamples = 8000;
    for (int i = 0; i < kSamples; ++i) {
      size_t s = rnd.select(M);
      assert(s < M);
      cnt[s]++;
    }
    for (size_t c : cnt) {
      double p = static_cast<double>(c) / kSamples;
      assert(p > 0.18 && p < 0.32); // 期望 0.25，宽界避免偶发抖动
    }
    // key 版本回退到无 key 随机（经接口调用，命中基类回退）
    ILoadBalancer &rndIface = rnd;
    assert(rndIface.select(M, "some_key") < M);
    // select(0) 安全返回 0
    assert(rnd.select(0) == 0);
  }

  std::cout << "OK: consistent hash + round-robin + random balancers\n";
  return 0;
}