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

  std::cout << "OK: consistent hash ring failover spreads keys\n";
  return 0;
}