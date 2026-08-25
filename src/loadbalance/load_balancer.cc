#include "load_balancer.h"
#include <cstdlib> //rand
#include <string>

ConsistentHashBalancer::ConsistentHashBalancer(int virtualNodes)
    : virtual_nodes_(virtualNodes) {}

//无key:随机选节点
size_t ConsistentHashBalancer::select(size_t nodeCount) {
  if (nodeCount == 0)
    return 0;
  return std::hash<std::string>{}(std::to_string(rand())) % nodeCount;
}

//有key:hash ring查找
size_t ConsistentHashBalancer::select(size_t nodeCount,
                                      const std::string &key) {
  if (ring_.empty() || nodeCount == 0)
    return 0;
  uint32_t hash = hasher_(key);
  auto it = ring_.lower_bound(hash);
  if (it == ring_.end()) {
    it = ring_.begin(); //环的闭环特性
  }
  return it->second % nodeCount;
}

//重建hash ring
void ConsistentHashBalancer::rebuild(const std::vector<std::string> &nodeIds) {
  ring_.clear();
  for (size_t i = 0; i < nodeIds.size(); ++i) {
    for (int v = 0; v < virtual_nodes_; ++v) {
      std::string vnode = nodeIds[i] + "_vnode_" + std::to_string(v);
      uint32_t hash = hasher_(vnode);
      ring_[hash] = i;
    }
  }
}

//轮询：节点列表变化时重置计数器
void RoundRobinBalancer::rebuild(const std::vector<std::string>&nodeIds){
  index_.store(0,std::memory_order_relaxed);
}