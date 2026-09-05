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
//沿环顺时针找第一个未被排除的物理节点（跳过 exclude 中的下标）。
//首选节点失败后调用，让故障节点的 key 均匀打散到其余节点。
size_t
ConsistentHashBalancer::select(size_t nodeCount, const std::string &key,
                               const std::unordered_set<size_t> &exclude) {
  if (ring_.empty() || nodeCount == 0)
    return 0;
  uint32_t hash = hasher_(key);
  auto it = ring_.lower_bound(hash);
  size_t scanned = 0;
  while (scanned < ring_.size()) {
    if (it == ring_.end())
      it = ring_.begin(); //环的闭环特性
    size_t idx = it->second % nodeCount;
    ++it;
    ++scanned;
    if (!exclude.count(idx))
      return idx;
  }
  // 全部节点都被排除（调用方 CallImpl 会先用 candidates.empty()
  // 兜住，不会走到这里）
  auto first = ring_.lower_bound(hash);
  if (first == ring_.end())
    first = ring_.begin();
  return first->second % nodeCount;
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
void RoundRobinBalancer::rebuild(const std::vector<std::string> &nodeIds) {
  index_.store(0, std::memory_order_relaxed);
}