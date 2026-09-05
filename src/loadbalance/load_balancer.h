#pragma once
#include <atomic>
#include <cstddef>
#include <functional>
#include <map>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>
//负载均衡接口
class ILoadBalancer {
public:
  virtual ~ILoadBalancer() = default;
  //从nodeCount个节点中选一个下标返回
  virtual size_t select(size_t nodeCount) = 0;
  //新增：根据 key（如 user_id）选择节点，默认回退到无 key 版本
  virtual size_t select(size_t nodeCount, const std::string &key) {
    return select(nodeCount);
  }
  //按 key 选择节点，跳过 exclude 中的物理节点下标。
  //一致性哈希实现沿环顺时针找下一个未排除节点；其余实现回退到 select(nodeCount,
  // key)。
  virtual size_t select(size_t nodeCount, const std::string &key,
                        const std::unordered_set<size_t> &exclude) {
    return select(nodeCount, key);
  }
  //当节点列表变化时通知负载均衡器重建内部状态
  virtual void rebuild(const std::vector<std::string> &nodeIds) {}
};

//轮询策略
class RoundRobinBalancer : public ILoadBalancer {
public:
  size_t select(size_t nodeCount) override {
    if (nodeCount == 0)
      return 0;
    return index_.fetch_add(1, std::memory_order_relaxed) % nodeCount;
  }
  void rebuild(const std::vector<std::string> &nodeIds) override;

private:
  std::atomic<size_t> index_{0};
};

class ConsistentHashBalancer : public ILoadBalancer {
public:
  explicit ConsistentHashBalancer(int virtualNodes = 150);

  size_t select(size_t nodeCount) override; //随机选一个节点
  size_t select(size_t nodeCount,
                const std::string &key) override; //按key hash选
  size_t select(size_t nodeCount, const std::string &key,
                const std::unordered_set<size_t> &exclude) override;
  //当节点列表变化时重建 ring（后续可优化为增量更新）
  void rebuild(const std::vector<std::string> &nodeIds);

private:
  int virtual_nodes_;
  std::map<uint32_t, size_t> ring_; // hash->物理节点index
  std::hash<std::string> hasher_;
};
//随机策略
class RandomBalancer : public ILoadBalancer {
public:
  RandomBalancer() : gen_(std::random_device{}()) {}
  size_t select(size_t nodeCount) override {
    if (nodeCount == 0)
      return 0;
    std::uniform_int_distribution<size_t> dist(0, nodeCount - 1);
    return dist(gen_);
  }

private:
  std::mt19937 gen_;
};