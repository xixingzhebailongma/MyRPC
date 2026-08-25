#pragma once
#include <string>
#include <atomic>
#include <cstddef>
#include <random>
#include <vector>
#include <map>
#include <functional>
//负载均衡接口
class ILoadBalancer {
public:
  virtual ~ILoadBalancer() = default;
  //从nodeCount个节点中选一个下标返回
  virtual size_t select(size_t nodeCount) = 0;
  //新增：根据 key（如 user_id）选择节点，默认回退到无 key 版本
  virtual size_t select(size_t nodeCount,const std::string& key){
    return select(nodeCount);
  }
  //当节点列表变化时通知负载均衡器重建内部状态
  virtual void rebuild(const std::vector<std::string>&nodeIds){}
};

//轮询策略
class RoundRobinBalancer : public ILoadBalancer {
public:
  size_t select(size_t nodeCount) override {
    if (nodeCount == 0)
      return 0;
    return index_.fetch_add(1, std::memory_order_relaxed) % nodeCount;
  }
  void rebuild(const std::vector<std::string>&nodeIds)override;
private:
  std::atomic<size_t> index_{0};
};

class ConsistentHashBalancer:public ILoadBalancer{
  public:
  explicit ConsistentHashBalancer(int virtualNodes = 150);

  size_t select(size_t nodeCount)override;    //随机选一个节点
  size_t select(size_t nodeCount,const std::string& key) override;  //按key hash选

  //当节点列表变化时重建 ring（后续可优化为增量更新）
  void rebuild(const std::vector<std::string>&nodeIds);
  private:
  int virtual_nodes_;
  std::map<uint32_t,size_t>ring_; //hash->物理节点index
  std::hash<std::string>hasher_;
};
//随机策略
class RandomBalancer:public ILoadBalancer{
    public:
    RandomBalancer():gen_(std::random_device{}()){}
    size_t select(size_t nodeCount)override{
        if(nodeCount == 0)return 0;
        std::uniform_int_distribution<size_t>dist(0,nodeCount-1);
        return dist(gen_);
    }
    private:
    std::mt19937 gen_;
};