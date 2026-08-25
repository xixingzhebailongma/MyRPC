#include "lb_rpc_client.h"
#include "Logger.h"
#include "load_balancer.h"
#include "service_discovery.h"
#include "rpc_channel.h"
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>
LbRpcClient::LbRpcClient(const std::string &etcdEndpoints,
                         const std::string &serviceName,
                         std::shared_ptr<ILoadBalancer> balancer)
    : discovery_(std::make_shared<ServiceDiscovery>(etcdEndpoints)),
      balancer_(std::move(balancer)), serviceName_(serviceName) {
  refreshNodes();
  LOG_INFO("LbRpcClient: initialized for service=%s, found %zu nodes",
           serviceName_.c_str(), nodes_.size());

  // etcd watch：节点上下线立刻感知 -> 立即 rebuild 哈希环。
  // watch 流断开后 EtcdClient 会自动重连，且每次重连成功都会再回调一次，
  // 触发全量重拉补齐断连期间丢失的事件，因此无需额外的定时轮询兜底。
  discovery_->watch(serviceName_, [this]() {
    LOG_INFO("LbRpcClient: etcd watch triggered, refreshing %s",
             serviceName_.c_str());
    refreshNodes();
  });
}
void LbRpcClient::refreshNodes() {
  auto newNodes = discovery_->discover(serviceName_);
  std::lock_guard<std::mutex> lock(mutex_);
  if (!newNodes) {
    // 查询失败 ≠ 服务下线：保留上一次已知的节点表继续提供服务，
    // 避免 etcd 瞬时抖动把哈希环清空、导致客户端集体失明。
    LOG_WARN("LbRpcClient: discover %s failed, keeping %zu stale nodes",
             serviceName_.c_str(), nodes_.size());
    return;
  }
  applyNodes(std::move(*newNodes));
}
// 替换节点列表并通知 balancer 重建内部状态（一致性哈希 ring 等）
// 前提：调用前必须已持有 mutex_
void LbRpcClient::applyNodes(std::vector<ServiceNode> newNodes) {
  nodes_ = std::move(newNodes);
  std::vector<std::string> nodeIds;
  nodeIds.reserve(nodes_.size());
  for (const auto &node : nodes_) {
    nodeIds.push_back(node.address());
  }
  balancer_->rebuild(nodeIds);
}

RpcChannel *LbRpcClient::getChannel(const std::string &ip, uint16_t port) {
  std::string addr = ip + ":" + std::to_string(port);
  auto it = channels_.find(addr);
  if (it != channels_.end()) {
    return it->second.get();
  }
  auto channel = std::make_unique<RpcChannel>(ip, port);
  RpcChannel *raw = channel.get();
  channels_[addr] = std::move(channel);
  return raw;
}

bool LbRpcClient::Call(const std::string &methodName,
                       const std::string &requestBody,
                       std::string &responseBody, int32_t &errorCode,
                       int maxRetries) {
  return CallImpl(methodName, requestBody, responseBody, errorCode, maxRetries,
                  nullptr);
}

bool LbRpcClient::Call(const std::string &methodName,
                       const std::string &requestBody,
                       std::string &responseBody, int32_t &errorCode,
                       const std::string &key, int maxRetries) {
  return CallImpl(methodName, requestBody, responseBody, errorCode, maxRetries,
                  &key);
}

bool LbRpcClient::CallImpl(const std::string &methodName,
                           const std::string &requestBody,
                           std::string &responseBody, int32_t &errorCode,
                           int maxRetries, const std::string *key) {
  // 本轮调用已尝试过的节点地址，保证 failover 不会重复打同一个节点
  std::unordered_set<std::string> tried;
  // 总共最多尝试 maxRetries+1 个节点（含首次）
  for (int attempt = 0; attempt <= maxRetries; ++attempt) {
    RpcChannel *channel = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);

      // 无节点时，尝试从 etcd 刷新
      if (nodes_.empty()) {
        if (auto newNodes = discovery_->discover(serviceName_))
          applyNodes(std::move(*newNodes));
      }

      if (nodes_.empty()) {
        LOG_ERROR("LbRpcClient: no available nodes for %s",
                  serviceName_.c_str());
        return false;
      }

      //收集尚未尝试过的节点下标
      std::vector<size_t> candidates;
      for (size_t i = 0; i < nodes_.size(); ++i) {
        if (!tried.count(nodes_[i].address())) {
          candidates.push_back(i);
        }
      }
      if (candidates.empty()) {
        LOG_ERROR("LbRpcClient: all %zu nodes have failed for %s",
                  nodes_.size(), serviceName_.c_str());
        return false;
      }

      size_t idx;
      if (key) {
        // 一致性哈希：优先命中 key 对应的固定节点
        idx = balancer_->select(nodes_.size(), *key);
        if (tried.count(nodes_[idx].address())) {
          // 首选节点已失败，改从剩余候选里兜底
          idx = candidates.front();
        }
      } else {
        idx = balancer_->select(nodes_.size());
        if (tried.count(nodes_[idx].address())) {
          // 轮询/随机又命中已尝试过的节点，跳过
          idx = candidates.front();
        }
      }
      ServiceNode node = nodes_[idx];
      tried.insert(node.address());
      channel = getChannel(node.ip, node.port);
    }
    if (channel->Call(serviceName_, methodName, requestBody, responseBody,
                      errorCode)) {
      return true;
    }
    LOG_WARN("LbRpcClient: call attempt %d failed, failover...", attempt + 1);
  }
  LOG_ERROR("LbRpcClient: RPC call failed after %d attempts", maxRetries + 1);
  return false;
}

LbRpcClient::~LbRpcClient() {
  // 停 watch（阻塞等 watch 线程退出），再随成员析构销毁
  discovery_->stopWatch();
}
