#include "lb_rpc_client.h"
#include "Logger.h"
#include "load_balancer.h"
#include "rpc_channel.h"
#include "rpc_error_code.h"
#include "service_discovery.h"
#include "metrics_registry.h"
#include "uuid.h"
#include <algorithm>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>
LbRpcClient::LbRpcClient(const std::string &etcdEndpoints,
                         const std::string &serviceName,
                         std::shared_ptr<ILoadBalancer> balancer,
                         const RpcClientConfig &cfg)
    : discovery_(std::make_shared<ServiceDiscovery>(etcdEndpoints)),
      balancer_(std::move(balancer)), serviceName_(serviceName), cfg_(cfg),
      channel_pool_(cfg) {
  // 节点表由 watch 首轮 Resync 同步；首调若早于 Resync 落地，走 pick/Call
  // 路径的懒 discover 兜底，所以这里不再同步全量 refreshNodes。
  LOG_INFO("LbRpcClient: initialized for service=%s (nodes synced via watch)",
           serviceName_.c_str());

  // etcd watch：节点上下线以增量 Add/Remove 应用，只在首连/compacted/断连过久
  // 时投递一次 ReplaceAll（全量）。无需定时轮询兜底。
  discovery_->watch(serviceName_, [this](const ServiceNodeEvent &e) {
    onNodeEvent(e);
  });
  registerMetrics();
}
// watch 推送的节点变化：增量应用 Add/Remove，全量 ReplaceAll 直接替换
void LbRpcClient::onNodeEvent(const ServiceNodeEvent &e) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<ServiceNode> updated = nodes_;
  switch (e.type) {
    case ServiceNodeEvent::Type::ReplaceAll:
      updated = e.all;
      break;
    case ServiceNodeEvent::Type::Add: {
      bool exists = false;
      for (const auto &n : updated) {
        if (n.address() == e.node.address()) {
          exists = true;
          break;
        }
      }
      if (!exists)
        updated.push_back(e.node);
      break;
    }
    case ServiceNodeEvent::Type::Remove:
      updated.erase(std::remove_if(updated.begin(), updated.end(),
                                   [&](const ServiceNode &n) {
                                     return n.address() == e.address;
                                   }),
                    updated.end());
      break;
  }
  applyNodes(std::move(updated));
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
  // 回收已下线节点的连接，避免连接池无限增长
  std::unordered_set<std::string> alive(nodeIds.begin(), nodeIds.end());
  channel_pool_.removeExcept(alive);
}

std::optional<ServiceNode> LbRpcClient::pickNode(const std::string *key) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 无节点时尝试从 etcd 刷新
  if (nodes_.empty()) {
    if (auto newNodes = discovery_->discover(serviceName_))
      applyNodes(std::move(*newNodes));
  }
  if (nodes_.empty()) {
    LOG_ERROR("LbRpcClient: no available nodes for %s", serviceName_.c_str());
    return std::nullopt;
  }
  size_t idx = key ? balancer_->select(nodes_.size(), *key)
                   : balancer_->select(nodes_.size());
  return nodes_[idx];
}

std::optional<ServiceNode>
LbRpcClient::pickNodeExcept(const std::unordered_set<std::string> &exclude) {
  std::lock_guard<std::mutex> lock(mutex_);
  // 无节点时尝试从 etcd 刷新
  if (nodes_.empty()) {
    if (auto newNodes = discovery_->discover(serviceName_))
      applyNodes(std::move(*newNodes));
  }
  if (nodes_.empty()) {
    LOG_ERROR("LbRpcClient: no available nodes for %s", serviceName_.c_str());
    return std::nullopt;
  }
  // 收集未被排除的候选节点下标
  std::vector<size_t> candidates;
  for (size_t i = 0; i < nodes_.size(); ++i) {
    if (!exclude.count(nodes_[i].address()))
      candidates.push_back(i);
  }
  if (candidates.empty()) {
    LOG_ERROR("LbRpcClient: all %zu nodes excluded for %s", nodes_.size(),
              serviceName_.c_str());
    return std::nullopt;
  }
  // 优先用 balancer 选；命中被排除节点时，在候选里轮询分摊（避免固定取首个）
  size_t idx = balancer_->select(nodes_.size());
  if (exclude.count(nodes_[idx].address())) {
    size_t rr = rr_counter_.fetch_add(1, std::memory_order_relaxed);
    idx = candidates[rr % candidates.size()];
  }
  return nodes_[idx];
}

bool LbRpcClient::Call(const std::string &methodName,
                       const std::string &requestBody,
                       std::string &responseBody, int32_t &errorCode,
                       int maxRetries, int timeout_ms) {
  return CallImpl(methodName, requestBody, responseBody, errorCode, maxRetries,
                  nullptr, timeout_ms);
}

bool LbRpcClient::Call(const std::string &methodName,
                       const std::string &requestBody,
                       std::string &responseBody, int32_t &errorCode,
                       const std::string &key, int maxRetries, int timeout_ms) {
  return CallImpl(methodName, requestBody, responseBody, errorCode, maxRetries,
                  &key, timeout_ms);
}
bool LbRpcClient::CallImpl(const std::string &methodName,
                           const std::string &requestBody,
                           std::string &responseBody, int32_t &errorCode,
                           int maxRetries, const std::string *key,
                           int timeout_ms) {
  // maxRetries < 0 表示用集中配置里的默认重试次数
  if (maxRetries < 0) {
    maxRetries = cfg_.max_retries;
  }
  // 本轮调用已尝试过的节点地址，保证 failover 不会重复打同一个节点
  std::unordered_set<std::string> tried;
  // 每个逻辑调用生成一个幂等键，跨 failover 复用同一个 key
  const std::string request_id = generateUuid();
  // 总共最多尝试 maxRetries+1 个节点（含首次）
  for (int attempt = 0; attempt <= maxRetries; ++attempt) {
    ServiceNode node;
    {
      std::lock_guard<std::mutex> lock(mutex_);

      // 无节点时，尝试从 etcd 刷新（首调可能早于 watch 首轮 Resync，兜底）
      if (nodes_.empty()) {
        if (auto newNodes = discovery_->discover(serviceName_))
          applyNodes(std::move(*newNodes));
      }
      if (nodes_.empty()) {
        LOG_ERROR("LbRpcClient: no available nodes for %s",
                  serviceName_.c_str());
        errorCode = static_cast<int32_t>(RpcError::NO_AVAILABLE_NODE);
        return false;
      }

      // 收集本轮可用节点下标：跳过已尝试(tried)与熔断中(broken)的节点。
      std::vector<size_t> candidates;
      std::unordered_set<size_t> broken;
      for (size_t i = 0; i < nodes_.size(); ++i) {
        if (tried.count(nodes_[i].address()))
          continue;
        auto ch = channel_pool_.get(nodes_[i].address());
        if (ch && ch->isCircuitOpen()) {
          broken.insert(i);
          continue;
        }
        candidates.push_back(i);
      }
      if (candidates.empty()) {
        LOG_ERROR("LbRpcClient: all %zu nodes have failed or are "
                  "circuit-broken for %s",
                  nodes_.size(), serviceName_.c_str());
        errorCode = static_cast<int32_t>(RpcError::NO_AVAILABLE_NODE);
        return false;
      }

      size_t idx;
      if (key) {
        // 一致性哈希：首选节点失败后沿环顺时针找下一个未尝试/未熔断的物理节点。
        std::unordered_set<size_t> excluded = broken;
        for (size_t i = 0; i < nodes_.size(); ++i) {
          if (tried.count(nodes_[i].address()))
            excluded.insert(i);
        }
        idx = balancer_->select(nodes_.size(), *key, excluded);
      } else {
        idx = balancer_->select(nodes_.size());
        if (tried.count(nodes_[idx].address()) || broken.count(idx)) {
          // 轮询/随机又命中已尝试或已熔断的节点，在剩余候选里轮询分摊。
          size_t rr = rr_counter_.fetch_add(1, std::memory_order_relaxed);
          idx = candidates[rr % candidates.size()];
        }
      }
      node = nodes_[idx];
      tried.insert(node.address());
    }
    std::shared_ptr<RpcChannel> channel =
        channel_pool_.getOrCreate(node.address(), node.ip, node.port);
    if (channel->Call(serviceName_, methodName, requestBody, responseBody,
                      errorCode, timeout_ms, request_id)) {
      // 运输成功但业务返回了「可重试」错误码：继续 failover（复用同一
      // request_id）， 让对端因「失败不缓存」而真正重执行。
      if (isRetryable(errorCode)) {
        failover_count_.fetch_add(1, std::memory_order_relaxed);
        LOG_WARN("LbRpcClient: attempt %d got retryable business error %d, "
                 "failover...",
                 attempt + 1, errorCode);
        continue;
      }
      return true; // 成功，或不可重试的业务结果，直接返回
    }
    // 仅临时性错误才 failover；不可重试错误（如 SERVER_ERROR/UNKNOWN）直接失败
    if (!isRetryable(errorCode)) {
      LOG_ERROR("LbRpcClient: non-retryable error %d, aborting", errorCode);
      return false;
    }
    failover_count_.fetch_add(1, std::memory_order_relaxed);
    LOG_WARN("LbRpcClient: call attempt %d failed, failover...", attempt + 1);
  }
  LOG_ERROR("LbRpcClient: RPC call failed after %d attempts", maxRetries + 1);
  return false;
}

LbRpcClient::~LbRpcClient() {
  unregisterMetrics();
  // 停 watch（阻塞等 watch 线程退出），再随成员析构销毁
  discovery_->stopWatch();
}

void LbRpcClient::registerMetrics() {
  metrics_token_ = MetricsRegistry::instance().registerClient(
      MetricsRegistry::ClientKind::kFailover, [this]() {
        ClientCounters c;
        c.failover = failover_count_.load(std::memory_order_relaxed);
        return c;
      });
}

void LbRpcClient::unregisterMetrics() {
  if (metrics_token_ != 0) {
    MetricsRegistry::instance().unregisterClient(metrics_token_);
    metrics_token_ = 0;
  }
}
