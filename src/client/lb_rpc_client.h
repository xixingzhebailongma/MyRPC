#pragma once

#include "load_balancer.h"
#include "rpc_channel.h"
#include "rpc_channel_pool.h"
#include "rpc_client_config.h"
#include "service_discovery.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>
class LbRpcClient {
public:
  // etcdEndpoints:"https/127.0.0.1:2379"
  // serviceName: 要调用的服务名，如 "UserService"
  //  balancer:      负载均衡策略
  LbRpcClient(const std::string &etcdEndpoints, const std::string &serviceName,
              std::shared_ptr<ILoadBalancer> balancer,
              const RpcClientConfig &cfg = {});
  ~LbRpcClient();
  // 同步 RPC 调用
  // 失败时自动 failover 到下一个节点
  // maxRetries: 最大重试次数（<0 表示用 config.max_retries，默认 3）

  bool Call(const std::string &methodName, const std::string &requestBody,
            std::string &responseBody, int32_t &errorCode, int maxRetries = -1,
            int timeout_ms = -1);
  // Call重载
  bool Call(const std::string &methodName, const std::string &requestBody,
            std::string &responseBody, int32_t &errorCode,
            const std::string &key, int maxRetries = -1, int timeout_ms = -1);
  // failover 次数（每次失败重试 +1），metrics 用
  uint64_t failoverCount() const { return failover_count_.load(); }
  // 选一个节点（不发起调用），供 Gateway 做连接级 pinning。
  // key 非空时按 key 一致哈希；空则按 balancer（轮询/随机）选。
  // 无可用节点返回 std::nullopt。
  std::optional<ServiceNode> pickNode(const std::string *key = nullptr);

  // 选一个不在 exclude 集合中的节点（供故障转移时跳过已失败节点）。
  // 优先用 balancer 选；命中 exclude 时在候选内轮询分摊，避免固定取首个。
  // 无可用节点返回 std::nullopt。
  std::optional<ServiceNode>
  pickNodeExcept(const std::unordered_set<std::string> &exclude);

private:
  // 处理 watch 推送的节点变化（增量 Add/Remove 或全量 ReplaceAll）
  void onNodeEvent(const ServiceNodeEvent &e);
  // 替换节点列表并重建 balancer 内部状态（一致性哈希 ring 等）
  // 前提：调用前必须已持有 mutex_
  void applyNodes(std::vector<ServiceNode> newNodes);
  bool CallImpl(const std::string &methodName, const std::string &requestBody,
                std::string &responseBody, int32_t &errorCode, int maxRetries,
                const std::string *key, int timeout_ms);

  std::shared_ptr<ServiceDiscovery> discovery_;
  std::shared_ptr<ILoadBalancer> balancer_;
  std::string serviceName_;

  std::vector<ServiceNode> nodes_;
  std::atomic<uint64_t> rr_counter_{0};
  std::atomic<uint64_t> failover_count_{0}; // failover 尝试次数（metrics）
  RpcClientConfig cfg_;
  // pickNodeExcept / CallImpl 无 key 兜底轮询计数
  RpcChannelPool channel_pool_;
  std::mutex mutex_;

  // metrics：failover 计数注册（读值回调）
  void registerMetrics();
  void unregisterMetrics();
  uint64_t metrics_token_{0};
};