#pragma once
#include "async_rpc_client.h"
#include "load_balancer.h"
#include "rpc_client_config.h"
#include "service_discovery.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

// 异步版 LbRpcClient：etcd 服务发现 + 负载均衡 + 异步 failover + request_id
// 幂等。 与同步版行为对齐，但 failover 走回调链，全程不阻塞、不新开线程。
//
// 生命周期约定：本对象须比所有在途调用活得更久（不要在还有未决 future
// 时析构）。
class AsyncLbRpcClient {
public:
  using ResponseCallback =
      std::function<void(std::string body, int32_t error_code)>;

  // etcdEndpoints: "http://127.0.0.1:2379"；serviceName: 要调用的服务名。
  AsyncLbRpcClient(const std::string &etcdEndpoints,
                   const std::string &serviceName,
                   std::shared_ptr<ILoadBalancer> balancer,
                   const RpcClientConfig &cfg = {});
  ~AsyncLbRpcClient();

  std::future<std::string> Call(const std::string &methodName,
                                const std::string &requestBody,
                                int timeout_ms = -1);
  std::future<std::string> Call(const std::string &methodName,
                                const std::string &requestBody,
                                const std::string &key, int timeout_ms = -1);

  void Call(const std::string &methodName, const std::string &requestBody,
            ResponseCallback cb, int timeout_ms = -1);
  void Call(const std::string &methodName, const std::string &requestBody,
            const std::string &key, ResponseCallback cb, int timeout_ms = -1);

  uint64_t failoverCount() const { return failover_count_.load(); }

private:
  struct CallState {
    std::string methodName;
    std::string requestBody;
    std::string request_id;
    int maxRetries;
    int timeout_ms;
    std::optional<std::string> key; // 一致哈希 key（无则空）
    std::unordered_set<std::string> tried;
    std::shared_ptr<std::promise<std::string>> promise;
    ResponseCallback cb;
  };

  void refreshNodes();
  void applyNodes(std::vector<ServiceNode> newNodes);
  std::optional<ServiceNode>
  pickNodeForAsync(const std::unordered_set<std::string> &tried,
                   const std::string *key);
  void attemptCall(std::shared_ptr<CallState> st);
  void onResponse(std::shared_ptr<CallState> st, int32_t ec, std::string body);
  void deliver(std::shared_ptr<CallState> st, std::string body, int32_t ec);
  void CallImpl(const std::string &methodName, const std::string &requestBody,
                const std::string *key, int timeout_ms,
                std::shared_ptr<std::promise<std::string>> promise,
                ResponseCallback cb);

  std::shared_ptr<ServiceDiscovery> discovery_;
  std::shared_ptr<ILoadBalancer> balancer_;
  std::string serviceName_;
  std::vector<ServiceNode> nodes_;
  std::atomic<uint64_t> rr_counter_{0};
  std::atomic<uint64_t> failover_count_{0};
  RpcClientConfig cfg_;
  AsyncRpcClient client_;
  std::mutex mutex_;
};