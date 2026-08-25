#pragma once

  #include "load_balancer.h"
  #include "service_discovery.h"
  #include "rpc_channel.h"
  #include <cstdint>
  #include <memory>
  #include <mutex>
  #include <string>
  #include <unordered_map>
  #include <vector>

  class LbRpcClient {
  public:
    // etcdEndpoints:"https/127.0.0.1:2379"
    // serviceName: 要调用的服务名，如 "UserService"
    //  balancer:      负载均衡策略
    LbRpcClient(const std::string &etcdEndpoints, const std::string &serviceName,
                std::shared_ptr<ILoadBalancer> balancer);
    ~LbRpcClient();
    // 同步 RPC 调用
    // 失败时自动 failover 到下一个节点
    // maxRetries: 最大重试次数（默认 3，即最多尝试 4 个节点）

    bool Call(const std::string &methodName, const std::string &requestBody,
              std::string &responseBody, int32_t &errorCode, int maxRetries = 3);
    // Call重载
    bool Call(const std::string &methodName, const std::string &requestBody,
              std::string &responseBody, int32_t &errorCode,
              const std::string &key, int maxRetries = 3);

  private:
    //从etcd刷新节点列表
    void refreshNodes();
    // 替换节点列表并重建 balancer 内部状态（一致性哈希 ring 等）
    // 前提：调用前必须已持有 mutex_
    void applyNodes(std::vector<ServiceNode> newNodes);
    bool CallImpl(const std::string &methodName, const std::string &requestBody,
                  std::string &responseBody, int32_t &errorCode, int maxRetries,
                  const std::string *key);
    // 获取或创建到指定地址的 RpcChannel（连接池复用）
    RpcChannel *getChannel(const std::string &ip, uint16_t port);

    std::shared_ptr<ServiceDiscovery> discovery_;
    std::shared_ptr<ILoadBalancer> balancer_;
    std::string serviceName_;

    std::vector<ServiceNode> nodes_;
    std::unordered_map<std::string, std::unique_ptr<RpcChannel>> channels_;
    std::mutex mutex_;
  };