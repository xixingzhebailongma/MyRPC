#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>
class EtcdClient;

//代表一个服务节点
struct ServiceNode {

  std::string ip;
  uint16_t port;
  std::string address() const { return ip + ":" + std::to_string(port); }
};

class ServiceDiscovery {
public:
  // etcdEndpoints:"http://127.0.0.1:2379"
  explicit ServiceDiscovery(const std::string &etcdEndpoints);
  // 查询某个服务的所有在线节点
  // nullopt = 查询失败；空 vector = 查询成功但当前无节点」
  std::optional<std::vector<ServiceNode>>
  discover(const std::string &serviceName);

  // 监听某个服务的节点变化（新增/下线时回调 on_change）
  bool watch(const std::string &serviceName, std::function<void()> on_change);
  //停止监听
  void stopWatch();

private:
  std::shared_ptr<EtcdClient> etcd_;
};