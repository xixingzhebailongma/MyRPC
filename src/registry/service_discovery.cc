#include "service_discovery.h"
#include "Logger.h"
#include "etcd_client.h"
#include <arpa/inet.h>

ServiceDiscovery::ServiceDiscovery(const std::string &etcdEndpoints)
    : etcd_(std::make_shared<EtcdClient>(etcdEndpoints)) {}

std::optional<std::vector<ServiceNode>>
ServiceDiscovery::discover(const std::string &serviceName) {
  std::string prefix = "/myrpc/services/" + serviceName + "/";
  auto kvs = etcd_->ls(prefix);
  if (!kvs) {
    LOG_WARN("ServiceDiscovery: discover %s failed (etcd unavailable)",
             serviceName.c_str());
    return std::nullopt;
  }

  std::vector<ServiceNode> nodes;
  for (const auto &kv : *kvs) {
    // value 即权威地址 "ip:port"（注册侧写入的就是 address_）
    const std::string &addr = kv.second;
    size_t colon = addr.rfind(':');
    if (colon == std::string::npos || colon + 1 >= addr.size())
      continue;

    ServiceNode node;
    node.ip = addr.substr(0, colon);
    try {
      node.port = static_cast<uint16_t>(std::stoi(addr.substr(colon + 1)));
    } catch (const std::exception &) {
      continue; // 格式非法，跳过该节点（discover 跑在 watch
                // 回调线程里，不能抛）
    }
    nodes.push_back(node);
  }
  LOG_INFO("ServiceDiscovery: found %zu nodes for %s", nodes.size(),
           serviceName.c_str());
  return nodes;
}

bool ServiceDiscovery::watch(const std::string &serviceName,
                             std::function<void()> on_change) {
  // 与 discover() 使用相同的前缀，保证监听范围一致
  std::string prefix = "/myrpc/services/" + serviceName + "/";
  return etcd_->watch(prefix, on_change);
}

void ServiceDiscovery::stopWatch() { etcd_->stopWatch(); }