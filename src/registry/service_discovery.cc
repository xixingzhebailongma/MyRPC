#include "service_discovery.h"
#include "Logger.h"
#include "etcd_client.h"
#include <arpa/inet.h>

ServiceDiscovery::ServiceDiscovery(const std::string &etcdEndpoints)
    : etcd_(std::make_shared<EtcdClient>(etcdEndpoints)) {}

std::optional<ServiceNode> ServiceDiscovery::parseAddress(const std::string &addr) {
  // value/key 即权威地址 "ip:port"（注册侧写入的就是 address_）
  size_t colon = addr.rfind(':');
  if (colon == std::string::npos || colon + 1 >= addr.size())
    return std::nullopt;

  ServiceNode node;
  node.ip = addr.substr(0, colon);
  try {
    node.port = static_cast<uint16_t>(std::stoi(addr.substr(colon + 1)));
  } catch (const std::exception &) {
    return std::nullopt; // 格式非法，跳过（watch 回调线程里不能抛）
  }
  return node;
}

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
    auto node = parseAddress(kv.second);
    if (node)
      nodes.push_back(*node);
  }
  LOG_INFO("ServiceDiscovery: found %zu nodes for %s", nodes.size(),
           serviceName.c_str());
  return nodes;
}

bool ServiceDiscovery::watch(const std::string &serviceName,
                             std::function<void(const ServiceNodeEvent &)> on_change) {
  // 与 discover() 使用相同的前缀，保证监听范围一致
  std::string prefix = "/myrpc/services/" + serviceName + "/";
  return etcd_->watch(prefix, [prefix, on_change](const EtcdWatchEvent &e) {
    ServiceNodeEvent ev;
    switch (e.type) {
      case EtcdWatchEvent::Type::Resync: {
        ev.type = ServiceNodeEvent::Type::ReplaceAll;
        ev.all.reserve(e.snapshot.size());
        for (const auto &kv : e.snapshot) {
          auto node = parseAddress(kv.second);
          if (node)
            ev.all.push_back(*node);
        }
        break;
      }
      case EtcdWatchEvent::Type::Put: {
        auto node = parseAddress(e.value);
        if (!node)
          return; // 非法 value，跳过
        ev.type = ServiceNodeEvent::Type::Add;
        ev.node = *node;
        break;
      }
      case EtcdWatchEvent::Type::Delete: {
        // key 形如 /myrpc/services/<svc>/<ip:port>，取最后一节作为地址
        std::string addr = e.key;
        if (addr.compare(0, prefix.size(), prefix) == 0)
          addr = addr.substr(prefix.size());
        ev.type = ServiceNodeEvent::Type::Remove;
        ev.address = std::move(addr);
        break;
      }
    }
    on_change(ev);
  });
}

void ServiceDiscovery::stopWatch() { etcd_->stopWatch(); }