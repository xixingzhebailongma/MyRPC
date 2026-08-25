#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

class EtcdClient;

class ServiceRegistry {
public:
  // etcdEndpoints: "http://127.0.0.1:2379"
  // serviceName: 服务名,如"UserService"
  // ip /port: 本服务监听的地址
  // ttl:      lease存活时间（秒),默认30

  ServiceRegistry(const std::string &etcdEndPoints,
                  const std::string &servcieName, const std::string &ip,
                  uint16_t port, int64_t ttl = 30);
  ~ServiceRegistry();

  // 注册服务：委托给 EtcdClient 的托管注册（grant lease + put +
  // 后台自动续约/恢复）。 返回 false
  // 只表示首轮没成功，后台仍会持续重试，服务可降级运行。
  bool registerService();

  

  // 停止续约并注销服务
  void stop();

private:
  std::shared_ptr<EtcdClient> etcd_; //共享，方便后续扩展
  std::string serviceName_;
  std::string address_; //"ip:port"
  std::string key_;     //"/myrpc/services/<name>/<ip:port>"
  int64_t ttl_;
  std::atomic<bool> active_{false};
};