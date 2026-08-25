#include "service_registry.h"
#include "Logger.h"
#include "etcd_client.h"
#include <string>

ServiceRegistry::ServiceRegistry(const std::string &etcdEndpoints,
                                 const std::string &serviceName,
                                 const std::string &ip, uint16_t port,
                                 int64_t ttl)
    : etcd_(std::make_shared<EtcdClient>(etcdEndpoints)),
      serviceName_(serviceName), address_(ip + ":" + std::to_string(port)),
      ttl_(ttl) {
  // key 格式必须与 service_discovery.cc 的查询前缀保持一致
  key_ = "/myrpc/services/" + serviceName_ + "/" + address_;
}

ServiceRegistry::~ServiceRegistry() { stop(); }

bool ServiceRegistry::registerService() {
  // grant lease + put + 后台续约/恢复，全部交给 EtcdClient 托管。
  // 注意：无论返回什么，后台线程都已启动，所以 active_ 一律置 true，
  // 保证 stop() 会去把它停掉。
  active_ = true;
  if (!etcd_->registerWithLease(key_, address_, ttl_)) {
    LOG_WARN("ServiceRegistry: first registration of %s failed, "
             "retrying in background",
             key_.c_str());
    return false;
  }
  LOG_INFO("ServiceRegistry: registered %s", key_.c_str());
  return true;
}



void ServiceRegistry::stop() {
  if (!active_.exchange(false))
    return;
  // 停后台线程 + 撤销 lease（key 随之自动删除）
  etcd_->unregister(key_);
  LOG_INFO("ServiceRegistry: deregistered %s", address_.c_str());
}