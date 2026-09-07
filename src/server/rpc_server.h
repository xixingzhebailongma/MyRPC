#pragma once
#include "Connection.h"
#include "EventLoop.h"
#include "TcpServer.h"
#include "ThreadPool.h"
#include "rpc_protocol.h"
#include "service_manager.h"
#include "service_registry.h"
#include <functional>
#include <memory>
class ServiceRegistry;  //前向声明,不需要#include
class IdempotencyStore; // 新增：幂等存储前向声明

class RpcServer {
public:
  RpcServer(const std::string &ip, uint16_t port, int threadnum = 4,
            int workthreadnum = 8);
  ~RpcServer();

  void start();
  void stop();

  void submitTask(std::function<void()> task);

  //开启etcd服务注册(在start()之前调用)
  void enableRegistry(const std::string &etcdEndpoints,
                      const std::string &serviceName, const std::string &ip,
                      uint16_t port, int64_t ttl = 30);
  //注入幂等存储(在start()之前调用)。默认已构造为 IdempotencyLru。
  void setIdempotencyStore(std::unique_ptr<IdempotencyStore> store);
  ServiceManager &serviceManager() { return serviceMgr_; }
  void setTimeoutCallback(std::function<void(EventLoop *)> cb);
  void setPeriodTimer(double interval, std::function<void(EventLoop *)> cb);
  void setIdleTimeout(double seconds);

private:
  void onMessage(spConnection conn, Buffer &buf);
  void dispatch(spConnection conn, std::string payload);
  void onConnection(spConnection conn);
  void rejectOverloaded(spConnection conn, uint64_t seq, bool dedup_enabled,
                        const std::string &cache_key, IdempotencyStore *idem);

  TcpServer server_;
  ThreadPool workPool_; //声明在server_之后
  ServiceManager serviceMgr_;
  std::unique_ptr<ServiceRegistry> registry_;
  std::unique_ptr<IdempotencyStore> idemStore_; // 新增
};