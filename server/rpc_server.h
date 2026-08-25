#pragma once
#include "Connection.h"
#include "EventLoop.h"
#include "TcpServer.h"
#include "ThreadPool.h"
#include "registry/service_registry.h"
#include "rpc_protocol.h"
#include "service_manager.h"
#include <functional>
class ServiceRegistry; //前向声明,不需要#include

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

  ServiceManager &serviceManager() { return serviceMgr_; }
  void setCloseConnectionCallback(std::function<void(spConnection)> cb);
  void setTimeoutCallback(std::function<void(EventLoop *)> cb);
  void setPeriodTimer(double interval,std::function<void(EventLoop*)>cb);
  void setIdleTimeout(double seconds);

private:
  void onMessage(spConnection conn, std::string &message);
  void onConnection(spConnection conn);

  TcpServer server_;
  ThreadPool workPool_; //声明在server_之后
  ServiceManager serviceMgr_;
  std::unique_ptr<ServiceRegistry> registry_;
  std::function<void(spConnection)> closeConnectionCb_;
};