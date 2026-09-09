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

struct IdemLease;   // 新增：fencing token 结构前向声明
class LeaseRenewer; // 新增：租约续期器前向声明

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
  // 执行软截止：handler 运行超过该时长即释放租约（0 = 禁用，仅续租不截止）
  void setExecutionDeadlineMs(uint64_t ms) { execution_deadline_ms_ = ms; }
  // 幂等 key 命名空间（默认 "default"）
  void setNamespace(const std::string &ns) { namespace_ = ns; }
  // 调用方身份提取：返回空串则不参与 key 隔离。IM 可注入 user_id/gateway_id
  // 等。
  void setCallerIdExtractor(std::function<std::string(const RpcHeader &)> f) {
    caller_id_extractor_ = std::move(f);
  }
  ServiceManager &serviceManager() { return serviceMgr_; }
  void setTimeoutCallback(std::function<void(EventLoop *)> cb);
  void setPeriodTimer(double interval, std::function<void(EventLoop *)> cb);
  void setIdleTimeout(double seconds);

private:
  void onMessage(spConnection conn, Buffer &buf);
  void dispatch(spConnection conn, std::string payload);
  void onConnection(spConnection conn);
  void rejectOverloaded(spConnection conn, uint64_t seq, bool dedup_enabled,
                        const std::string &cache_key, IdempotencyStore *idem,
                        const IdemLease &lease, LeaseRenewer *renewer);
  std::string buildCacheKey(const std::string &service_name,
                            const std::string &method_name,
                            const std::string &request_id,
                            const RpcHeader &header);
  bool isValidRequestId(const std::string &request_id);
  TcpServer server_;
  ThreadPool workPool_; //声明在server_之后
  ServiceManager serviceMgr_;
  std::unique_ptr<ServiceRegistry> registry_;
  std::unique_ptr<IdempotencyStore> idemStore_; // 新增
  std::unique_ptr<LeaseRenewer> renewer_; // 后台租约续期，start() 时创建
  uint64_t execution_deadline_ms_ = 30000; // 执行软截止（0 = 禁用）
  std::string namespace_ = "default";      // 幂等 key 命名空间
  std::function<std::string(const RpcHeader &)> caller_id_extractor_;
  // 调用方身份
};