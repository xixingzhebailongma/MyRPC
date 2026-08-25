#include "rpc_server.h"
#include "Connection.h"
#include "Logger.h"
#include "TcpServer.h"
#include "registry/service_registry.h"
#include "rpc_header.pb.h"
#include "rpc_protocol.h"
#include "service_manager.h"
#include <cstdint>
#include <string>

RpcServer::RpcServer(const std::string &ip, uint16_t port, int threadnum,
                     int workthreadnum)
    : server_(ip, port, threadnum), workPool_(workthreadnum, "WORK") {
  //绑定消息回调
  server_.setonmessagecb([this](spConnection conn, std::string &message) {
    this->onMessage(conn, message);
  });
  //绑定新连接回调
  server_.setnewconnectioncb(
      [this](spConnection conn) { this->onConnection(conn); });
  //绑定连接关闭回调
  server_.setcloseconnectioncb([this](spConnection conn) {
    if (closeConnectionCb_)
      closeConnectionCb_(conn);
  });
}

RpcServer::~RpcServer() = default;

void RpcServer::start() {
  server_.start();

  if (registry_) {
    // 首轮失败不再意味着永久降级：EtcdClient 后台线程会持续重试注册
    if (!registry_->registerService()) {
      LOG_WARN("RpcServer: first etcd registration failed, "
               "retrying in background");
    }
  }
}

void RpcServer::stop() {
  if (registry_) {
    registry_->stop();
  }
  server_.stop(); // 先 join 全部 IO 线程，之后不再有新的 onMessage
  workPool_.stop(); // 再排空并 join 工作线程
}

void RpcServer::submitTask(std::function<void()> task) {
  workPool_.addtask(std::move(task));
}

void RpcServer::onMessage(spConnection conn, std::string &message) {
  // message已经被muduo的pickmessage()去掉4字节长度前缀
  //所以这里直接反序列化proto

  RpcMessage request;
  if (!decodeMessage(message, request)) {
    LOG_WARN("RpcServer: failed to decode RPC message from fd=%d", conn->fd());
    return;
  }
  const auto &header = request.header();

  // message/request 是临时引用，不能跨线程引用；按值拷到局部变量
  std::string service_name = header.service_name();
  std::string method_name = header.method_name();
  uint64_t seq = header.sequence_id(); // proto 字段为 uint64
  std::string body = request.body();

  LOG_INFO("RpcServer: request service=%s method=%s seq=%lu",
           header.service_name().c_str(), header.method_name().c_str(),
           header.sequence_id());

  // IO线程内解析handler并按值拷出std::function,避免worker并发查map
  std::function<std::string(spConnection, const std::string &)> callable;
  // 优先查找需要 connection 的 handler
  auto handlerWithConn = serviceMgr_.findMethodWithConn(header.service_name(),
                                                        header.method_name());

  RpcMessage response;
  if (handlerWithConn) {
    // 调用带连接的处理函数
    callable = handlerWithConn; // conn-aware直调
  } else {
    auto handler =
        serviceMgr_.findMethod(header.service_name(), header.method_name());

    if (handler) {
      // 非 conn-aware 忽略 conn
      callable = [handler](spConnection, const std::string &body) {
        return handler(body);
      };
    } else {
      //交给业务线程池执行（捕获 conn 共享指针保活）
      LOG_WARN("RpcServer: method not found: %s.%s",
               header.service_name().c_str(), header.method_name().c_str());
      std::string wire =
          encodeMessage(buildResponse(header.sequence_id(), -1, ""));
      conn->send(wire.data(), wire.size());
      return;
    }
  }
  // 交给业务线程池执行（捕获 conn 共享指针保活）
  workPool_.addtask([conn, callable, seq, body]() {
    std::string resp = callable(conn, body);
    std::string wire = encodeMessage(buildResponse(seq, 0, resp));
    conn->send(wire.data(), wire.size());
  });
}

void RpcServer::onConnection(spConnection conn) {
  LOG_INFO("RpcServer: new connection from %s:%d", conn->ip().c_str(),
           conn->port());
}

void RpcServer::enableRegistry(const std::string &etcdEndpoints,
                               const std::string &serviceName,
                               const std::string &ip, uint16_t port,
                               int64_t tll) {
  registry_ = std::make_unique<ServiceRegistry>(etcdEndpoints, serviceName, ip,
                                                port, tll);
}

void RpcServer::setCloseConnectionCallback(
    std::function<void(spConnection)> cb) {
  closeConnectionCb_ = std::move(cb);
}

void RpcServer::setTimeoutCallback(std::function<void(EventLoop *)> cb) {
  server_.settimeoutcb(std::move(cb));
}
void RpcServer::setPeriodTimer(double interval,
                               std::function<void(EventLoop *)> cb) {
  server_.setPeriodicTimer(interval, std::move(cb));
}
void RpcServer::setIdleTimeout(double seconds) {
  server_.setIdleTimeout(seconds);
}