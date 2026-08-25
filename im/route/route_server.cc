#include "route_server.h"
#include "EventLoop.h"
#include "im.pb.h"
#include <functional>
RouteServer::RouteServer(const std::string &ip, uint16_t port,
                         const std::string &redis_ip, int redis_port,
                         const std::string &etcd_endpoints,
                         const std::string &service_name, int thread_num)
    : rpc_server_(ip, port, thread_num) {
  // 1.连接redis
  bool ok = redis_.connect(redis_ip, redis_port);
  if (!ok) {
    /* LOG_ERROR + throw 或 exit */
  }
  // 2. 注册 3 个 conn-aware handler
  //    格式: rpc_server_.serviceManager().registerMethod(service_name,
  //    method_name, handler) conn-aware 版本接受
  //    std::function<std::string(spConnection, const std::string&)>

  rpc_server_.serviceManager().registerMethod(
      service_name, "RouteRegister",
      std::bind(&RouteServer::handleRouteRegister, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      service_name, "RouteQuery",
      std::bind(&RouteServer::handleRouteQuery, this, std::placeholders::_1,
                std::placeholders::_2));
  rpc_server_.serviceManager().registerMethod(
      service_name, "RouteUnregister",
      std::bind(&RouteServer::handleRouteUnregister, this,
                std::placeholders::_1, std::placeholders::_2));
  //...RouteQuery,RouteUnregister同理
  // 3.注册到etcd
  rpc_server_.enableRegistry(etcd_endpoints, service_name, ip, port, 30);
}

std::string RouteServer::handleRouteRegister(spConnection conn,
                                             const std::string &request_body) {
  im::RouteRegisterRequest req;
  im::RouteRegisterResponse resp;

  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    std::string result;
    resp.SerializeToString(&result);
    return result;
  }

  std::string key = "im:route:" + req.user_id();

  bool ok = true;
  ok = ok && redis_.hset(key, "server_id", req.server_id());
  ok = ok && redis_.hset(key, "server_ip", req.server_ip());
  ok = ok && redis_.hset(key, "server_port", std::to_string(req.server_port()));

  // 路由变更：广播"上线"事件给所有 IM 节点，让其本地缓存即时预暖
  if (ok) {
    im::RouteChangeEvent ev;
    ev.set_user_id(req.user_id());
    ev.set_online(true);
    ev.set_server_id(req.server_id());
    ev.set_server_ip(req.server_ip());
    ev.set_server_port(req.server_port());
    std::string payload;
    ev.SerializeToString(&payload);
    redis_.publish("im:route:events", payload);
  }
  resp.set_success(ok);
  std::string result;
  resp.SerializeToString(&result);
  return result;
}

std::string RouteServer::handleRouteQuery(spConnection conn,
                                          const std::string &request_body) {
  im::RouteQueryRequest req;
  im::RouteQueryResponse resp;

  if (!req.ParseFromString(request_body)) {
    resp.set_found(false);
    std::string result;
    resp.SerializeToString(&result);
    return result;
  }

  std::string key = "im:route:" + req.user_id();
  std::string sid = redis_.hget(key, "server_id");

  if (sid.empty()) {
    resp.set_found(false);
  } else {
    resp.set_found(true);
    resp.set_server_id(sid);
    resp.set_server_ip(redis_.hget(key, "server_ip"));
    // server_port存的是字符串，需要转回int32
    std::string port_str = redis_.hget(key, "server_port");
    if (!port_str.empty()) {
      resp.set_server_port(std::stoi(port_str));
    }
  }
  std::string result;
  resp.SerializeToString(&result);
  return result;
}

std::string
RouteServer::handleRouteUnregister(spConnection conn,
                                   const std::string &request_body) {
  im::RouteUnregisterRequest req;
  im::RouteUnregisterResponse resp;

  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    std::string result;
    resp.SerializeToString(&result);
    return result;
  }

  std::string key = "im:route:" + req.user_id();
  redis_.del(key);
  // 路由变更：通知各 IM 节点失效该用户的本地缓存
  im::RouteChangeEvent ev;
  ev.set_user_id(req.user_id());
  ev.set_online(false);
  std::string payload;
  ev.SerializeToString(&payload);
  redis_.publish("im:route:events", payload);
  resp.set_success(true);
  std::string result;
  resp.SerializeToString(&result);
  return result;
}

void RouteServer::start() { rpc_server_.start(); }
void RouteServer::stop() { rpc_server_.stop(); }