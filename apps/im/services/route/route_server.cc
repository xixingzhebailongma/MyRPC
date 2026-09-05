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

  // 用 lambda 而非 std::bind：bind 表达式可吞掉多余实参，会同时匹配
  // conn-aware 和 with-context 两个 registerMethod 重载而产生歧义。
  rpc_server_.serviceManager().registerMethod(
      service_name, "RouteRegister",
      [this](spConnection conn, const std::string &body) {
        return handleRouteRegister(conn, body);
      });
  rpc_server_.serviceManager().registerMethod(
      service_name, "RouteQuery",
      [this](spConnection conn, const std::string &body) {
        return handleRouteQuery(conn, body);
      });
  rpc_server_.serviceManager().registerMethod(
      service_name, "RouteUnregister",
      [this](spConnection conn, const std::string &body) {
        return handleRouteUnregister(conn, body);
      });
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

  // field="gateway_id:conn_id", value="ip:port"：连接粒度，天然去重
  std::string field = req.server_id() + ":" + std::to_string(req.conn_id());
  std::string value = req.server_ip() + ":" + std::to_string(req.server_port());
  bool ok = redis_.hset(key, field, value);

  // 路由变更：广播"上线"事件给所有 IM 节点，让其本地缓存即时预暖
  if (ok) {
    im::RouteChangeEvent ev;
    ev.set_user_id(req.user_id());
    ev.set_online(true);
    auto *s = ev.mutable_server();
    s->set_server_id(req.server_id());
    s->set_server_ip(req.server_ip());
    s->set_server_port(req.server_port());
    s->set_conn_id(req.conn_id());
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
  auto fields = redis_.hgetall(key); // field="gateway_id:conn_id", value="ip:port"

  if (fields.empty()) {
    resp.set_found(false);
  } else {
    resp.set_found(true);
    for (const auto &kv : fields) {
      auto *s = resp.add_servers();
      // field 格式 "gateway_id:conn_id"
      auto pos = kv.first.rfind(':');
      if (pos != std::string::npos) {
        s->set_server_id(kv.first.substr(0, pos));            // gateway_id
        s->set_conn_id(std::stoull(kv.first.substr(pos + 1))); // conn_id
      } else {
        s->set_server_id(kv.first); // 兼容无 conn_id 的旧数据
      }
      // value 格式 "ip:port"
      auto pos2 = kv.second.rfind(':'); // IPv4 假设：ip 不含 ':'
      if (pos2 != std::string::npos) {
        s->set_server_ip(kv.second.substr(0, pos2));
        s->set_server_port(std::stoi(kv.second.substr(pos2 + 1)));
      }
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
  std::string field = req.server_id() + ":" + std::to_string(req.conn_id());
  // 只删本连接字段，其它连接路由保留
  bool ok = redis_.hdel(key, field);

  // 路由变更：通知各 IM 节点从缓存移除该连接
  im::RouteChangeEvent ev;
  ev.set_user_id(req.user_id());
  ev.set_online(false);
  ev.mutable_server()->set_server_id(req.server_id());
  ev.mutable_server()->set_conn_id(req.conn_id());
  std::string payload;
  ev.SerializeToString(&payload);
  redis_.publish("im:route:events", payload);
  resp.set_success(ok);
  std::string result;
  resp.SerializeToString(&result);
  return result;
}

void RouteServer::start() { rpc_server_.start(); }
void RouteServer::stop() { rpc_server_.stop(); }