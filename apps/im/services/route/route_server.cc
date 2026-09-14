#include "route_server.h"
#include "EventLoop.h"
#include "im.pb.h"
#include "mq_constants.h"
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
  rpc_server_.serviceManager().registerMethod(
      service_name, "RouteUnregisterByConn",
      [this](spConnection conn, const std::string &body) {
        return handleRouteUnregisterByConn(conn, body);
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

  // field="gateway_id:conn_id", value="ip:port:session_id"：连接粒度，天然去重
  std::string field = req.server_id() + ":" + std::to_string(req.conn_id());
  std::string value = req.server_ip() + ":" + std::to_string(req.server_port()) +
                      ":" + req.session_id();
  // 反查索引 key：conn -> user_id，断线清理时据此反查该删哪个 user 的哪条路由。
  // 与 route field 原子写入，避免「路由写了、索引没写」导致断线找不到 user 而泄漏。
  std::string conn_key =
      "im:conn:" + req.server_id() + ":" + std::to_string(req.conn_id());
  bool ok = redis_.eval(
      "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]) "
      "redis.call('SET', KEYS[2], ARGV[3]) "
      "return 1",
      {key, conn_key}, {field, value, req.user_id()});

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
    s->set_session_id(req.session_id());
    std::string payload;
    ev.SerializeToString(&payload);
    redis_.xaddTrimmed(immq::kRouteEventsStream, immq::kBodyField, payload,
                       immq::kEventsMaxLen);
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
      // value 格式 "ip:port[:session_id]"（session_id 可为空，兼容旧数据）
      auto first = kv.second.find(':');
      if (first != std::string::npos) {
        s->set_server_ip(kv.second.substr(0, first));
        auto second = kv.second.find(':', first + 1);
        if (second != std::string::npos) {
          s->set_server_port(
              std::stoi(kv.second.substr(first + 1, second - first - 1)));
          s->set_session_id(kv.second.substr(second + 1));
        } else {
          s->set_server_port(std::stoi(kv.second.substr(first + 1)));
        }
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
  std::string conn_key =
      "im:conn:" + req.server_id() + ":" + std::to_string(req.conn_id());
  // 只删本连接字段，其它连接路由保留；同步删反查索引
  bool ok = redis_.eval(
      "redis.call('HDEL', KEYS[1], ARGV[1]) "
      "redis.call('DEL', KEYS[2]) "
      "return 1",
      {key, conn_key}, {field});

  // 路由变更：通知各 IM 节点从缓存移除该连接
  im::RouteChangeEvent ev;
  ev.set_user_id(req.user_id());
  ev.set_online(false);
  ev.mutable_server()->set_server_id(req.server_id());
  ev.mutable_server()->set_conn_id(req.conn_id());
  std::string payload;
  ev.SerializeToString(&payload);
  redis_.xaddTrimmed(immq::kRouteEventsStream, immq::kBodyField, payload,
                     immq::kEventsMaxLen);
  resp.set_success(ok);
  std::string result;
  resp.SerializeToString(&result);
  return result;
}

std::string
RouteServer::handleRouteUnregisterByConn(spConnection conn,
                                         const std::string &request_body) {
  im::RouteUnregisterByConnRequest req;
  im::RouteUnregisterByConnResponse resp;
  if (!req.ParseFromString(request_body)) {
    resp.set_success(false);
    return resp.SerializeAsString();
  }
  std::string field = req.gateway_id() + ":" + std::to_string(req.conn_id());
  std::string conn_key =
      "im:conn:" + req.gateway_id() + ":" + std::to_string(req.conn_id());
  // 先反查 user_id（广播离线事件用）
  std::string user_id = redis_.get(conn_key);
  // 原子：反查 user_id → HDEL 该 user 的 route field → DEL 反查索引。
  // 未认证连接没有反查索引（uid 为空），脚本自然 no-op，天然幂等。
  bool ok = redis_.eval(
      "local uid = redis.call('GET', KEYS[1]) "
      "if uid and uid ~= '' then "
      "  redis.call('HDEL', 'im:route:' .. uid, ARGV[1]) "
      "  redis.call('DEL', KEYS[1]) "
      "end "
      "return 1",
      {conn_key}, {field});
  // 广播离线事件：失效各 IM 节点本地 route 缓存（否则在线状态会 60s 内陈旧）
  if (ok && !user_id.empty()) {
    im::RouteChangeEvent ev;
    ev.set_user_id(user_id);
    ev.set_online(false);
    ev.mutable_server()->set_server_id(req.gateway_id());
    ev.mutable_server()->set_conn_id(req.conn_id());
    std::string payload;
    ev.SerializeToString(&payload);
    redis_.xaddTrimmed(immq::kRouteEventsStream, immq::kBodyField, payload,
                       immq::kEventsMaxLen);
  }
  resp.set_success(ok);
  return resp.SerializeAsString();
}

void RouteServer::start() { rpc_server_.start(); }
void RouteServer::stop() { rpc_server_.stop(); }