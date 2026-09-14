#pragma once
#include "redis_client.h"
#include "Connection.h"
#include "rpc_server.h"
#include <memory>
#include <string>
/*key    = "im:route:" + user_id            // 每个用户一个 hash
  field  = "server_id:conn_id"              // 如 "GW1:12345"
  value  = "server_ip:server_port"          // 如 "10.0.0.1:9000"*/
class RouteServer {
public:
  RouteServer(const std::string &ip, uint16_t port, const std::string &redis_ip,
              int redis_port, const std::string &etcd_endpoints,
              const std::string &service_name, int thread_num = 4);

  void start();
  void stop();

private:
  // 3个 conn-aware handler（签名: std::string(spConnection, const
  // std::string&)）
  std::string handleRouteRegister(spConnection conn,
                                  const std::string &request_body);
  std::string handleRouteQuery(spConnection conn,
                               const std::string &request_body);
  std::string handleRouteUnregister(spConnection conn,
                                    const std::string &request_body);
  std::string handleRouteUnregisterByConn(spConnection conn,
                                          const std::string &request_body);

  RpcServer rpc_server_;
  RedisClient redis_;
};