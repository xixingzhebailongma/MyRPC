#include "Logger.h"
#include "im_server.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <string>
#include <thread>

std::atomic<bool> running{true};

void signalHandler(int) { running = false; }

int main(int argc, char *argv[]) {
  Logger::instance().init(LogLevel::DEBUG, "im_server.log", true);
  // 2. 默认值
  std::string ip = "0.0.0.0";
  int port = 9001;
  std::string server_id = "IM1";
  std::string etcd_endpoints = "http://127.0.0.1:2379";
  std::string route_service = "RouteService";
  std::string auth_service = "AuthService";
  bool auth_enabled = true;
  //初始化 Redis(消息持久化)
  std::string redis_ip = "127.0.0.1";
  int redis_port = 6379;
  std::string mysql_host = "127.0.0.1";
  int mysql_port = 3306;
  std::string mysql_user = "root";
  std::string mysql_password = "";
  std::string mysql_db = "myrpc_im";
  int mysql_pool_size = 4;
  // 3. 解析命令行参数（简单字符串匹配）
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--server.ip=", 0) == 0) {
      ip = arg.substr(12);
    } else if (arg.rfind("--server.port=", 0) == 0) {
      port = std::stoi(arg.substr(14));
    } else if (arg.rfind("--server.id=", 0) == 0) {
      server_id = arg.substr(12);
    } else if (arg.rfind("--etcd.endpoints=", 0) == 0) {
      etcd_endpoints = arg.substr(17);
    } else if (arg.rfind("--route.service=", 0) == 0) {
      route_service = arg.substr(16);
    } else if (arg.rfind("--redis.ip=", 0) == 0) {
      redis_ip = arg.substr(11);
    } else if (arg.rfind("--redis.port=", 0) == 0) {
      redis_port = std::stoi(arg.substr(13));
    } else if (arg.rfind("--mysql.host=", 0) == 0) {
      mysql_host = arg.substr(13);
    } else if (arg.rfind("--mysql.port=", 0) == 0) {
      mysql_port = std::stoi(arg.substr(13));
    } else if (arg.rfind("--mysql.user=", 0) == 0) {
      mysql_user = arg.substr(13);
    } else if (arg.rfind("--mysql.password=", 0) == 0) {
      mysql_password = arg.substr(17);
    } else if (arg.rfind("--mysql.db=", 0) == 0) {
      mysql_db = arg.substr(11);
    } else if (arg.rfind("--mysql.pool.size=", 0) == 0) {
      mysql_pool_size = std::stoi(arg.substr(18));
    } else if (arg.rfind("--auth.service=", 0) == 0) {
      auth_service = arg.substr(15);
    } else if (arg.rfind("--auth.enabled=", 0) == 0) {
      std::string v = arg.substr(15);
      auth_enabled = (v == "true" || v == "1");
    }
  }
  DbConfig db_cfg;
  db_cfg.host = mysql_host;
  db_cfg.port = mysql_port;
  db_cfg.user = mysql_user;
  db_cfg.passwd = mysql_password;
  db_cfg.db = mysql_db;
  db_cfg.pool_size = mysql_pool_size;
  // 4.创建ImServer
  ImServer server(ip, port, server_id, route_service, etcd_endpoints, redis_ip,
                  redis_port, db_cfg, auth_service, auth_enabled);

  LOG_INFO("Redis connected at %s:%d", redis_ip.c_str(), redis_port);
  //启动
  server.start();
  LOG_INFO("IM Server started on %s:%d", ip.c_str(), port);
  // 6. 等 SIGINT / SIGTERM
  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);
  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  // 7. 优雅关闭
  LOG_INFO("Shutting down...");
  server.stop();
  return 0;
}
