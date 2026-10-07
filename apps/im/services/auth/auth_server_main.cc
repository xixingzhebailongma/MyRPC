#include "Logger.h"
#include "auth_server.h"
#include "metrics_http_server.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>   // std::getenv
#include <string>
#include <thread>

std::atomic<bool> running{true};

void signalHandler(int) { running = false; }

int main(int argc, char *argv[]) {
  // 对端断开时向已断 socket 写会触发 SIGPIPE，默认终止进程；忽略它，避免单个断开连接打垮整个服务进程。
  signal(SIGPIPE, SIG_IGN);

  // 环境变量 MYRPC_LOG_DROP_ON_OVERFLOW：未设置=丢弃（默认）；设为 "0"=阻塞（不丢日志）
  bool drop_on_overflow = true;
  if (const char *env = std::getenv("MYRPC_LOG_DROP_ON_OVERFLOW"))
    drop_on_overflow = (std::string(env) != "0");
  // 环境变量 MYRPC_LOG_MIN_GUARANTEED_LEVEL：>= 此等级的日志永不丢弃（默认 WARN）
  LogLevel min_guaranteed = LogLevel::WARN;
  if (const char *env = std::getenv("MYRPC_LOG_MIN_GUARANTEED_LEVEL"))
    min_guaranteed = Logger::levelFromString(env);
  Logger::instance().init(LogLevel::DEBUG, "auth_server.log", true,
                          drop_on_overflow, min_guaranteed);

  // 默认值
  std::string ip = "0.0.0.0";
  int port = 9101;
  std::string redis_ip = "127.0.0.1";
  int redis_port = 6379;
  std::string etcd_endpoints = "http://127.0.0.1:2379";
  std::string service_name = "AuthService";
  std::string mysql_host = "127.0.0.1";
  int mysql_port = 3306;
  std::string mysql_user = "root";
  std::string mysql_password = "";
  std::string mysql_db = "myrpc_im";
  int mysql_pool_size = 4;
  // /metrics HTTP 端点（默认只绑 127.0.0.1，仅内网暴露）
  std::string metrics_bind = "127.0.0.1";
  int metrics_port = 9091;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--server.ip=", 0) == 0) {
      ip = arg.substr(12);
    } else if (arg.rfind("--server.port=", 0) == 0) {
      port = std::stoi(arg.substr(14));
    } else if (arg.rfind("--redis.ip=", 0) == 0) {
      redis_ip = arg.substr(11);
    } else if (arg.rfind("--redis.port=", 0) == 0) {
      redis_port = std::stoi(arg.substr(13));
    } else if (arg.rfind("--etcd.endpoints=", 0) == 0) {
      etcd_endpoints = arg.substr(17);
    } else if (arg.rfind("--service.name=", 0) == 0) {
      service_name = arg.substr(15);
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
    } else if (arg.rfind("--metrics.port=", 0) == 0) {
      metrics_port = std::stoi(arg.substr(15));
    } else if (arg.rfind("--metrics.bind=", 0) == 0) {
      metrics_bind = arg.substr(15);
    }
  }

  DbConfig db_cfg;
  db_cfg.host = mysql_host;
  db_cfg.port = mysql_port;
  db_cfg.user = mysql_user;
  db_cfg.passwd = mysql_password;
  db_cfg.db = mysql_db;
  db_cfg.pool_size = mysql_pool_size;

  AuthServer server(ip, port, redis_ip, redis_port, etcd_endpoints,
                    service_name, db_cfg);

  // /metrics HTTP 端点（默认只绑 127.0.0.1，仅内网暴露）
  MetricsHttpServer metrics(metrics_bind, static_cast<uint16_t>(metrics_port));
  metrics.start();

  server.start();
  LOG_INFO("Auth Server started on %s:%d", ip.c_str(), port);

  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);
  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  LOG_INFO("Shutting down...");
  server.stop();
  return 0;
}