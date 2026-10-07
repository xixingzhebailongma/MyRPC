#include "Logger.h"
#include "deliver_server.h"
#include "metrics_http_server.h"
#include "span_exporter.h"
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
  Logger::instance().setServiceName("deliver");
  Logger::instance().init(LogLevel::DEBUG, "deliver_server.log", true,
                          drop_on_overflow, min_guaranteed);
  SpanExporter::instance().init("spans_deliver.jsonl");

  std::string server_id = "deliver";
  uint64_t worker_id = 0;
  std::string etcd_endpoints = "http://127.0.0.1:2379";
  std::string route_service = "RouteService";
  std::string redis_ip = "127.0.0.1";
  int redis_port = 6379;
  std::string consumer_name = "deliver-1";
  // /metrics HTTP 端点（默认只绑 127.0.0.1，仅内网暴露）
  std::string metrics_bind = "127.0.0.1";
  int metrics_port = 9094;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--server.id=", 0) == 0) {
      server_id = arg.substr(12);
    } else if (arg.rfind("--worker.id=", 0) == 0) {
      worker_id = std::stoull(arg.substr(12));
    } else if (arg.rfind("--etcd.endpoints=", 0) == 0) {
      etcd_endpoints = arg.substr(17);
    } else if (arg.rfind("--route.service=", 0) == 0) {
      route_service = arg.substr(16);
    } else if (arg.rfind("--redis.ip=", 0) == 0) {
      redis_ip = arg.substr(11);
    } else if (arg.rfind("--redis.port=", 0) == 0) {
      redis_port = std::stoi(arg.substr(13));
    } else if (arg.rfind("--consumer.name=", 0) == 0) {
      consumer_name = arg.substr(16);
    } else if (arg.rfind("--metrics.port=", 0) == 0) {
      metrics_port = std::stoi(arg.substr(15));
    } else if (arg.rfind("--metrics.bind=", 0) == 0) {
      metrics_bind = arg.substr(15);
    }
  }

  DeliverServer server(server_id, worker_id, route_service, etcd_endpoints,
                       redis_ip, redis_port, consumer_name);
  if (!server.init()) {
    LOG_ERROR("deliver_server: init failed");
    return 1;
  }
  // /metrics HTTP 端点（默认只绑 127.0.0.1，仅内网暴露）
  MetricsHttpServer metrics(metrics_bind, static_cast<uint16_t>(metrics_port));
  metrics.start();

  server.start();
  LOG_INFO("DeliverServer started (consumer=%s)", consumer_name.c_str());

  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);
  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  LOG_INFO("Shutting down...");
  server.stop();
  return 0;
}