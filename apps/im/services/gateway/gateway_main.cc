#include "Logger.h"
#include "gateway_server.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>   // std::getenv
#include <cstdint>
#include <string>
#include <thread>

std::atomic<bool> running{true};
void signalHandler(int) { running = false; }

int main(int argc, char *argv[]) {
  // SSL_write 无法带 MSG_NOSIGNAL：TLS 客户端断开时，服务端向已断连接写会触发
  // SIGPIPE，默认动作是终止进程（导致 acceptor 随进程一起死掉，后续 connect
  // ECONNREFUSED）。必须忽略，否则「一个 TLS 客户端断开」会打垮整个 gateway。
  signal(SIGPIPE, SIG_IGN);

  // 环境变量 MYRPC_LOG_DROP_ON_OVERFLOW：未设置=丢弃（默认）；设为 "0"=阻塞（不丢日志）
  bool drop_on_overflow = true;
  if (const char *env = std::getenv("MYRPC_LOG_DROP_ON_OVERFLOW"))
    drop_on_overflow = (std::string(env) != "0");
  Logger::instance().init(LogLevel::DEBUG, "gateway_server.log", true,
                          drop_on_overflow);

  std::string client_ip = "0.0.0.0";
  int client_port = 9000; // 客户端连这个端口
  std::string rpc_ip = "0.0.0.0";
  int rpc_port = 9100; // IM 节点回推到这
  std::string gateway_id = "GW1";
  std::string etcd_endpoints = "http://127.0.0.1:2379";
  std::string im_service = "ImService";
  std::string auth_service = "AuthService";
  std::string shared_secret = "";
  std::string tls_cert = "";
  std::string tls_key = "";

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--client.ip=", 0) == 0) {
      client_ip = arg.substr(12);
    } else if (arg.rfind("--client.port=", 0) == 0) {
      client_port = std::stoi(arg.substr(14));
    } else if (arg.rfind("--rpc.ip=", 0) == 0) {
      rpc_ip = arg.substr(9);
    } else if (arg.rfind("--rpc.port=", 0) == 0) {
      rpc_port = std::stoi(arg.substr(11));
    } else if (arg.rfind("--gateway.id=", 0) == 0) {
      gateway_id = arg.substr(13);
    } else if (arg.rfind("--etcd.endpoints=", 0) == 0) {
      etcd_endpoints = arg.substr(17);
    } else if (arg.rfind("--im.service=", 0) == 0) {
      im_service = arg.substr(13);
    } else if (arg.rfind("--auth.service=", 0) == 0) {
      auth_service = arg.substr(15);
    } else if (arg.rfind("--shared.secret=", 0) == 0) {
      shared_secret = arg.substr(16);
    } else if (arg.rfind("--tls.cert=", 0) == 0) {
      tls_cert = arg.substr(11);
    } else if (arg.rfind("--tls.key=", 0) == 0) {
      tls_key = arg.substr(10);
    }
  }

  GatewayServer gw(client_ip, static_cast<uint16_t>(client_port), rpc_ip,
                   static_cast<uint16_t>(rpc_port), gateway_id, etcd_endpoints,
                   im_service, auth_service, shared_secret);
  // 两个都给才启用 TLS（enableTls 里也会对证书/私钥做校验，失败回退明文）
  if (!tls_cert.empty() && !tls_key.empty()) {
    gw.enableClientTls(tls_cert, tls_key);
  }
  LOG_INFO("Gateway %s: client=%s:%d rpc=%s:%d", gateway_id.c_str(),
           client_ip.c_str(), client_port, rpc_ip.c_str(), rpc_port);

  gw.start();
  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);
  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  LOG_INFO("Shutting down...");
  gw.stop();
  return 0;
}
