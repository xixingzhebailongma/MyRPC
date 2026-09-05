#include "Logger.h"
#include "gateway_server.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <string>
#include <thread>

std::atomic<bool> running{true};
void signalHandler(int) { running = false; }

int main(int argc, char *argv[]) {
  Logger::instance().init(LogLevel::DEBUG, "gateway_server.log", true);

  std::string client_ip = "0.0.0.0";
  int client_port = 9000; // 客户端连这个端口
  std::string rpc_ip = "0.0.0.0";
  int rpc_port = 9100; // IM 节点回推到这
  std::string gateway_id = "GW1";
  std::string etcd_endpoints = "http://127.0.0.1:2379";
  std::string im_service = "ImService";
  std::string auth_service = "AuthService";
  std::string shared_secret = "";

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
    }
  }

  GatewayServer gw(client_ip, static_cast<uint16_t>(client_port), rpc_ip,
                   static_cast<uint16_t>(rpc_port), gateway_id, etcd_endpoints,
                   im_service, auth_service, shared_secret);
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
