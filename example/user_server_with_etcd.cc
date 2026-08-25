#include "Logger.h"
#include "rpc_server.h"
#include "service_manager.h"
#include "user.pb.h"
#include <csignal>
#include <string>

std::atomic<bool> running{true};

void signalHandler(int) { running = false; }

std::string handleLogin(const std::string &request_body) {
  LoginRequest req;
  if (!req.ParseFromString(request_body)) {
    LoginResponse resp;
    resp.set_message("parse request failed");
    std::string out;
    resp.SerializeToString(&out);
    return out;
  }

  LoginResponse resp;
  if (req.username() == "admin" && req.password() == "123456") {
    resp.set_success(true);
    resp.set_message("Welcome," + req.username() + "!");
  } else {
    resp.set_success(false);
    resp.set_message("Invalid username or password");
  }

  std::string out;
  resp.SerializeToString(&out);
  return out;
}

int main(int argc, char *argv[]) {
  Logger::instance().init(LogLevel::DEBUG, "server_etcd.log", true);

  //可以通过命令行参数修改: --server.port = 8001
  std::string ip = "0.0.0.0";
  int port = 8888;
  std::string serviceName = "UserService";
  std::string etcdEp = "http://127.0.0.1:2379";

  //简单的命令行参数解析
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--server.port=", 0) == 0) {
      port = std::stoi(arg.substr(14));
    } else if (arg.rfind("--etcd.endpoints=", 0) == 0) {
      etcdEp = arg.substr(17);
    } else if (arg.rfind("--service.name=", 0) == 0) {
      serviceName = arg.substr(15);
    }
  }
  RpcServer server(ip, port, 4);
  server.serviceManager().registerMethod(serviceName, "Login", handleLogin);

  //开启etcd注册
  server.enableRegistry(etcdEp, serviceName, ip, port, 30);

  LOG_INFO("RPC Server with etcd started on %s:%d", ip.c_str(), port);
  // 等待退出信号
  signal(SIGINT, signalHandler);
  signal(SIGTERM, signalHandler);

  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  LOG_INFO("Shutting down...");
  server.stop();
  return 0;
}
