// example/user_client_with_etcd.cc
#include "Logger.h"
#include "lb_rpc_client.h"
#include "loadbalance/load_balancer.h"
#include "user.pb.h"
#include <iostream>

int main(int argc,char* argv[]) {
  Logger::instance().init(LogLevel::DEBUG, "client_etcd.log", true);

  std::string serviceName = "UserService";
  std::string etcdEp = "http://127.0.0.1:2379";

  //简单的命令行参数解析
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--etcd.endpoints=", 0) == 0) {
      etcdEp = arg.substr(17);
    } else if (arg.rfind("--service.name=", 0) == 0) {
      serviceName = arg.substr(15);
    }
  }
  //使用轮询负载均衡
  auto balancer = std::make_shared<RoundRobinBalancer>();
  LbRpcClient client(etcdEp, serviceName, balancer);

  LoginRequest req;
  req.set_username("admin");
  req.set_password("123456");

  std::string requestBody;
  req.SerializeToString(&requestBody);
  std::string responseBody;
  int32_t errorCode = 0;
  if (client.Call("Login", requestBody, responseBody, errorCode)) {
    LoginResponse resp;
    resp.ParseFromString(responseBody);
    std::cout << "Login: " << (resp.success() ? "SUCCESS" : "FAILED") << " — "
              << resp.message() << std::endl;
  } else {
    LOG_ERROR("RPC call failed, error_code=%d", errorCode);
  }

  return 0;
}