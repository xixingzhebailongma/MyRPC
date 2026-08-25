#include "Logger.h"
#include "route_server.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <string>
#include <thread>

std::atomic<bool>running{true};

void signalHandler(int){running = false;}

int main(int argc,char* argv[]){
    //1初始化日志
    Logger::instance().init(LogLevel::DEBUG,"route_server.log",true);

    //2.默认值
    std::string ip      = "0.0.0.0";
    int port            = 8889;
    std::string redis_ip = "127.0.0.1";
    int redis_port = 6379;
    std::string etcd_endpoints = "http://127.0.0.1:2379";
    std::string service_name = "RouteService";

    // 3. 解析命令行参数（简单字符串匹配）
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
         }
     }

     // 4. 创建 RouteServer（构造函数内部会：连接 Redis、注册 3 个 RPC 方法、
     //    注册到 etcd）

     RouteServer server(ip,port,redis_ip,redis_port,etcd_endpoints,service_name);

     //启动
     server.start();
     LOG_INFO("Route Server started on %s:%d", ip.c_str(), port);

     // 6. 等 SIGINT/SIGTERM
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