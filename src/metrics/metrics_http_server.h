#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

class TcpServer;
class Connection;
class Buffer;

// 极简 HTTP 服务器：只暴露 GET /metrics，输出 Prometheus 文本格式后关连接。
// - 默认只绑 127.0.0.1（仅内网暴露），可用 --metrics.bind 覆盖。
// - 读超时 + 最大 header 长度上限，畸形/慢速请求安全关闭，不崩。
class MetricsHttpServer {
public:
  MetricsHttpServer(const std::string &bind_ip, uint16_t port);
  ~MetricsHttpServer();

  void start(); // 在独立线程跑 mainloop，立即返回
  void stop();  // 停 mainloop 并 join

private:
  void onHttp(std::shared_ptr<Connection> conn, Buffer &buf);

  std::string bind_ip_;
  uint16_t port_;
  std::unique_ptr<TcpServer> server_;
  std::thread thread_;
};
