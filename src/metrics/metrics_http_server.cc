#include "metrics_http_server.h"
#include "Buffer.h"
#include "Connection.h"
#include "Logger.h"
#include "TcpServer.h"
#include "metrics_registry.h"
#include <cstdint>
#include <memory>
#include <string>

namespace {
constexpr size_t kMaxHttpHeaderLen = 8192; // 最大 header 长度（字节）
constexpr double kReadTimeoutSec = 10.0;   // 读超时：空闲即关
} // namespace

MetricsHttpServer::MetricsHttpServer(const std::string &bind_ip, uint16_t port)
    : bind_ip_(bind_ip), port_(port),
      server_(std::make_unique<TcpServer>(bind_ip, port, 1)) {
  server_->setonmessagecb(
      [this](spConnection conn, Buffer &buf) { onHttp(conn, buf); });
  server_->setIdleTimeout(kReadTimeoutSec);
}

MetricsHttpServer::~MetricsHttpServer() { stop(); }

void MetricsHttpServer::start() {
  LOG_INFO("MetricsHttpServer: serving /metrics on %s:%u", bind_ip_.c_str(),
           static_cast<unsigned>(port_));
  thread_ = std::thread([this] { server_->start(); });
}

void MetricsHttpServer::stop() {
  server_->stop();
  if (thread_.joinable())
    thread_.join();
}

void MetricsHttpServer::onHttp(std::shared_ptr<Connection> conn, Buffer &buf) {
  size_t n = buf.readableBytes();
  if (n > kMaxHttpHeaderLen) {
    // header 超长：畸形/攻击请求，直接关，绝不进入解析。
    conn->forceClose();
    return;
  }

  std::string data(buf.peek(), n);
  size_t head_end = data.find("\r\n\r\n");
  if (head_end == std::string::npos)
    return; // 头还没读完，继续等（空闲超时兜底关闭）

  // 只解析请求行，判断是否 GET /metrics（含 /metrics 与 /metrics?…）。
  size_t line_end = data.find("\r\n");
  std::string reqline = data.substr(0, line_end);
  bool is_metrics =
      (reqline == "GET /metrics") || (reqline.rfind("GET /metrics ", 0) == 0) ||
      (reqline.rfind("GET /metrics?", 0) == 0);

  std::string status;
  std::string body;
  if (is_metrics) {
    status = "200 OK";
    body = MetricsRegistry::instance().renderPrometheus();
  } else {
    status = "404 Not Found";
    body = "not found\n";
  }

  std::string resp;
  resp.reserve(body.size() + 128);
  resp += "HTTP/1.1 " + status + "\r\n";
  resp += "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n";
  resp += "Content-Length: " + std::to_string(body.size()) + "\r\n";
  resp += "Connection: close\r\n";
  resp += "\r\n";
  resp += body;

  buf.retrieveAll();
  conn->sendThenClose(resp.data(), resp.size());
}
