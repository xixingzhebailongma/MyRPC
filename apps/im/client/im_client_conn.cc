#include "im_client_conn.h"
#include "rpc_protocol.h"
#include <arpa/inet.h>
#include <chrono>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

ImClientConn::ImClientConn() = default;
ImClientConn::~ImClientConn() { close(); }

bool ImClientConn::connect(const std::string &ip, int port) {
  sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd_ < 0)
    return false;
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

  if (::connect(sockfd_, reinterpret_cast<struct sockaddr *>(&addr),
                sizeof(addr)) < 0) {
    ::close(sockfd_);
    sockfd_ = -1;
    return false;
  }
  running_.store(true);
  thread_ = std::thread(&ImClientConn::readLoop, this);
  return true;
}

void ImClientConn::close() {
  running_.store(false);
  if (sockfd_ >= 0) {
    ::shutdown(sockfd_, SHUT_RDWR); // 解除 readLoop 的阻塞
    ::close(sockfd_);
    sockfd_ = -1;
  }
  if (thread_.joinable())
    thread_.join();
}

bool ImClientConn::sendAll(const char *data, size_t n) {
  size_t sent = 0;
  while (sent < n) {
    ssize_t r = ::send(sockfd_, data + sent, n - sent, 0);
    if (r <= 0)
      return false;
    sent += static_cast<size_t>(r);
  }
  return true;
}

bool ImClientConn::readFull(char *buf, size_t n) {
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::recv(sockfd_, buf + got, n - got, 0);
    if (r <= 0)
      return false;
    got += static_cast<size_t>(r);
  }
  return true;
}

void ImClientConn::readLoop() {
  while (running_.load()) {
    // 帧格式：[4 字节 LE 长度][payload]
    uint32_t len = 0;
    if (!readFull(reinterpret_cast<char *>(&len), 4))
      break;
    if (len == 0 || len > (64u << 20)) // 长度 sanity check
      break;

    std::string payload(len, '\0');
    if (!readFull(&payload[0], len))
      break;

    RpcMessage msg;
    if (msg.ParseFromString(payload) && msg.has_header()) {
      // RPC 响应：按 sequence_id 匹配
      std::lock_guard<std::mutex> lock(mutex_);
      responses_[msg.header().sequence_id()] = msg.body();
      cv_.notify_all();
    } else {
      // 服务端推送
      PushHandler h;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        h = push_handler_;
      }
      if (h)
        h(payload);
    }
  }
}

std::string ImClientConn::call(const std::string &service_name,
                               const std::string &method_name,
                               const std::string &request_body,
                               int timeout_ms) {
  if (sockfd_ < 0)
    return "";

  uint64_t seq = next_seq_.fetch_add(1);
  RpcMessage req = buildRequest(service_name, method_name, seq, request_body);
  std::string wire = encodeMessage(req);
  if (!sendAll(wire.data(), wire.size()))
    return "";

  std::unique_lock<std::mutex> lock(mutex_);
  bool ok = cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                         [&] { return responses_.count(seq) != 0; });
  if (!ok)
    return ""; // 超时
  std::string result = responses_[seq];
  responses_.erase(seq);
  return result;
}

void ImClientConn::setPushHandler(PushHandler h) {
  std::lock_guard<std::mutex> lock(mutex_);
  push_handler_ = std::move(h);
}