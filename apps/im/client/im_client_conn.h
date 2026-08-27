#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>

// 单连接 IM 测试客户端底层：一条 TCP，后台线程收帧。
// 按 "RpcMessage.has_header()" 区分 RPC 响应与服务端推送。
// 不依赖 im.pb.h / auth.pb.h —— 推送回调收到的是服务端推帧的原始 payload。
class ImClientConn {
public:
  using PushHandler = std::function<void(const std::string &payload)>;

  ImClientConn();
  ~ImClientConn();

  ImClientConn(const ImClientConn &) = delete;
  ImClientConn &operator=(const ImClientConn &) = delete;

  bool connect(const std::string &ip, int port);
  void close();
  bool connected() const { return sockfd_ >= 0; }

  // 同步 RPC 调用：返回响应 body（失败/超时返回空字符串）。
  std::string call(const std::string &service_name,
                   const std::string &method_name,
                   const std::string &request_body, int timeout_ms = 3000);

  // 设置服务端推送回调（在收帧线程里回调，注意线程安全）。
  void setPushHandler(PushHandler h);

private:
  void readLoop();
  bool sendAll(const char *data, size_t n);
  bool readFull(char *buf, size_t n);

  int sockfd_ = -1;
  std::atomic<uint64_t> next_seq_{0};
  std::atomic<bool> running_{false};
  std::thread thread_;

  std::mutex mutex_;
  std::condition_variable cv_;
  std::map<uint64_t, std::string> responses_; // seq -> 响应 body
  PushHandler push_handler_;
};