#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <openssl/ssl.h>
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
  bool connected() const { return sockfd_.load() >= 0; }

  // 启用 TLS：ca_path 为校验服务端证书用的信任锚（自签证书即证书本身）。
  // insecure=true 跳过校验（仅加密不认证）。需在 connect() 前调用。
  void enableTls(const std::string &ca_path, bool insecure);

  // 同步 RPC 调用：返回响应 body（失败/超时返回空字符串）。
  std::string call(const std::string &service_name,
                   const std::string &method_name,
                   const std::string &request_body, int timeout_ms = 3000);

  // 设置服务端推送回调（在收帧线程里回调，注意线程安全）。
  void setPushHandler(PushHandler h);

private:
  void readLoop();
  // 前提：调用前必须已持有 send_mutex_。
  bool sendAll(const char *data, size_t n);
  bool readFull(char *buf, size_t n);
  // 等 fd 可读/可写（TLS 非阻塞路径用），被 running_/shutdown 打断返回 false。
  bool waitFd(int fd, short events);

  std::atomic<int> sockfd_{-1}; // 与 close()/readLoop 并发访问，必须原子
  std::atomic<uint64_t> next_seq_{0};
  std::atomic<bool> running_{false};
  std::thread thread_;

  // TLS
  std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> tls_ctx_{nullptr,
                                                             SSL_CTX_free};
  std::unique_ptr<SSL, decltype(&SSL_free)> ssl_{nullptr, SSL_free};
  bool tls_ = false;
  bool tls_insecure_ = false;
  std::string tls_ca_;
  // 串行化每次 SSL_read/SSL_write。非阻塞 SSL 调用 + 这把锁只在调用瞬间持有，
  // poll 等待时释放，才能既避免并发读写同一 SSL*，又不死锁。
  std::mutex ssl_mutex_;

  // 串行化整帧发送，防止并发 call() 的帧在 socket 上交错。
  // 必须与 mutex_ 分开：readLoop 投递响应要拿 mutex_，若发送方持 mutex_
  // 阻塞在 ::send，readLoop 就会停止收帧，进而和对端形成死锁。
  std::mutex send_mutex_;

  std::mutex mutex_;
  std::condition_variable cv_;
  std::map<uint64_t, std::string> responses_; // seq -> 响应 body
  bool closed_ = false; // 连接已断开，由 mutex_ 保护
  PushHandler push_handler_;
};