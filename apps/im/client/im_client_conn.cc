#include "im_client_conn.h"
#include "rpc_protocol.h"
#include "uuid.h"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

ImClientConn::ImClientConn() = default;
ImClientConn::~ImClientConn() { close(); }

void ImClientConn::enableTls(const std::string &ca_path, bool insecure) {
  tls_ = true;              // 打开 TLS 开关
  tls_ca_ = ca_path;        // 记下 CA 证书路径
  tls_insecure_ = insecure; // 是否跳过证书校验
}

// 设 fd 为非阻塞（TLS 数据阶段配合 poll 使用）
static bool setNonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0)
    return false;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool ImClientConn::connect(const std::string &ip, int port) {
  // 先用局部 fd，连接成功后再发布到 sockfd_，
  // 避免连接尚未建立就被并发 call() 看到。
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return false;
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

  if (::connect(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) <
      0) {
    ::close(fd);
    return false;
  }

  // TLS：阻塞式握手（此刻还没起 readLoop，无并发，可以阻塞）
  if (tls_) {
    tls_ctx_.reset(SSL_CTX_new(TLS_client_method()));
    if (!tls_ctx_) {
      ::close(fd);
      return false;
    }
    if (tls_insecure_) {
      SSL_CTX_set_verify(tls_ctx_.get(), SSL_VERIFY_NONE, nullptr);
    } else {
      SSL_CTX_set_verify(tls_ctx_.get(), SSL_VERIFY_PEER, nullptr);
      if (SSL_CTX_load_verify_locations(tls_ctx_.get(), tls_ca_.c_str(),
                                        nullptr) != 1) {
        ::close(fd);
        return false;
      }
    }
    ssl_.reset(SSL_new(tls_ctx_.get()));
    if (!ssl_) {
      ::close(fd);
      return false;
    }
    SSL_set_fd(ssl_.get(), fd);
    if (!tls_insecure_) {
      // 主机名/IP 校验：ip 是 "127.0.0.1"，会匹配证书里的 IP SAN
      SSL_set1_host(ssl_.get(), ip.c_str());
    }
    if (SSL_connect(ssl_.get()) != 1) {
      ::close(fd);
      return false;
    }
    // 握手完成后数据阶段改非阻塞：让 ssl_mutex_ 只在每次 SSL_ 调用瞬间持有。
    SSL_set_mode(ssl_.get(), SSL_MODE_ENABLE_PARTIAL_WRITE |
                                 SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    if (!setNonblocking(fd)) {
      ::close(fd);
      return false;
    }
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = false; // 支持 close() 后重连
  }
  sockfd_.store(fd);
  running_.store(true);
  thread_ = std::thread(&ImClientConn::readLoop, this);
  return true;
}

// 等 fd 就绪；close() 通过 running_ 置位 + shutdown 打断。
bool ImClientConn::waitFd(int fd, short events) {
  while (running_.load()) {
    struct pollfd p {
      fd, events, 0
    };
    int r = ::poll(&p, 1, 50); // 50ms 超时，及时响应 running_ 置位
    if (r > 0)
      return true;
    if (r < 0) {
      if (errno == EINTR)
        continue;
      return false;
    }
    // r == 0：超时，继续循环检查 running_
  }
  return false;
}

void ImClientConn::close() {
  running_.store(false);
  int fd = sockfd_.load();
  // 先 shutdown 但不 close：既解除 readLoop 阻塞、中断在途 send，
  // 又保持 fd 号有效。若在这里就 close，readLoop 可能还阻塞在该 fd 上，
  // 该 fd 号会立刻被别处复用，收帧线程就读到别人的 fd 了。
  if (fd >= 0)
    ::shutdown(fd, SHUT_RDWR);
  if (thread_.joinable())
    thread_.join(); // 等 readLoop 真正退出，此后没人再碰 ssl_ 的读侧
  {
    // 再等在途的 sendAll 结束，才真正回收 fd 与 ssl_。
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (sockfd_.load() >= 0) {
      ssl_.reset(); // SSL_free（此刻无并发 SSL 读写）
      ::close(fd);
      sockfd_.store(-1);
    }
  }
}

// 前提：调用前必须已持有 send_mutex_。
bool ImClientConn::sendAll(const char *data, size_t n) {
  int fd = sockfd_.load();
  if (fd < 0)
    return false;

  if (!tls_) {
    size_t sent = 0;
    while (sent < n) {
      ssize_t r = ::send(fd, data + sent, n - sent, MSG_NOSIGNAL);
      if (r < 0) {
        if (errno == EINTR)
          continue;
        return false;
      }
      if (r == 0)
        return false;
      sent += static_cast<size_t>(r);
    }
    return true;
  }

  // TLS：非阻塞 SSL_write + poll。err 必须在锁内取（SSL_get_error 依赖
  // 上一条对同一 SSL* 的操作，不能在解锁后被别的线程覆盖）。
  size_t sent = 0;
  while (sent < n && running_.load()) {
    int r = 0, err = SSL_ERROR_NONE;
    {
      std::lock_guard<std::mutex> lk(ssl_mutex_);
      r = SSL_write(ssl_.get(), data + sent, static_cast<int>(n - sent));
      if (r <= 0)
        err = SSL_get_error(ssl_.get(), r);
    }
    if (r > 0) {
      sent += static_cast<size_t>(r);
      continue;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
      if (!waitFd(fd, POLLOUT))
        return false;
    } else if (err == SSL_ERROR_WANT_READ) {
      if (!waitFd(fd, POLLIN))
        return false;
    } else {
      return false;
    }
  }
  return sent == n;
}

bool ImClientConn::readFull(char *buf, size_t n) {
  int fd = sockfd_.load();
  if (fd < 0)
    return false;

  if (!tls_) {
    size_t got = 0;
    while (got < n) {
      ssize_t r = ::recv(fd, buf + got, n - got, 0);
      if (r < 0) {
        if (errno == EINTR)
          continue;
        return false;
      }
      if (r == 0)
        return false;
      got += static_cast<size_t>(r);
    }
    return true;
  }

  // TLS：非阻塞 SSL_read + poll
  size_t got = 0;
  while (got < n && running_.load()) {
    int r = 0, err = SSL_ERROR_NONE;
    {
      std::lock_guard<std::mutex> lk(ssl_mutex_);
      r = SSL_read(ssl_.get(), buf + got, static_cast<int>(n - got));
      if (r <= 0)
        err = SSL_get_error(ssl_.get(), r);
    }
    if (r > 0) {
      got += static_cast<size_t>(r);
      continue;
    }
    if (err == SSL_ERROR_WANT_READ) {
      if (!waitFd(fd, POLLIN))
        return false;
    } else if (err == SSL_ERROR_WANT_WRITE) {
      if (!waitFd(fd, POLLOUT))
        return false;
    } else {
      return false; // 含 SSL_ERROR_ZERO_RETURN：对端关闭
    }
  }
  return got == n;
}

void ImClientConn::readLoop() {
  while (running_.load()) {
    // 帧格式：[4 字节 BE 长度][payload]
    char hdr[kHeaderLen];
    if (!readFull(hdr, kHeaderLen))
      break;
    uint32_t len = readLenBE(hdr);
    if (len == 0 || len > kMaxMessageLen) // 长度 sanity check
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
  // 收帧结束（对端断开或 close()）：置位并唤醒所有等待者，
  // 否则在途的 call() 要白等满 timeout_ms 才返回。
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = true;
    cv_.notify_all();
  }
}

std::string ImClientConn::call(const std::string &service_name,
                               const std::string &method_name,
                               const std::string &request_body,
                               int timeout_ms) {
  if (sockfd_.load() < 0)
    return "";

  uint64_t seq = next_seq_.fetch_add(1);
  RpcMessage req = buildRequest(service_name, method_name, seq, request_body);
  // trace_id 客户端优先生成（网关兜底）；判空，避免无条件覆盖。
  if (req.header().trace_id().empty()) {
    req.mutable_header()->set_trace_id(generateUuid());
  }
  std::string wire = encodeMessage(req);
  {
    // 整帧原子发送。少了这把锁，两个线程的帧会在 socket 上交错，
    // 对端按长度前缀切帧就切歪了，整条连接的流报废。
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (!sendAll(wire.data(), wire.size()))
      return "";
  }

  std::unique_lock<std::mutex> lock(mutex_);
  bool ok = cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                         [&] { return closed_ || responses_.count(seq) != 0; });
  if (!ok) {
    responses_.erase(seq); // 超时：清掉可能晚到的响应，避免 map 无界增长
    return "";
  }
  // 先查 map 再看 closed_：响应已投递、紧接着 readLoop 才读到错误退出的情况下，
  // 调用方仍应拿到这个有效响应。
  auto it = responses_.find(seq);
  if (it == responses_.end())
    return ""; // 被 closed_ 唤醒且无响应
  std::string result = std::move(it->second);
  responses_.erase(it);
  return result;
}

void ImClientConn::setPushHandler(PushHandler h) {
  std::lock_guard<std::mutex> lock(mutex_);
  push_handler_ = std::move(h);
}