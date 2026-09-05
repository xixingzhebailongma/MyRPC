#include "im_client_conn.h"
#include "rpc_protocol.h"
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

ImClientConn::ImClientConn() = default;
ImClientConn::~ImClientConn() { close(); }

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
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed_ = false; // 支持 close() 后重连
  }
  sockfd_.store(fd);
  running_.store(true);
  thread_ = std::thread(&ImClientConn::readLoop, this);
  return true;
}

void ImClientConn::close() {
  running_.store(false);
  int fd = sockfd_.load();
  // 先 shutdown 但不 close：既解除 readLoop 阻塞的 recv、中断在途的 send，
  // 又保持 fd 号有效。若在这里就 close，readLoop 可能还阻塞在 ::recv(fd) 里，
  // 该 fd 号会立刻被别处 socket()/open() 复用，收帧线程就读到别人的 fd 了。
  if (fd >= 0)
    ::shutdown(fd, SHUT_RDWR);
  if (thread_.joinable())
    thread_.join(); // 等 readLoop 真正退出，此后没人再碰这个 fd
  {
    // 再等在途的 sendAll 结束，才真正回收 fd。
    std::lock_guard<std::mutex> lock(send_mutex_);
    if (sockfd_.load() >= 0) {
      ::close(fd);
      sockfd_.store(-1);
    }
  }
}

// 前提：调用前必须已持有 send_mutex_。
bool ImClientConn::sendAll(const char *data, size_t n) {
  // 快照 fd 是安全的：close() 里真正的 ::close 也在 send_mutex_ 保护下，
  // 不会在本函数执行期间发生，所以快照到的 fd 号不会变成被复用的新 fd。
  int fd = sockfd_.load();
  if (fd < 0)
    return false;
  size_t sent = 0;
  while (sent < n) {
    ssize_t r = ::send(fd, data + sent, n - sent, MSG_NOSIGNAL); // 不要 SIGPIPE
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

bool ImClientConn::readFull(char *buf, size_t n) {
  int fd = sockfd_.load();
  if (fd < 0)
    return false;
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

void ImClientConn::readLoop() {
  while (running_.load()) {
    // 帧格式：[4 字节 LE 长度][payload]
    uint32_t len = 0;
    if (!readFull(reinterpret_cast<char *>(&len), 4))
      break;
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