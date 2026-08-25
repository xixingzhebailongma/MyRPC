#include "rpc_channel.h"
#include "Logger.h"
#include "rpc_header.pb.h"
#include "rpc_protocol.h"
#include <arpa/inet.h>
#include <asm-generic/errno-base.h>
#include <asm-generic/errno.h>
#include <asm-generic/socket.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

RpcChannel::RpcChannel(const std::string &server_ip, uint16_t server_port,
                       int timeout_ms)
    : server_ip_(server_ip), server_port_(server_port), sockfd_(-1),
      timeout_ms_(timeout_ms), next_seq_id_(0) {}

RpcChannel::~RpcChannel() { close(); }

bool RpcChannel::connect() {
  if (sockfd_ >= 0)
    return true;

  sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd_ < 0) {
    LOG_ERROR("RpcChannel: socket() failed");
    return false;
  }

  // 1. 设为非阻塞，配合 poll 实现 connect 超时
  int flags = fcntl(sockfd_, F_GETFL, 0);
  fcntl(sockfd_, F_SETFL, flags | O_NONBLOCK);

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(server_port_);
  inet_pton(AF_INET, server_ip_.c_str(), &addr.sin_addr);

  // 2. 非阻塞 connect：立即返回，EINPROGRESS 表示"连接进行中"
  int ret = ::connect(sockfd_, (struct sockaddr *)&addr, sizeof(addr));
  if (ret < 0 && errno != EINPROGRESS) {
    LOG_ERROR("RpcChannel: connect() to %s:%d failed", server_ip_.c_str(),
              server_port_);
    ::close(sockfd_);
    sockfd_ = -1;
    return false;
  }

  // 3.用 poll 等待连接建立（可写），超时则放弃
  if (ret < 0) { //说明上面是EINPROGRESS
    struct pollfd pfd;
    pfd.fd = sockfd_;
    pfd.events = POLLOUT;
    pfd.revents = 0;
    int pr = ::poll(&pfd, 1, timeout_ms_);
    if (pr <= 0) { // 0=超时，<0=出错
      LOG_ERROR("RpcChannel: connect() to %s:%d timeout", server_ip_.c_str(),
                server_port_);
      ::close(sockfd_);
      sockfd_ = -1;
      return false;
    }
    // 4. 检查套接字是否真的连接成功（poll 可写不代表一定成功）
    int so_error = 0;
    socklen_t len = sizeof(so_error);
    getsockopt(sockfd_, SOL_SOCKET, SO_ERROR, &so_error, &len);
    if (so_error != 0) {
      LOG_ERROR("RpcChannel: connect() to %s:%d failed: %s", server_ip_.c_str(),
                server_port_, strerror(so_error));
      ::close(sockfd_);
      sockfd_ = -1;
      return false;
    }
  }
  // 5. 恢复阻塞模式，改用 SO_RCVTIMEO/SO_SNDTIMEO 控制后续收发超时
  fcntl(sockfd_, F_SETFL, flags);

  // 6. 设置收发超时（timeout_ms 毫秒）
  struct timeval tv;
  tv.tv_sec = timeout_ms_ / 1000;
  tv.tv_usec = (timeout_ms_ % 1000) * 1000;
  setsockopt(sockfd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  setsockopt(sockfd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

  LOG_INFO("RpcChannel: connected to %s:%d", server_ip_.c_str(), server_port_);
  return true;
}

void RpcChannel::close() {
  std::lock_guard<std::mutex> lock(call_mutex_);
  closeLocked();
}
void RpcChannel::closeLocked() {
  if (sockfd_ >= 0) {
    ::close(sockfd_);
    sockfd_ = -1;
  }
}
bool RpcChannel::sendFrame(const std::string &data) {
  // data 已是 encodeMessage 产出的 [4字节长度][proto]
  // 帧，这里只负责把字节发完， 不要再叠加长度前缀（否则会与服务器 pickmessage
  // 只剥一层的约定不一致）。
  size_t total_sent = 0;
  while (total_sent < data.size()) {
    ssize_t n =
        ::send(sockfd_, data.data() + total_sent, data.size() - total_sent, 0);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        LOG_ERROR("RpcChannel: send() timeout");
      else
        LOG_ERROR("RpcChannel: send() failed, errno=%d", errno);
      return false;
    }
    total_sent += n;
  }
  return true;
}

bool RpcChannel::recvFrame(std::string &data) {
  //先读4字节长度
  uint32_t len = 0;
  size_t total_read = 0;
  char *buf = reinterpret_cast<char *>(&len);

  while (total_read < 4) {
    ssize_t n = ::recv(sockfd_, buf + total_read, 4 - total_read, 0);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        LOG_ERROR("RpcChannel: recv() header timeout");
      else
        LOG_ERROR("RpcChannel: recv() header failed, errno=%d", errno);
      return false;
    }
    if (n <= 0) {
      LOG_ERROR("RpcChannel: recv() header failed, n=%ld", n);
      return false;
    }
    total_read += n;
  }
  //再读len字节数据
  data.resize(len);
  total_read = 0;
  while (total_read < len) {
    ssize_t n = ::recv(sockfd_, &data[total_read], len - total_read, 0);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        LOG_ERROR("RpcChannel: recv() body timeout");
      else
        LOG_ERROR("RpcChannel: recv() body failed, errno=%d", errno);
      return false;
    }
    if (n <= 0) {
      LOG_ERROR("RpcChannel: recv() body failed, n=%ld", n);
      return false;
    }
    total_read += n;
  }
  return true;
}

bool RpcChannel::Call(const std::string &service_name,
                      const std::string &method_name,
                      const std::string &request_body,
                      std::string &response_body, int32_t &error_code) {
  std::lock_guard<std::mutex> lock(call_mutex_);
  //懒连接：首次调用时自动连接
  if (sockfd_ < 0 && !connect()) {
    return false;
  }

  //构造请求
  uint64_t seq = next_seq_id_.fetch_add(1);
  RpcMessage request =
      buildRequest(service_name, method_name, seq, request_body);
  std::string wire_data = encodeMessage(request);

  //发送
  if (!sendFrame(wire_data)) {
    closeLocked();
    return false;
  }

  //接受响应
  std::string recv_data;
  if (!recvFrame(recv_data)) {
    closeLocked();
    return false;
  }

  //解码响应
  RpcMessage response;
  if (!decodeMessage(recv_data, response)) {
    LOG_ERROR("RpcChannel: failed to decode response");
    return false;
  }

  //检查序列号是否匹配
  if (response.header().sequence_id() != seq) {
    LOG_ERROR("RpcChannel: sequence_id mismatch, expected %lu got %lu", seq,
              response.header().sequence_id());
    return false;
  }

  error_code = response.header().error_code();
  response_body = response.body();
  return true;
}