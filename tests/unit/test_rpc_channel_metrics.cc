// RpcChannel metrics + per-call 超时回归测试。
//
// 验证：totalCalls/successCalls/failCalls/延迟 正确累计；per-call 超时生效
//（服务端慢回 300ms，客户端只等 50ms 即超时失败）。

#include "rpc_channel.h"
#include "rpc_protocol.h"

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {

bool readFullFd(int fd, char *buf, size_t n) {
  size_t got = 0;
  while (got < n) {
    ssize_t r = ::recv(fd, buf + got, n - got, 0);
    if (r < 0 && errno == EINTR)
      continue;
    if (r <= 0)
      return false;
    got += static_cast<size_t>(r);
  }
  return true;
}

bool sendFullFd(int fd, const std::string &data) {
  size_t sent = 0;
  while (sent < data.size()) {
    ssize_t r =
        ::send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
    if (r < 0 && errno == EINTR)
      continue;
    if (r <= 0)
      return false;
    sent += static_cast<size_t>(r);
  }
  return true;
}

int listenLoopback(int *port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;
  int on = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(0);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (::bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) <
          0 ||
      ::listen(fd, 8) < 0) {
    ::close(fd);
    return -1;
  }
  socklen_t alen = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<struct sockaddr *>(&addr), &alen) <
      0) {
    ::close(fd);
    return -1;
  }
  *port = ntohs(addr.sin_port);
  return fd;
}

// 慢回回显服务端：reply_delay_ms 后再回 buildResponse(seq, 0, "echo:"+body)。
void echoServerSlow(int listenfd, std::atomic<int> *frames_ok,
                    int reply_delay_ms) {
  int c = ::accept(listenfd, nullptr, nullptr);
  ::close(listenfd);
  if (c < 0)
    return;
  for (;;) {
    uint32_t len = 0;
    if (!readFullFd(c, reinterpret_cast<char *>(&len), 4))
      break;
    if (len == 0 || len > (64u << 20))
      break;
    std::string payload(len, '\0');
    if (!readFullFd(c, &payload[0], len))
      break;

    RpcMessage msg;
    if (!decodeMessage(payload, msg) || !msg.has_header())
      break;
    frames_ok->fetch_add(1);

    if (reply_delay_ms > 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(reply_delay_ms));

    std::string out = encodeMessage(
        buildResponse(msg.header().sequence_id(), 0, "echo:" + msg.body()));
    if (!sendFullFd(c, out))
      break;
  }
  ::close(c);
}

} // namespace

int main() {
  int port = 0;
  int listenfd = listenLoopback(&port);
  if (listenfd < 0) {
    perror("listen");
    return 1;
  }
  std::atomic<int> frames_ok{0};
  std::thread server(echoServerSlow, listenfd, &frames_ok,
                     /*reply_delay_ms=*/300);

  RpcChannel ch("127.0.0.1", static_cast<uint16_t>(port), 3000);

  // 1) 正常调用：成功，metrics 累计。
  std::string body;
  int32_t err = -1;
  if (!ch.Call("Echo", "Echo", "a", body, err)) {
    fprintf(stderr, "FAILED: 正常 Call 失败\n");
    return 1;
  }
  if (ch.totalCalls() != 1 || ch.successCalls() != 1 || ch.failCalls() != 0) {
    fprintf(stderr, "FAILED: 成功调用 metrics 不对\n");
    return 1;
  }

  // 2) per-call 超时：服务端 300ms 才回，这里只等 50ms，应超时失败。
  if (ch.Call("Echo", "Echo", "b", body, err, /*timeout_ms=*/50)) {
    fprintf(stderr, "FAILED: per-call 超时未生效\n");
    return 1;
  }
  if (ch.totalCalls() != 2 || ch.failCalls() != 1) {
    fprintf(stderr, "FAILED: 超时调用 metrics 不对\n");
    return 1;
  }
  if (ch.maxLatencyUs() == 0) {
    fprintf(stderr, "FAILED: 延迟未记录\n");
    return 1;
  }

  printf("total=%llu success=%llu fail=%llu maxLatencyUs=%llu\n",
         (unsigned long long)ch.totalCalls(),
         (unsigned long long)ch.successCalls(),
         (unsigned long long)ch.failCalls(),
         (unsigned long long)ch.maxLatencyUs());

  ch.close();
  server.join();
  printf("PASSED\n");
  return 0;
}