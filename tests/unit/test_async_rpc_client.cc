// AsyncRpcClient 功能验证：一条连接上并发 future 多路复用，
// 响应按 sequence_id 正确路由，帧不交错。
#include "async_rpc_client.h"
#include "rpc_protocol.h"

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <future>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

int envInt(const char *name, int fallback) {
  const char *v = ::getenv(name);
  return v ? ::atoi(v) : fallback;
}

const int kCalls = envInt("ASYNC_TEST_CALLS", 500);
const int kTimeoutMs = 15000;

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

// 单连接回显服务端：读帧 -> 回 buildResponse(seq, 0, "echo:" + body)
void echoServer(int listenfd, std::atomic<int> *frames_ok) {
  int c = ::accept(listenfd, nullptr, nullptr);
  ::close(listenfd);
  if (c < 0)
    return;
  for (;;) {
    char hdr[4];
    if (!readFullFd(c, hdr, 4))
      break;
    uint32_t len = readLenBE(hdr);
    if (len == 0 || len > (64u << 20))
      break;
    std::string payload(len, '\0');
    if (!readFullFd(c, &payload[0], len))
      break;
    RpcMessage msg;
    if (!decodeMessage(payload, msg) || !msg.has_header())
      break;
    frames_ok->fetch_add(1);
    std::string out = encodeMessage(
        buildResponse(msg.header().sequence_id(), 0, "echo:" + msg.body()));
    if (!sendFullFd(c, out))
      break;
  }
  ::close(c);
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
  addr.sin_port = htons(0); // 内核挑端口
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (::bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) <
          0 ||
      ::listen(fd, 8) < 0) {
    ::close(fd);
    return -1;
  }
  socklen_t alen = sizeof(addr);
  ::getsockname(fd, reinterpret_cast<struct sockaddr *>(&addr), &alen);
  *port = ntohs(addr.sin_port);
  return fd;
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
  std::thread server(echoServer, listenfd, &frames_ok);

  AsyncRpcClient client(4); // 4 个 event loop 线程

  std::vector<std::future<std::string>> futs;
  futs.reserve(kCalls);
  for (int i = 0; i < kCalls; ++i) {
    std::string body = "req-" + std::to_string(i);
    futs.push_back(client.Call("127.0.0.1", static_cast<uint16_t>(port), "Echo",
                               "Echo", body, kTimeoutMs));
  }

  int failures = 0, mismatches = 0;
  for (int i = 0; i < kCalls; ++i) {
    std::string body = "req-" + std::to_string(i);
    try {
      std::string got = futs[i].get();
      if (got != "echo:" + body)
        mismatches++;
    } catch (const std::exception &) {
      failures++;
    }
  }

  client.stop();
  server.join();

  printf("总调用 %d：异常 %d，内容不匹配 %d，服务端收到 %d 帧\n", kCalls,
         failures, mismatches, frames_ok.load());
  if (failures != 0 || mismatches != 0 || frames_ok.load() != kCalls) {
    fprintf(stderr, "FAILED\n");
    return 1;
  }
  printf("PASSED\n");
  return 0;
}