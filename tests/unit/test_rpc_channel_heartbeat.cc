// RpcChannel 应用层心跳（type 字段方案）回归测试。
//
// 验证：空闲期客户端 readLoop 会发 type=MSG_HEARTBEAT 的心跳；服务端回
// MSG_HEARTBEAT_ACK；连接不被误判为死亡。

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

// 回显服务端：收到 MSG_HEARTBEAT 回 MSG_HEARTBEAT_ACK（并计数 heartbeats），
// 其余帧回 buildResponse(seq, 0, "echo:" + body)（计数 frames_ok）。
void echoServer(int listenfd, std::atomic<int> *frames_ok,
                std::atomic<int> *heartbeats) {
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
    uint64_t seq = msg.header().sequence_id();

    if (msg.header().type() == MessageType::MSG_HEARTBEAT) {
      heartbeats->fetch_add(1);
      if (!sendFullFd(c, encodeMessage(buildHeartbeatAck(seq))))
        break;
    } else {
      frames_ok->fetch_add(1);
      std::string out =
          encodeMessage(buildResponse(seq, 0, "echo:" + msg.body()));
      if (!sendFullFd(c, out))
        break;
    }
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
  std::atomic<int> heartbeats{0};
  std::thread server(echoServer, listenfd, &frames_ok, &heartbeats);

  RpcChannel ch("127.0.0.1", static_cast<uint16_t>(port), 3000);
  ch.setHeartbeat(/*interval_ms=*/200, /*miss_threshold=*/3);

  // 先发起一次真实调用建立连接。
  std::string body;
  int32_t err = -1;
  if (!ch.Call("Echo", "Echo", "hello", body, err) || err != 0) {
    fprintf(stderr, "FAILED: 首次 Call 失败\n");
    return 1;
  }

  // 空置 ~1s：readLoop 应每 200ms 发一帧 MSG_HEARTBEAT，服务端回 ACK。
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  if (heartbeats.load() == 0) {
    fprintf(stderr, "FAILED: 空闲期未发 type=MSG_HEARTBEAT 的心跳\n");
    return 1;
  }
  if (!ch.isConnected()) {
    fprintf(stderr, "FAILED: 心跳被误判为死亡\n");
    return 1;
  }
  printf("空闲期收到 %d 帧心跳（type=MSG_HEARTBEAT），连接保持存活\n",
         heartbeats.load());

  ch.close();
  server.join();
  printf("PASSED\n");
  return 0;
}