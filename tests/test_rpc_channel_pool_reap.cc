// RpcChannelPool 空闲回收 + 剔除即关闭回归测试。
//
// 验证 P0 生命周期治理：
//   1. 空闲回收：channel 超过 idle_ttl_ms 未被调用，被后台 reaper 剔除并主动
//   close；
//   2. 剔除即关闭：removeExcept 剔除节点后，其 channel 的连接被主动 shutdown，
//      在途/后续 Call 快速失败（isConnected()==false），而非等 shared_ptr
//      归零。
//
// 用短 idle_ttl_ms + 短 reap_interval 构造池，使回收在几百毫秒内可观测。

#include "rpc_channel.h"
#include "rpc_channel_pool.h"
#include "rpc_protocol.h"

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
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

// 回显服务端：读一帧 -> decode -> 回 buildResponse(seq, 0, "echo:" + body)。
void echoServer(int listenfd, std::atomic<int> *frames_ok) {
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

// 轮询等待 pool.size()==0，超时返回 false（避免固定 sleep 引入的抖动）。
bool waitUntilReaped(RpcChannelPool &pool, int timeout_ms) {
  auto t0 = std::chrono::steady_clock::now();
  while (pool.size() != 0) {
    if (std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0)
            .count() > timeout_ms)
      return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return true;
}

} // namespace

int main() {
  // ---- 阶段一：空闲回收 ----
  {
    int port = 0;
    int listenfd = listenLoopback(&port);
    if (listenfd < 0) {
      perror("listen");
      return 1;
    }
    std::atomic<int> frames_ok{0};
    std::thread server(echoServer, listenfd, &frames_ok);

    // 短 TTL + 短扫描周期，让回收在几百毫秒内发生。
    RpcChannelPool pool(/*idle_ttl_ms=*/100, std::chrono::milliseconds(50));
    const std::string key = "127.0.0.1:" + std::to_string(port);
    auto ch = pool.getOrCreate(key, "127.0.0.1", static_cast<uint16_t>(port));

    std::string body;
    int32_t err = -1;
    if (!ch->Call("Echo", "Echo", "hello", body, err) || err != 0) {
      fprintf(stderr, "FAILED: 首次 Call 失败\n");
      return 1;
    }

    // 空置，等 reaper 回收。
    if (!waitUntilReaped(pool, /*timeout_ms=*/1000)) {
      fprintf(stderr, "FAILED: 空闲 channel 未被回收\n");
      return 1;
    }
    if (pool.contains(key)) {
      fprintf(stderr, "FAILED: size()==0 但 contains 仍为真\n");
      return 1;
    }

    // reaper 先 erase 再 close()，两者有微小先后；再轮询等连接真正断开。
    auto t1 = std::chrono::steady_clock::now();
    while (ch->isConnected()) {
      if (std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t1)
              .count() > 1000) {
        fprintf(stderr, "FAILED: 回收后 channel 未断开\n");
        return 1;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    printf("空闲回收：空闲 channel 已被 reaper 主动 close 并剔除\n");

    server.join();
  }

  // ---- 阶段二：剔除即关闭 ----
  {
    int port = 0;
    int listenfd = listenLoopback(&port);
    if (listenfd < 0) {
      perror("listen");
      return 1;
    }
    std::atomic<int> frames_ok{0};
    std::thread server(echoServer, listenfd, &frames_ok);

    // 阶段二用长 TTL，避免 reaper 干扰，只测 removeExcept 的主动关闭。
    RpcChannelPool pool(/*idle_ttl_ms=*/60'000,
                        std::chrono::milliseconds(10'000));
    const std::string key = "127.0.0.1:" + std::to_string(port);
    auto ch = pool.getOrCreate(key, "127.0.0.1", static_cast<uint16_t>(port));

    std::string body;
    int32_t err = -1;
    if (!ch->Call("Echo", "Echo", "hello", body, err) || err != 0) {
      fprintf(stderr, "FAILED: 阶段二首次 Call 失败\n");
      return 1;
    }

    pool.removeExcept({}); // 保留空集 → 全部剔除
    if (pool.size() != 0) {
      fprintf(stderr, "FAILED: removeExcept 后池非空\n");
      return 1;
    }
    // close() 已同步 shutdown + join，返回后 sockfd_ 必为 -1。
    if (ch->isConnected()) {
      fprintf(stderr, "FAILED: removeExcept 后 channel 仍处于连接态\n");
      return 1;
    }
    printf("剔除即关闭：removeExcept 已主动 shutdown 连接\n");

    server.join();
  }

  printf("PASSED\n");
  return 0;
}