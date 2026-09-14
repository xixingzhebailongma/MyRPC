// RpcChannel 并发多路复用回归测试。
//
// 验证重构后的 RpcChannel 不再靠 call_mutex_ 整体串行，而是一条 TCP 上
// 后台收帧线程按 sequence_id 分发响应，多线程可并发 Call 且帧不交错。
//
// 背压制造手法与 test_im_conn_concurrency.cc 相同：
//   1. 回显端 SO_RCVBUF 压到 4KB（listen socket 设置，accept 后继承）
//   2. 回显端每帧之间 sleep，拖慢排空速度
// 逼 ::send 在半帧处阻塞，任何帧交错都会让回显端切歪长度前缀而报错退出。

#include "rpc_channel.h"
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
#include <vector>

namespace {

int envInt(const char *name, int fallback) {
  const char *v = ::getenv(name);
  return v ? ::atoi(v) : fallback;
}

const int kThreads = envInt("RPC_TEST_THREADS", 8);
const int kCallsPerThread = envInt("RPC_TEST_CALLS", 10);
const size_t kBodySize =
    static_cast<size_t>(envInt("RPC_TEST_BODY_KB", 256)) * 1024;
const int kSlowMs = envInt("RPC_TEST_SLOW_MS", 2);
const int kRecvBufSize = 4096;
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

// 回显服务端：读一帧 -> decode -> 回 buildResponse(seq, 0, "echo:" + body)。
// 帧一旦切歪（长度前缀是垃圾）就退出，模拟真实服务端丢弃损坏连接的行为。
void echoServer(int listenfd, std::atomic<int> *frames_ok,
                std::atomic<bool> *stream_corrupted) {
  int c = ::accept(listenfd, nullptr, nullptr);
  ::close(listenfd);
  if (c < 0)
    return;

  for (;;) {
    uint32_t len = 0;
    if (!readFullFd(c, reinterpret_cast<char *>(&len), 4))
      break; // 对端正常关闭
    if (len == 0 || len > (64u << 20)) {
      fprintf(stderr, "[echo] 长度前缀不合法 (%u)，流已损坏\n", len);
      stream_corrupted->store(true);
      break;
    }
    std::string payload(len, '\0');
    if (!readFullFd(c, &payload[0], len))
      break;

    RpcMessage msg;
    if (!decodeMessage(payload, msg) || !msg.has_header()) {
      fprintf(stderr, "[echo] protobuf 解析失败 (len=%u)，流已损坏\n", len);
      stream_corrupted->store(true);
      break;
    }
    frames_ok->fetch_add(1);
    if (kSlowMs > 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(kSlowMs));

    std::string out = encodeMessage(
        buildResponse(msg.header().sequence_id(), 0, "echo:" + msg.body()));
    if (!sendFullFd(c, out))
      break;
  }
  ::close(c);
}

int listenLoopback(int *port, bool small_rcvbuf) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;
  int on = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  if (small_rcvbuf) {
    int rcvbuf = kRecvBufSize;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  }
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(0); // 让内核挑端口
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

// 对端收到请求后直接断开，Call 应该立刻返回而不是等满 timeout。
bool checkFailFastOnDisconnect() {
  int port = 0;
  int listenfd = listenLoopback(&port, false);
  if (listenfd < 0) {
    fprintf(stderr, "listen 失败\n");
    return false;
  }
  std::thread rude([listenfd] {
    int c = ::accept(listenfd, nullptr, nullptr);
    ::close(listenfd);
    if (c < 0)
      return;
    char buf[64];
    ::recv(c, buf, sizeof(buf), 0); // 收到点东西就翻脸
    ::close(c);
  });

  RpcChannel ch("127.0.0.1", static_cast<uint16_t>(port), kTimeoutMs);
  auto t0 = std::chrono::steady_clock::now();
  std::string body;
  int32_t err = 0;
  bool ok = ch.Call("Echo", "Echo", "hello", body, err);
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - t0)
                     .count();
  ch.close();
  rude.join();

  printf("对端断开后 Call 返回耗时 %lldms（timeout 设 %dms）\n",
         static_cast<long long>(elapsed), kTimeoutMs);
  if (ok) {
    fprintf(stderr, "FAILED: 对端没回响应，Call 却返回成功\n");
    return false;
  }
  if (elapsed >= kTimeoutMs / 2) {
    fprintf(stderr, "FAILED: 对端已断开，Call 仍等满了 timeout\n");
    return false;
  }
  return true;
}

} // namespace

int main() {
  int port = 0;
  int listenfd = listenLoopback(&port, /*small_rcvbuf=*/true);
  if (listenfd < 0) {
    perror("listen");
    return 1;
  }

  std::atomic<int> frames_ok{0};
  std::atomic<bool> stream_corrupted{false};
  std::thread server(echoServer, listenfd, &frames_ok, &stream_corrupted);

  RpcChannel ch("127.0.0.1", static_cast<uint16_t>(port), kTimeoutMs);

  printf("并发 Call 测试：%d 线程 x %d 次调用，每次 body %zu KB\n", kThreads,
         kCallsPerThread, kBodySize / 1024);

  std::atomic<int> failures{0};
  std::atomic<int> mismatches{0};
  std::vector<std::thread> workers;
  for (int i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i] {
      for (int j = 0; j < kCallsPerThread; ++j) {
        // 每个线程用不同的填充字符，交错后一眼可辨。
        std::string body = "t" + std::to_string(i) + "-c" + std::to_string(j) +
                           "-" +
                           std::string(kBodySize, static_cast<char>('a' + i));
        std::string got;
        int32_t err = -1;
        if (!ch.Call("Echo", "Echo", body, got, err))
          failures.fetch_add(1);
        else if (err != 0 || got != "echo:" + body)
          mismatches.fetch_add(1);
      }
    });
  }
  for (auto &t : workers)
    t.join();

  ch.close();
  server.join();

  const int total = kThreads * kCallsPerThread;
  printf("总调用 %d：失败/超时 %d，内容/错误码不匹配 %d\n", total,
         failures.load(), mismatches.load());
  printf("服务端完整收到 %d 帧，流损坏: %s\n", frames_ok.load(),
         stream_corrupted.load() ? "是" : "否");

  if (failures.load() != 0 || mismatches.load() != 0 ||
      stream_corrupted.load() || frames_ok.load() != total) {
    fprintf(stderr, "FAILED: 并发 Call 破坏了帧边界或响应路由\n");
    return 1;
  }

  // 第二阶段：对端断开后，在途 Call 应立刻返回，而不是白等满 timeout_ms。
  if (!checkFailFastOnDisconnect())
    return 1;

  printf("PASSED\n");
  return 0;
}