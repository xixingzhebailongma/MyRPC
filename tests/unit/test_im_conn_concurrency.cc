// ImClientConn 并发发送回归测试。
//
// 复现的缺陷：call() 里的 sendAll() 在任何锁之外调用，两个线程并发 call()
// 时，各自的 [4字节LE长度][protobuf] 帧会在 socket 上交错，对端按长度前缀
// 切帧就切歪了，整条连接的流报废。
//
// 关键点：必须让 sendAll 里的 ::send 真正分多次返回，否则复现不出来。
// 阻塞 socket 上一次 ::send 通常会把整个 buffer 一次性塞进内核发送缓冲区，
// 所以光把 body 调大没用（实测 256KB + 快速回显端，测试是绿的）。
// 这里靠两件事制造背压，逼 ::send 在帧中间阻塞让出：
//   1. 回显端 SO_RCVBUF 压到 4KB（listen socket 上设置，accept 后继承）
//   2. 回显端每帧之间 sleep，拖慢排空速度
// 于是客户端发送缓冲区被填满，::send 在半帧处阻塞，另一个线程趁机插进去。
//
// 测试自带一个进程内回显服务端，不依赖 im_server / etcd / redis / mysql。

#include "im_client_conn.h"
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

// 默认值针对"复现帧交错"调优。TSan 下插桩太慢，跑不动这个量，
// 可用环境变量调小（race 检测不需要大 payload，只需要并发）：
//   IMTEST_BODY_KB=4 IMTEST_CALLS=5 IMTEST_SLOW_MS=0 ./test_im_conn_concurrency
int envInt(const char *name, int fallback) {
  const char *v = ::getenv(name);
  return v ? ::atoi(v) : fallback;
}

const int kThreads = envInt("IMTEST_THREADS", 8);
const int kCallsPerThread = envInt("IMTEST_CALLS", 10);
const size_t kBodySize =
    static_cast<size_t>(envInt("IMTEST_BODY_KB", 256)) * 1024;
const int kSlowMs = envInt("IMTEST_SLOW_MS", 2); // 回显端每帧的拖慢时间
const int kRecvBufSize = 4096;                   // 故意压小，制造背压
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
    // 拖慢排空速度，让客户端发送缓冲区有机会填满。
    if (kSlowMs > 0)
      std::this_thread::sleep_for(std::chrono::milliseconds(kSlowMs));

    std::string out = encodeMessage(
        buildResponse(msg.header().sequence_id(), 0, "echo:" + msg.body()));
    if (!sendFullFd(c, out))
      break;
  }
  ::close(c);
}

// 起一个监听 socket，返回 listenfd，端口写入 *port。
int listenLoopback(int *port, bool small_rcvbuf) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;
  int on = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
  if (small_rcvbuf) {
    // 压小接收缓冲，accept 出来的连接会继承 —— 制造背压的关键一步。
    int rcvbuf = kRecvBufSize;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  }
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(0); // 让内核挑端口
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (::bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0 ||
      ::listen(fd, 8) < 0) {
    ::close(fd);
    return -1;
  }
  socklen_t alen = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<struct sockaddr *>(&addr), &alen) < 0) {
    ::close(fd);
    return -1;
  }
  *port = ntohs(addr.sin_port);
  return fd;
}

// 对端收到请求后直接断开，call() 应该立刻返回而不是等满 timeout。
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

  ImClientConn conn;
  if (!conn.connect("127.0.0.1", port)) {
    fprintf(stderr, "connect 失败\n");
    rude.join();
    return false;
  }
  auto t0 = std::chrono::steady_clock::now();
  std::string got = conn.call("Echo", "Echo", "hello", kTimeoutMs);
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - t0)
                     .count();
  conn.close();
  rude.join();

  printf("对端断开后 call() 返回耗时 %lldms（timeout 设的是 %dms）\n",
         static_cast<long long>(elapsed), kTimeoutMs);
  if (!got.empty()) {
    fprintf(stderr, "FAILED: 对端没回响应，call() 却返回了内容\n");
    return false;
  }
  // 放宽到 timeout 的一半，只要不是"白等满"就算通过。
  if (elapsed >= kTimeoutMs / 2) {
    fprintf(stderr, "FAILED: 对端已断开，call() 仍等满了 timeout\n");
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

  ImClientConn conn;
  if (!conn.connect("127.0.0.1", port)) {
    fprintf(stderr, "connect 失败\n");
    return 1;
  }

  printf("并发发送测试：%d 线程 x %d 次调用，每次 body %zu KB\n", kThreads,
         kCallsPerThread, kBodySize / 1024);

  std::atomic<int> mismatches{0};
  std::atomic<int> empties{0};
  std::vector<std::thread> workers;
  for (int i = 0; i < kThreads; ++i) {
    workers.emplace_back([&, i] {
      for (int j = 0; j < kCallsPerThread; ++j) {
        // 每个线程用不同的填充字符，交错后一眼可辨。
        std::string body = "t" + std::to_string(i) + "-c" + std::to_string(j) +
                           "-" +
                           std::string(kBodySize, static_cast<char>('a' + i));
        std::string got = conn.call("Echo", "Echo", body, kTimeoutMs);
        if (got.empty())
          empties.fetch_add(1);
        else if (got != "echo:" + body)
          mismatches.fetch_add(1);
      }
    });
  }
  for (auto &t : workers)
    t.join();

  conn.close();
  server.join();

  const int total = kThreads * kCallsPerThread;
  printf("总调用 %d：空响应(失败/超时) %d，内容不匹配 %d\n", total,
         empties.load(), mismatches.load());
  printf("服务端完整收到 %d 帧，流损坏: %s\n", frames_ok.load(),
         stream_corrupted.load() ? "是" : "否");

  if (empties.load() != 0 || mismatches.load() != 0 ||
      stream_corrupted.load() || frames_ok.load() != total) {
    fprintf(stderr, "FAILED: 并发 call() 破坏了帧边界\n");
    return 1;
  }

  // 第二阶段：对端断开后，在途 call() 应立刻返回，而不是白等满 timeout_ms。
  // 覆盖 readLoop 退出时 closed_ + notify_all 的修复。
  if (!checkFailFastOnDisconnect())
    return 1;

  printf("PASSED\n");
  return 0;
}
