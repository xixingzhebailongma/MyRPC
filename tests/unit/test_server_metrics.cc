// RpcServer 服务端指标埋点：请求计数、失败计数、幂等 replay 不重复执行、
// overload（工作队列满）记 failed。
#include "metrics_registry.h"
#include "rpc_channel.h"
#include "rpc_error_code.h"
#include "rpc_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>

namespace {

constexpr const char *kIp = "127.0.0.1";

uint16_t pickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return 0;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }
  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
    ::close(fd);
    return 0;
  }
  uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

bool waitForServer(uint16_t port, const char *service) {
  for (int i = 0; i < 100; ++i) {
    RpcChannel ch(kIp, port);
    std::string resp;
    int32_t ec = 0;
    if (ch.Call(service, "Ping", "ping", resp, ec) && ec == 0 &&
        resp == "pong")
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

int fail(const char *msg) {
  std::fprintf(stderr, "FAILED: %s\n", msg);
  return 1;
}

} // namespace

int main() {
  auto &reg = MetricsRegistry::instance();

  // ===== 1) 成功 + 幂等 replay：同 request_id 两次 → total +2、failed +0、
  // 业务只执行一次 =====
  {
    uint16_t port = pickFreePort();
    RpcServer server(kIp, port, 2, 4);
    std::atomic<int> counter{0};
    server.serviceManager().registerMethod(
        "MetricService", "Count", [&counter](const std::string &) {
          int n = counter.fetch_add(1) + 1;
          return std::to_string(n);
        });
    server.serviceManager().registerMethod(
        "MetricService", "Ping",
        [](const std::string &) { return std::string("pong"); });

    std::thread st([&server] { server.start(); });
    if (!waitForServer(port, "MetricService")) {
      server.stop();
      st.join();
      return fail("server (replay) not ready");
    }
    RpcChannel ch(kIp, port);

    uint64_t t0 = reg.serverSnapshot("MetricService", "Count").total;
    uint64_t f0 = reg.serverSnapshot("MetricService", "Count").failed;
    std::string resp;
    int32_t ec = -1;
    if (!ch.Call("MetricService", "Count", "", resp, ec, -1, "dup-id"))
      return fail("first Count transport failed");
    if (!ch.Call("MetricService", "Count", "", resp, ec, -1, "dup-id"))
      return fail("replay Count transport failed");
    uint64_t t1 = reg.serverSnapshot("MetricService", "Count").total;
    uint64_t f1 = reg.serverSnapshot("MetricService", "Count").failed;
    if (t1 - t0 != 2)
      return fail("same request_id should total +2");
    if (f1 - f0 != 0)
      return fail("same request_id failed should +0");
    if (counter.load() != 1)
      return fail("business re-executed on replay");

    server.stop();
    st.join();
  }

  // ===== 2) overload：工作队列满 → rejectOverloaded → failed +1 =====
  {
    uint16_t port = pickFreePort();
    RpcServer server(kIp, port, 1, 1, /*workQueueSize=*/1);
    std::atomic<int> entered{0};
    std::atomic<bool> unblock{false};
    server.serviceManager().registerMethod(
        "OverloadService", "Block", [&](const std::string &) {
          entered.fetch_add(1);
          while (!unblock.load())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          return std::string("ok");
        });
    server.serviceManager().registerMethod(
        "OverloadService", "Ping",
        [](const std::string &) { return std::string("pong"); });

    std::thread st([&server] { server.start(); });
    if (!waitForServer(port, "OverloadService")) {
      server.stop();
      st.join();
      return fail("server (overload) not ready");
    }
    RpcChannel ch(kIp, port);

    // 三个并发请求：1 个在跑（阻塞）、1 个排队、1 个被拒（队列满）。
    std::string r1, r2, r3;
    int32_t e1 = 0, e2 = 0, e3 = 0;
    std::thread t1(
        [&] { ch.Call("OverloadService", "Block", "", r1, e1); });
    while (entered.load() < 1)
      std::this_thread::sleep_for(std::chrono::milliseconds(1)); // handler 已开始
    std::thread t2(
        [&] { ch.Call("OverloadService", "Block", "", r2, e2); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50)); // t2 已入队
    std::thread t3(
        [&] { ch.Call("OverloadService", "Block", "", r3, e3); });
    t3.join(); // t3 应立即拿到 overload 响应

    if (e3 != static_cast<int32_t>(RpcError::SERVER_OVERLOADED))
      return fail("overloaded request error_code != SERVER_OVERLOADED");
    if (reg.serverSnapshot("OverloadService", "Block").failed != 1)
      return fail("overload failed != 1");

    unblock.store(true); // 放行阻塞 handler
    t1.join();
    t2.join();

    if (reg.serverSnapshot("OverloadService", "Block").total != 3)
      return fail("overload total != 3");
    if (reg.serverSnapshot("OverloadService", "Block").failed != 1)
      return fail("overload failed should stay 1");

    server.stop();
    st.join();
  }

  std::printf("PASSED\n");
  return 0;
}
