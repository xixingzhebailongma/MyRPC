// AsyncRpcClient 吞吐/延迟基准：起 K 个回显 RpcServer，用 AsyncRpcClient
// （epoll 多路复用）以 W 个并发 worker 打满指定时长，量化：
//   总请求数 / QPS / 平均与 p50/p90/p99 延迟。
//
// 用法：./bench_async_rpc [--servers=K] [--duration=S] [--threads=L]
//                          [--concurrency=W] [--payload=N]
//   servers      回显服务节点数（= 客户端并发连接数）
//   duration     压测时长（秒）
//   threads      AsyncRpcClient 事件循环线程数
//   concurrency  并发打流的 worker 线程数
//   payload      echo 请求体字节数
#include "async_rpc_client.h"
#include "rpc_server.h"
#include "service_manager.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

int argInt(int argc, char **argv, const char *key, int def) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind(key, 0) == 0)
      return std::atoi(a.c_str() + std::strlen(key));
  }
  return def;
}

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

} // namespace

int main(int argc, char **argv) {
  const int kServers = argInt(argc, argv, "--servers=", 4);
  const int kDuration = argInt(argc, argv, "--duration=", 5);
  const int kThreads = argInt(argc, argv, "--threads=", 0); // 0 = 自动
  const int kConcurrency = argInt(argc, argv, "--concurrency=", 8);
  const int kPayload = argInt(argc, argv, "--payload=", 64);
  constexpr const char *kIp = "127.0.0.1";
  constexpr const char *kSvc = "Echo";

  // 起 K 个回显 RpcServer。
  std::vector<uint16_t> ports;
  std::vector<std::unique_ptr<RpcServer>> servers;
  std::vector<std::thread> serverThreads;
  for (int i = 0; i < kServers; ++i) {
    uint16_t port = pickFreePort();
    if (!port) {
      std::fprintf(stderr, "pickFreePort failed\n");
      return 1;
    }
    auto srv = std::make_unique<RpcServer>(kIp, port, 2, 8);
    srv->serviceManager().registerMethod(
        kSvc, "Echo", [](const std::string &b) { return b; });
    ports.push_back(port);
    serverThreads.emplace_back([srv = srv.get()] { srv->start(); });
    servers.push_back(std::move(srv));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300)); // 等 accept

  // 请求体。
  const std::string body(kPayload, 'x');

  AsyncRpcClient client(kThreads);

  std::atomic<uint64_t> total{0};
  std::atomic<uint64_t> errors{0};
  std::vector<std::vector<uint64_t>> latencies(kConcurrency);
  std::atomic<bool> stop{false};

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::seconds(kDuration);

  std::vector<std::thread> workers;
  for (int w = 0; w < kConcurrency; ++w) {
    workers.emplace_back([&, w] {
      latencies[w].reserve(1 << 16);
      uint64_t i = 0;
      while (!stop.load()) {
        uint16_t port = ports[(i++) % ports.size()];
        auto t0 = std::chrono::steady_clock::now();
        bool ok = true;
        try {
          std::string resp =
              client.Call(kIp, port, kSvc, "Echo", body, 5000).get();
          if (resp != body)
            ok = false;
        } catch (const std::exception &) {
          ok = false;
        }
        auto t1 = std::chrono::steady_clock::now();
        uint64_t us =
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count();
        if (ok) {
          total.fetch_add(1);
          latencies[w].push_back(us);
        } else {
          errors.fetch_add(1);
        }
        if (t1 >= deadline)
          stop.store(true);
      }
    });
  }
  for (auto &t : workers)
    t.join();

  client.stop();

  // 汇总延迟。
  std::vector<uint64_t> all;
  for (auto &v : latencies)
    all.insert(all.end(), v.begin(), v.end());
  std::sort(all.begin(), all.end());

  uint64_t sum = 0;
  for (auto us : all)
    sum += us;
  double avg = all.empty() ? 0.0 : double(sum) / all.size();
  auto pct = [&](double p) {
    if (all.empty())
      return uint64_t(0);
    size_t idx = std::min(all.size() - 1,
                          size_t(p * all.size()));
    return all[idx];
  };

  double qps = double(total.load()) / kDuration;
  std::printf("========== AsyncRpcClient 基准 ==========\n");
  std::printf("servers=%d duration=%ds threads=%d concurrency=%d payload=%dB\n",
              kServers, kDuration, kThreads, kConcurrency, kPayload);
  std::printf("总请求: %llu   错误: %llu\n",
              (unsigned long long)total.load(),
              (unsigned long long)errors.load());
  std::printf("QPS: %.0f\n", qps);
  std::printf("延迟(us): avg=%.1f  p50=%llu  p90=%llu  p99=%llu  max=%llu\n",
              avg, (unsigned long long)pct(0.50), (unsigned long long)pct(0.90),
              (unsigned long long)pct(0.99),
              all.empty() ? 0ULL : (unsigned long long)all.back());

  for (auto &s : servers)
    s->stop();
  for (auto &t : serverThreads)
    t.join();
  return 0;
}
