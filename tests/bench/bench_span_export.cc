// SpanExporter 同步写对 RPC 延迟的影响基准：固定 ping 负载，开/关对比 P50/P99。
// 用法：./bench_span_export          （关：不 init SpanExporter）
//       MYRPC_SPAN_EXPORT=1 ./bench_span_export  （开：写 /tmp/bench_spans.jsonl）
#include "Logger.h"
#include "span_exporter.h"
#include "rpc_channel.h"
#include "rpc_server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

static uint16_t pickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
  socklen_t len = sizeof(addr);
  ::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len);
  uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

int main() {
  Logger::instance().setServiceName("bench");
  Logger::instance().init(LogLevel::INFO, "", /*console=*/false);
  if (std::getenv("MYRPC_SPAN_EXPORT")) {
    SpanExporter::instance().init("/tmp/bench_spans.jsonl");
    std::printf("[span export ON]\n");
  } else {
    std::printf("[span export OFF]\n");
  }

  uint16_t port = pickFreePort();
  RpcServer server("127.0.0.1", port, 2, 4);
  server.serviceManager().registerMethod(
      "Bench", "Ping", [](const std::string &) { return std::string("pong"); });
  std::thread st([&server] { server.start(); });

  RpcChannel ch("127.0.0.1", port);
  std::string resp;
  int32_t ec = 0;
  for (int i = 0; i < 100 && !ch.Call("Bench", "Ping", "", resp, ec); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

  constexpr int kNum = 10000;
  std::vector<uint64_t> lats;
  lats.reserve(kNum);
  for (int i = 0; i < kNum; ++i) {
    auto t0 = std::chrono::steady_clock::now();
    ch.Call("Bench", "Ping", "", resp, ec);
    auto t1 = std::chrono::steady_clock::now();
    lats.push_back(static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count()));
  }
  std::sort(lats.begin(), lats.end());
  auto pct = [&](double q) { return lats[static_cast<size_t>(q * (kNum - 1))]; };
  std::printf("N=%d P50=%lluus P99=%lluus\n", kNum,
              (unsigned long long)pct(0.50), (unsigned long long)pct(0.99));

  server.stop();
  st.join();
  return 0;
}
