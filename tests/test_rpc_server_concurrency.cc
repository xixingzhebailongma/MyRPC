#include "rpc_client.h"
#include "rpc_server.h"
#include "service_manager.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr const char *kIp = "127.0.0.1";
constexpr const char *kService = "EchoService";

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

bool waitForServer(uint16_t port) {
  for (int i = 0; i < 100; ++i) {
    RpcClient client(kIp, port);
    std::string resp;
    int32_t ec = 0;
    if (client.Call(kService, "Ping", "ping", resp, ec) && ec == 0 &&
        resp == "pong") {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

int envInt(const char *name, int def) {
  const char *v = std::getenv(name);
  if (!v || !*v)
    return def;
  return std::atoi(v);
}

} // namespace

int main() {
  uint16_t port = pickFreePort();
  if (port == 0) {
    std::fprintf(stderr, "FAILED: pickFreePort()\n");
    return 1;
  }

  RpcServer server(kIp, port, 4, 8);
  server.serviceManager().registerMethod(kService, "Ping",
                                         [](const std::string &) {
                                           return std::string("pong");
                                         });
  server.serviceManager().registerMethod(kService, "Echo",
                                         [](const std::string &b) {
                                           return b;
                                         });

  std::thread serverThread([&server] { server.start(); });

  if (!waitForServer(port)) {
    server.stop();
    serverThread.join();
    std::fprintf(stderr, "FAILED: server did not become ready\n");
    return 1;
  }

  int nthreads = envInt("RPC_TEST_THREADS", 8);
  int ncalls = envInt("RPC_TEST_CALLS", 200);

  std::atomic<int> failures{0};

  std::vector<std::thread> pool;
  pool.reserve(static_cast<size_t>(nthreads));
  for (int t = 0; t < nthreads; ++t) {
    pool.emplace_back([&, t] {
      RpcClient client(kIp, port);
      for (int i = 0; i < ncalls; ++i) {
        std::string req = "t" + std::to_string(t) + ":" + std::to_string(i) +
                          ":" + std::string(64, static_cast<char>('a' + t % 26));
        std::string resp;
        int32_t ec = -1;
        if (!client.Call(kService, "Echo", req, resp, ec) || ec != 0 ||
            resp != req) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto &th : pool)
    th.join();

  server.stop();
  serverThread.join();

  if (failures.load() != 0) {
    std::fprintf(stderr, "FAILED: %d mismatches\n", failures.load());
    return 1;
  }
  std::printf("PASSED\n");
  return 0;
}
