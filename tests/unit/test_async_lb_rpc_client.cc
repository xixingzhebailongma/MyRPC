// AsyncLbRpcClient 功能验证：etcd 服务发现 + 负载均衡 + 异步 failover。
// 与同步版 test_rpc_lb_failover_etcd 对齐，但走 AsyncLbRpcClient 的 future
// 回调链，验证：轮询打散到多节点、可重试业务错误触发 failover、一致哈希 key 粘滞。
#include "async_lb_rpc_client.h"
#include "load_balancer.h"
#include "rpc_server.h"
#include "service_discovery.h"
#include "service_manager.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr const char *kIp = "127.0.0.1";
constexpr int kTtl = 10; // 短租约：崩溃残留的 key 10s 内自动过期

std::string envStr(const char *name, const std::string &def) {
  const char *v = std::getenv(name);
  return (v && *v) ? std::string(v) : def;
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

bool etcdReachable(const std::string &etcd) {
  ServiceDiscovery sd(etcd);
  for (int i = 0; i < 20; ++i) {
    if (sd.discover("__myrpc_probe__").has_value())
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

std::vector<ServiceNode> waitForNodes(ServiceDiscovery &sd,
                                      const std::string &serviceName, size_t n) {
  for (int i = 0; i < 100; ++i) {
    auto nodes = sd.discover(serviceName);
    if (nodes && nodes->size() == n)
      return *nodes;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return {};
}

int fail(const char *msg) {
  std::fprintf(stderr, "FAILED: %s\n", msg);
  return 1;
}

// select 始终返回「坏节点」的下标，确定性触发 failover。
struct BadFirstBalancer : public ILoadBalancer {
  explicit BadFirstBalancer(std::string badAddr)
      : badAddr_(std::move(badAddr)) {}
  size_t select(size_t) override {
    for (size_t i = 0; i < nodeIds_.size(); ++i)
      if (nodeIds_[i] == badAddr_)
        return i;
    return 0;
  }
  void rebuild(const std::vector<std::string> &nodeIds) override {
    nodeIds_ = nodeIds;
  }
  std::string badAddr_;
  std::vector<std::string> nodeIds_;
};

} // namespace

int main() {
  const std::string etcd = envStr("RPC_TEST_ETCD", "http://127.0.0.1:2379");
  if (!etcdReachable(etcd)) {
    std::printf("test_async_lb_rpc_client: SKIP (no etcd at %s)\n",
                etcd.c_str());
    return 0;
  }

  const std::string serviceName =
      "AsyncLbTestService." +
      std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count());

  const uint16_t portA = pickFreePort(); // 坏节点（retryable 业务码）
  const uint16_t portB = pickFreePort(); // 好节点
  if (portA == 0 || portB == 0)
    return fail("pickFreePort()");
  const std::string addrA = std::string(kIp) + ":" + std::to_string(portA);
  const std::string addrB = std::string(kIp) + ":" + std::to_string(portB);

  RpcServer serverA(kIp, portA, 2, 4);
  RpcServer serverB(kIp, portB, 2, 4);
  serverA.serviceManager().registerMethod(serviceName, "Who",
                                          [](const std::string &) {
                                            return std::string("A");
                                          });
  serverA.serviceManager().registerMethodWithResult(
      serviceName, "Failover", [](const std::string &) -> RpcMethodResult {
        return {1500, "retryable-A"};
      });
  serverB.serviceManager().registerMethod(serviceName, "Who",
                                          [](const std::string &) {
                                            return std::string("B");
                                          });
  serverB.serviceManager().registerMethodWithResult(
      serviceName, "Failover", [](const std::string &) -> RpcMethodResult {
        return {0, "ok-B"};
      });

  serverA.enableRegistry(etcd, serviceName, kIp, portA, kTtl);
  serverB.enableRegistry(etcd, serviceName, kIp, portB, kTtl);

  std::thread threadA([&serverA] { serverA.start(); });
  std::thread threadB([&serverB] { serverB.start(); });

  ServiceDiscovery sd(etcd);
  auto nodes = waitForNodes(sd, serviceName, 2);
  if (nodes.size() != 2) {
    serverA.stop();
    serverB.stop();
    threadA.join();
    threadB.join();
    return fail("discover did not return 2 nodes");
  }

  RpcClientConfig cfg;
  cfg.max_retries = 3;
  cfg.timeout_ms = 3000;

  // 阶段 1：轮询负载均衡——多次异步调用应打到两个节点
  {
    auto rr = std::make_shared<RoundRobinBalancer>();
    AsyncLbRpcClient client(etcd, serviceName, rr, cfg);
    std::set<std::string> seen;
    for (int i = 0; i < 20; ++i) {
      try {
        std::string resp = client.Call("Who", "").get();
        seen.insert(resp);
      } catch (const std::exception &) {
        serverA.stop();
        serverB.stop();
        threadA.join();
        threadB.join();
        return fail("round-robin async Who call threw");
      }
    }
    if (!seen.count("A") || !seen.count("B"))
      return fail("round-robin async did not hit both nodes");
  }

  // 阶段 2：failover——坏节点返回可重试业务码，应自动切到好节点
  {
    auto balancer = std::make_shared<BadFirstBalancer>(addrA);
    AsyncLbRpcClient client(etcd, serviceName, balancer, cfg);
    std::string resp;
    try {
      resp = client.Call("Failover", "").get();
    } catch (const std::exception &) {
      return fail("failover async call threw");
    }
    if (resp != "ok-B")
      return fail("failover async response came from wrong node");
    if (client.failoverCount() < 1)
      return fail("failoverCount did not advance");
  }

  // 阶段 3：一致哈希 key 粘滞——同一 key 固定到同一节点
  {
    auto ch = std::make_shared<ConsistentHashBalancer>(150);
    AsyncLbRpcClient client(etcd, serviceName, ch, cfg);
    std::string first;
    for (int i = 0; i < 5; ++i) {
      std::string resp = client.Call("Who", "", "sticky-user").get();
      if (i == 0)
        first = resp;
      else if (resp != first)
        return fail("consistent-hash key did not stick to one node");
    }
  }

  serverA.stop();
  serverB.stop();
  threadA.join();
  threadB.join();

  std::printf("PASSED\n");
  return 0;
}
