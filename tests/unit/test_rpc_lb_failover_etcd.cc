#include "lb_rpc_client.h"
#include "rpc_error_code.h"
#include "rpc_server.h"
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
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr const char *kIp = "127.0.0.1";
constexpr int kTtl = 10; // 短租约：崩溃残留的 key 10s 内自动过期，避免串扰

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

// 探测 etcd 是否可达：对一个不可能存在的 prefix 做 discover，
// 返回 nullopt 表示查询失败（etcd 不可达）；has_value 表示可达（空向量也算成功）。
bool etcdReachable(const std::string &etcd) {
  ServiceDiscovery sd(etcd);
  for (int i = 0; i < 20; ++i) {
    if (sd.discover("__myrpc_probe__").has_value())
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

// 等 discover 返回恰好 n 个节点。
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

// 自定义负载均衡器：select 始终返回「坏节点」的下标，
// 用来确定性触发 LbRpcClient 的 failover 逻辑。
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
    std::printf("test_rpc_lb_failover_etcd: SKIP (no etcd at %s)\n",
                etcd.c_str());
    return 0;
  }

  // 每次运行用唯一 service 名，避免残留 key 串扰。
  const std::string serviceName =
      "LbTestService." +
      std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count());

  const uint16_t portA = pickFreePort(); // 坏节点
  const uint16_t portB = pickFreePort(); // 好节点
  if (portA == 0 || portB == 0)
    return fail("pickFreePort()");
  const std::string addrA = std::string(kIp) + ":" + std::to_string(portA);
  const std::string addrB = std::string(kIp) + ":" + std::to_string(portB);

  // A：Failover 返回可重试业务错误码 1500；B：返回成功。
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

  // 阶段 1：注册 + 发现
  ServiceDiscovery sd(etcd);
  auto nodes = waitForNodes(sd, serviceName, 2);
  if (nodes.size() != 2)
    return fail("discover did not return 2 nodes");

  RpcClientConfig cfg;
  cfg.max_retries = 3;
  cfg.timeout_ms = 3000;

  // 阶段 2：轮询负载均衡——多次调用应打到两个节点
  {
    auto rr = std::make_shared<RoundRobinBalancer>();
    LbRpcClient client(etcd, serviceName, rr, cfg);
    std::set<std::string> seen;
    for (int i = 0; i < 20; ++i) {
      std::string resp;
      int32_t ec = -1;
      if (!client.Call("Who", "", resp, ec) || ec != 0)
        return fail("round-robin Who call failed");
      seen.insert(resp);
    }
    if (!seen.count("A") || !seen.count("B"))
      return fail("round-robin did not hit both nodes");
  }

  // 阶段 3：failover——坏节点返回可重试业务码，应自动切到好节点
  {
    auto balancer = std::make_shared<BadFirstBalancer>(addrA);
    LbRpcClient client(etcd, serviceName, balancer, cfg);
    std::string resp;
    int32_t ec = -1;
    if (!client.Call("Failover", "", resp, ec))
      return fail("failover call transport failed");
    if (ec != 0)
      return fail("failover call did not recover to good node");
    if (resp != "ok-B")
      return fail("failover response came from wrong node");
    if (client.failoverCount() < 1)
      return fail("failoverCount did not advance");
  }

  // 阶段 4：节点下线——stop(B) 撤销租约，watch 触发刷新，剩余节点继续服务
  serverB.stop();
  threadB.join();

  auto remaining = waitForNodes(sd, serviceName, 1);
  if (remaining.size() != 1)
    return fail("discover did not shrink to 1 node after stop");
  if (remaining[0].address() != addrA)
    return fail("remaining node is not the expected one");
  {
    auto rr = std::make_shared<RoundRobinBalancer>();
    LbRpcClient client(etcd, serviceName, rr, cfg);
    std::string resp;
    int32_t ec = -1;
    if (!client.Call("Who", "", resp, ec) || ec != 0 || resp != "A")
      return fail("call after node removal failed");
  }

  serverA.stop();
  threadA.join();

  std::printf("PASSED\n");
  return 0;
}
