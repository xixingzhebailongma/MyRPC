// 覆盖 etcd 服务发现 watch 的增量/全量行为，以及应用层断连探测。
//
// 依赖一个真实的 etcd 二进制（RPC_TEST_ETCD_BIN，默认
// /tmp/e2e-infra/etcd-root/usr/bin/etcd）与 etcdctl（RPC_TEST_ETCDCTL_BIN）。
// 测试自管一个独立单节点 etcd 实例（独立端口/数据目录），因此可以随意 kill/restart。
//
// 场景：
//  1/2) 存量 LbRpcClient 经 watch 的 Add/Remove 增量感知节点
//  A)   pkill etcd -> 探活失败 -> 重启（同端口、空数据）-> 客户端强制全量 ls 恢复
//  B)   etcd 短暂重启（< 阈值）-> 从 last_revision+1 续传，不触发全量
//  C)   etcd 在线长时间空闲 -> 探活持续成功，不触发全量、不误判
//  D)   etcd 快速重启（断连 gap < isolate_ms）-> 靠 watchDisconnected_ 续传恢复，不卡死

#include "etcd_client.h"
#include "lb_rpc_client.h"
#include "load_balancer.h"
#include "rpc_client_config.h"
#include "rpc_server.h"
#include "service_manager.h"
#include "test_helpers.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <condition_variable>
#include <string>
#include <thread>
#include <vector>

namespace {

std::string envOr(const char *name, const char *def) {
  const char *v = std::getenv(name);
  return (v && *v) ? std::string(v) : std::string(def);
}
std::string etcdBin() {
  return envOr("RPC_TEST_ETCD_BIN", "/tmp/e2e-infra/etcd-root/usr/bin/etcd");
}
std::string etcdctlBin() {
  return envOr("RPC_TEST_ETCDCTL_BIN",
               "/tmp/e2e-infra/etcd-client-root/usr/bin/etcdctl");
}

struct EtcdProc {
  pid_t pid = -1;
  uint16_t port = 0;
  uint16_t peerPort = 0;
  std::string dataDir;
};

std::vector<std::string> etcdArgs(const EtcdProc &p, bool fresh) {
  return {
      etcdBin(),
      "--name=test",
      "--data-dir=" + p.dataDir,
      "--listen-client-urls=http://127.0.0.1:" + std::to_string(p.port),
      "--advertise-client-urls=http://127.0.0.1:" + std::to_string(p.port),
      "--listen-peer-urls=http://127.0.0.1:" + std::to_string(p.peerPort),
      "--initial-advertise-peer-urls=http://127.0.0.1:" +
          std::to_string(p.peerPort),
      "--initial-cluster=test=http://127.0.0.1:" + std::to_string(p.peerPort),
      "--initial-cluster-state=" +
          (fresh ? std::string("new") : std::string("existing")),
  };
}

bool startEtcd(EtcdProc &p, bool fresh) {
  if (fresh) {
    std::string rm = "rm -rf " + p.dataDir + " && mkdir -p " + p.dataDir;
    ::system(rm.c_str());
  }
  p.pid = testutil::spawn(etcdArgs(p, fresh), p.dataDir + "/etcd.log");
  if (p.pid < 0)
    return false;
  return testutil::waitForPort("127.0.0.1", p.port, 300);
}

void stopEtcd(EtcdProc &p) {
  if (p.pid > 0) {
    testutil::stopProcess(p.pid);
    p.pid = -1;
  }
}

// 硬杀（SIGKILL）：模拟宕机。SIGTERM 优雅关闭会让 gRPC wait-for-ready 一直重试，
// 客户端感知不到断连，只有 SIGKILL 才能触发探活失败路径。
void killEtcd(EtcdProc &p) {
  if (p.pid > 0) {
    ::kill(p.pid, SIGKILL);
    int st = 0;
    ::waitpid(p.pid, &st, 0);
    p.pid = -1;
  }
}

// RAII：保证无论测试成功/失败退出，都会停掉自管 etcd
struct EtcdGuard {
  EtcdProc &p;
  explicit EtcdGuard(EtcdProc &proc) : p(proc) {}
  ~EtcdGuard() { stopEtcd(p); }
};

std::string etcdUrl(const EtcdProc &p) {
  return "http://127.0.0.1:" + std::to_string(p.port);
}

// ---------- 事件收集 ----------
struct Collector {
  std::mutex mu;
  std::condition_variable cv;
  std::vector<EtcdWatchEvent> events;

  void push(const EtcdWatchEvent &e) {
    {
      std::lock_guard<std::mutex> lk(mu);
      events.push_back(e);
    }
    cv.notify_all();
  }

  size_t count(EtcdWatchEvent::Type t) {
    std::lock_guard<std::mutex> lk(mu);
    size_t n = 0;
    for (const auto &e : events)
      if (e.type == t)
        ++n;
    return n;
  }

  bool waitCount(EtcdWatchEvent::Type t, size_t n, int timeoutMs) {
    std::unique_lock<std::mutex> lk(mu);
    return cv.wait_for(lk, std::chrono::milliseconds(timeoutMs), [&] {
      size_t c = 0;
      for (const auto &e : events)
        if (e.type == t)
          ++c;
      return c >= n;
    });
  }
};

int fail(const char *msg) {
  std::fprintf(stderr, "FAILED: %s\n", msg);
  return 1;
}

// 快速探活/重连的 watch 客户端配置
EtcdConfig fastCfg(const std::string &endpoints, int disconnectFullMs) {
  EtcdConfig c;
  c.endpoints = endpoints;
  c.retry_base_ms = 100;
  c.retry_max_ms = 500;
  c.health_check_interval_ms = 100; // 探活间隔
  c.watch_probe_timeout_ms = 1000;   // 探活 Range 超时
  c.watch_probe_isolate_ms = 300;   // 隔离确认窗口
  c.watch_disconnect_full_ms = disconnectFullMs;
  return c;
}

bool putRetry(EtcdClient &c, const std::string &k, const std::string &v,
              int tries = 50) {
  for (int i = 0; i < tries; ++i) {
    if (c.put(k, v))
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

// 路由测试用的 RPC server：start 后不自我注册，由测试直接 put/del key 控制
struct Node {
  uint16_t port;
  std::string addr;
  RpcServer server;
  std::thread th;

  Node(const std::string &svc, const std::string &tag, uint16_t p)
      : port(p), addr("127.0.0.1:" + std::to_string(p)),
        server("127.0.0.1", p, 2, 4) {
    server.serviceManager().registerMethod(svc, "Who",
                                           [tag](const std::string &) {
                                             return tag;
                                           });
  }
  ~Node() {
    if (th.joinable()) { // start() 过但没 stop()（早退路径）：先停 server 再 join
      server.stop();
      th.join();
    }
  }
  void start() {
    th = std::thread([this] { server.start(); });
    testutil::waitForPort("127.0.0.1", port, 100);
  }
  void stop() {
    server.stop();
    if (th.joinable())
      th.join();
  }
};

bool waitUntilHit(LbRpcClient &client, const std::string &target, int timeoutMs) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    std::string resp;
    int32_t ec = -1;
    if (client.Call("Who", "", resp, ec) && ec == 0 && resp == target)
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
  }
  return false;
}

bool waitUntilOnly(LbRpcClient &client, const std::string &a,
                   const std::string &b, int timeoutMs, int streak = 10) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeoutMs);
  int cnt = 0;
  while (std::chrono::steady_clock::now() < deadline) {
    std::string resp;
    int32_t ec = -1;
    if (client.Call("Who", "", resp, ec) && ec == 0) {
      if (resp == b) {
        cnt = 0;
      } else if (resp == a) {
        if (++cnt >= streak)
          return true;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
  }
  return false;
}

} // namespace

int main() {
  if (::access(etcdBin().c_str(), X_OK) != 0) {
    std::printf("test_watch_incremental: SKIP (no etcd binary at %s)\n",
                etcdBin().c_str());
    return 0;
  }
  if (::access(etcdctlBin().c_str(), X_OK) != 0) {
    std::printf("test_watch_incremental: SKIP (no etcdctl at %s)\n",
                etcdctlBin().c_str());
    return 0;
  }

  EtcdProc etcd;
  etcd.port = testutil::pickFreePort();
  etcd.peerPort = testutil::pickFreePort();
  etcd.dataDir = "/tmp/myrpc_test_etcd_" + std::to_string(::getpid());
  if (etcd.port == 0 || etcd.peerPort == 0)
    return fail("pickFreePort()");
  if (!startEtcd(etcd, /*fresh=*/true))
    return fail("startEtcd()");
  EtcdGuard guard(etcd);
  const std::string etcdU = etcdUrl(etcd);
  const std::string svc =
      "WatchTestSvc." +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());

  RpcClientConfig cfg;
  cfg.max_retries = 2;
  cfg.timeout_ms = 2000;

  // ============ 场景 1 + 2：存量 client 的 Add/Remove 路由感知 ============
  {
    const uint16_t portA = testutil::pickFreePort();
    const uint16_t portB = testutil::pickFreePort();
    if (portA == 0 || portB == 0)
      return fail("pickFreePort()");
    Node nodeA(svc, "A", portA);
    Node nodeB(svc, "B", portB);
    nodeA.start();
    nodeB.start();

    EtcdClient w(etcdU);
    const std::string kA = "/myrpc/services/" + svc + "/" + nodeA.addr;
    const std::string kB = "/myrpc/services/" + svc + "/" + nodeB.addr;

    if (!putRetry(w, kA, nodeA.addr))
      return fail("put A");
    {
      auto rr = std::make_shared<RoundRobinBalancer>();
      LbRpcClient client(etcdU, svc, rr, cfg);
      if (!waitUntilHit(client, "A", 5000))
        return fail("场景1: client 未感知初始节点 A");

      if (!putRetry(w, kB, nodeB.addr))
        return fail("put B");
      if (!waitUntilHit(client, "B", 5000))
        return fail("场景1: client 未通过 watch Add 感知新节点 B");

      if (!w.del(kB))
        return fail("del B");
      if (!waitUntilOnly(client, "A", "B", 5000))
        return fail("场景2: client 未通过 watch Remove 移除节点 B");
    }

    nodeA.stop();
    nodeB.stop();
    w.del(kA);
  }

  // ============ 场景 A：pkill -> 探活失败 -> 空数据重启 -> 强制全量恢复 ============
  {
    const std::string prefix = "/watch-kill-" + std::to_string(::getpid()) + "/";
    Collector col;
    EtcdClient c(fastCfg(etcdU, /*disconnectFullMs=*/500)); // 阈值很小
    c.watch(prefix, [&](const EtcdWatchEvent &e) { col.push(e); });

    if (!col.waitCount(EtcdWatchEvent::Type::Resync, 1, 5000))
      return fail("场景A: 未收到初始 Resync");
    if (!c.put(prefix + "k", "v"))
      return fail("场景A: put k");
    if (!col.waitCount(EtcdWatchEvent::Type::Put, 1, 5000))
      return fail("场景A: 未收到 Put(k)");

    killEtcd(etcd);                                 // 宕机
    std::this_thread::sleep_for(std::chrono::milliseconds(1500)); // 断连 > 阈值
    if (!startEtcd(etcd, /*fresh=*/true))            // 同端口、空数据
      return fail("场景A: restart etcd");

    // 探活恢复后，因断连超过阈值 -> 强制全量 ls（第二条 Resync）
    if (!col.waitCount(EtcdWatchEvent::Type::Resync, 2, 15000))
      return fail("场景A: 断连恢复后未强制全量（无新 Resync）");
  }

  // ============ 场景 B：短暂重启（< 阈值）-> 从 last_revision+1 续传 ============
  {
    const std::string prefix = "/watch-brief-" + std::to_string(::getpid()) + "/";
    Collector col;
    EtcdClient c(fastCfg(etcdU, /*disconnectFullMs=*/60000)); // 阈值很大
    c.watch(prefix, [&](const EtcdWatchEvent &e) { col.push(e); });

    if (!col.waitCount(EtcdWatchEvent::Type::Resync, 1, 5000))
      return fail("场景B: 未收到初始 Resync");
    if (!c.put(prefix + "k", "v"))
      return fail("场景B: put k");
    if (!col.waitCount(EtcdWatchEvent::Type::Put, 1, 5000))
      return fail("场景B: 未收到 Put(k)");

    killEtcd(etcd);
    std::this_thread::sleep_for(std::chrono::milliseconds(800)); // 短暂断连
    if (!startEtcd(etcd, /*fresh=*/false))        // 同数据重启（k 保留）
      return fail("场景B: restart etcd");

    // 断连 < 阈值 -> 续传（不强制全量）：Resync 数保持 1，且 watch 仍能收到新事件
    if (!putRetry(c, prefix + "k2", "v2"))
      return fail("场景B: put k2");
    if (!col.waitCount(EtcdWatchEvent::Type::Put, 2, 15000))
      return fail("场景B: 续传后未收到 Put(k2)");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (col.count(EtcdWatchEvent::Type::Resync) != 1)
      return fail("场景B: 短暂断连被误判为全量（出现了新 Resync）");
  }

  // ============ 场景 C：在线长时间空闲 -> 不触发全量、不误判 ============
  {
    const std::string prefix = "/watch-idle-" + std::to_string(::getpid()) + "/";
    Collector col;
    EtcdClient c(fastCfg(etcdU, /*disconnectFullMs=*/500)); // 阈值很小，若误判会触发
    c.watch(prefix, [&](const EtcdWatchEvent &e) { col.push(e); });

    if (!col.waitCount(EtcdWatchEvent::Type::Resync, 1, 5000))
      return fail("场景C: 未收到初始 Resync");

    std::this_thread::sleep_for(std::chrono::milliseconds(2000)); // 空闲 > 阈值
    if (col.count(EtcdWatchEvent::Type::Resync) != 1)
      return fail("场景C: 空闲被误判为隔离，触发了全量");

    if (!c.put(prefix + "k", "v"))
      return fail("场景C: put k");
    if (!col.waitCount(EtcdWatchEvent::Type::Put, 1, 5000))
      return fail("场景C: 空闲后 watch 失效，未收到 Put");
  }

  // ============ 场景 D：快速重启（gap < isolate_ms）-> 续传恢复，不卡死 ============
  {
    const std::string prefix = "/watch-fast-" + std::to_string(::getpid()) + "/";
    Collector col;
    // isolate_ms 拉大到 10s，让 2s 断连远小于它。若恢复通知仍被「gap > isolate_ms」
    // 卡住（老逻辑），watch 就会永久卡死，下面 waitCount(Put,2) 必超时。
    EtcdConfig cfgD = fastCfg(etcdU, /*disconnectFullMs=*/60000);
    cfgD.watch_probe_isolate_ms = 10000;
    EtcdClient c(cfgD);
    c.watch(prefix, [&](const EtcdWatchEvent &e) { col.push(e); });

    if (!col.waitCount(EtcdWatchEvent::Type::Resync, 1, 5000))
      return fail("场景D: 未收到初始 Resync");
    if (!c.put(prefix + "k", "v"))
      return fail("场景D: put k");
    if (!col.waitCount(EtcdWatchEvent::Type::Put, 1, 5000))
      return fail("场景D: 未收到 Put(k)");

    killEtcd(etcd);
    std::this_thread::sleep_for(std::chrono::milliseconds(2000)); // gap < isolate
    if (!startEtcd(etcd, /*fresh=*/false)) // 同数据重启（k 保留）
      return fail("场景D: restart etcd");

    // gap < isolate：老逻辑不 notify 会卡死 watch；新逻辑靠 watchDisconnected_ 放行。
    // 必须仍能收到新事件（续传），且不触发全量（Resync 仍为 1）。
    if (!putRetry(c, prefix + "k2", "v2"))
      return fail("场景D: put k2");
    if (!col.waitCount(EtcdWatchEvent::Type::Put, 2, 15000))
      return fail("场景D: 快速重启后 watch 卡死，未收到 Put(k2)");
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (col.count(EtcdWatchEvent::Type::Resync) != 1)
      return fail("场景D: 快速重启被误判为全量（出现了新 Resync）");
  }

  std::printf("PASSED\n");
  return 0;
}
