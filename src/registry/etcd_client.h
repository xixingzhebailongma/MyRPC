#pragma once
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
//前向声明，避免在头文件中暴露etcd库的头文件
namespace etcd {
class SyncClient;
class KeepAlive;
class Watcher;
} // namespace etcd

// etcd 连接与行为配置
struct EtcdConfig {
  std::string endpoints = "http://127.0.0.1:2379";
  std::string load_balancer =
      "round_robin"; // round_robin|pick_first|grpclb|xds

  // 幂等重试
  int max_retries = 3;     // 首次之外最多重试次数
  int retry_base_ms = 100; // 指数退避基数
  int retry_max_ms = 2000; // 退避上限

  // 健康监控（同时作为 watch 探活间隔）
  int health_check_interval_ms = 5000;

  // —— 断连恢复的两个行为分界点（须满足 isolate_ms <= full_ms，init() 会夹紧）——
  // 设一次断连时长 gap = 首次探活失败 → 首次探活恢复：
  //   gap <= isolate_ms 且 watch 流未断：未达隔离，不重建 client_（watch 维持原样）；
  //   gap <= isolate_ms 但 watch 流已断：重建 client_ 并续传（防快速重启卡死）；
  //   isolate_ms < gap <= full_ms：      重建 client_ 并续传（确认隔离但可续传）；
  //   gap > full_ms：                    重建 client_ 并强制全量 ls（Resync）。
  // 即：watch 流一旦断开，恢复时都重建 client_ 并续传，与 gap 是否越过 isolate_ms
  // 无关；isolate_ms 只决定「watch 未断」时要不要重建。
  int watch_disconnect_full_ms = 60000; // 断连超过该时长才强制全量
  int watch_probe_isolate_ms = 10000;   // 隔离确认窗口

  // watch 探活：用带 deadline 的轻量 Range 探测连通性（head()/ls() 无 deadline，
  // 服务端宕机后仍返回 ok，不可信）。
  int watch_probe_timeout_ms = 2000; // Range 超时
};

// 一次 watch 推送：单条 PUT/DELETE，或一条携带全量快照的 Resync。
struct EtcdWatchEvent {
  enum class Type { Put, Delete, Resync };
  Type type = Type::Put;
  std::string key;    // Put/Delete 的完整 key
  std::string value;  // Put 的 value（Delete/Resync 为空）
  int64_t revision = 0; // 事件对应的 etcd revision
  // Resync 专用：全量快照（key -> value）
  std::vector<std::pair<std::string, std::string>> snapshot;
};

enum class EtcdHealth {
  Disconnected, // 尚未连通 / 已断开
  Connected,    // 探活成功
  Degraded      // 曾连通后出现异常
};

class EtcdClient {
public:
  // endpoints格式:"https://127.0.0.1:2379"
  // 兼容旧构造（等价于只传 endpoints 的 config）
  explicit EtcdClient(const std::string &endpoints);
  explicit EtcdClient(const EtcdConfig &config);
  ~EtcdClient();

  //禁止拷贝(持有unique_ptr)
  EtcdClient(const EtcdClient &) = delete;
  EtcdClient &operator=(const EtcdClient &) = delete;
  // ---------- 带重试的 KV 高层 API ----------
  //写入key-value
  bool put(const std::string &key, const std::string &value);

  //带lease写入(用于服务注册,lease过期后key自动删除)
  bool putWithLease(const std::string &key, const std::string &value,
                    int64_t leaseId);

  //前缀查询,返回{key,value}列表
  // nullopt = 查询失败（etcd 不可用等）；空 vector = 查询成功但该前缀下无 key
  std::optional<std::vector<std::pair<std::string, std::string>>>
  ls(const std::string &prefix);

  //删除key
  bool del(const std::string &key);

  //创建lease,返回leaseID,失败返回-1
  int64_t grantLease(int64_t ttl);

  //撤销lease
  bool revokeLease(int64_t leaseId);
  // ---------- 托管注册（lease 自动续约 + 失败自动恢复）----------
  // 注册一个带 lease 的 key：grant lease + put，随后启动后台线程自动续约；
  // 续约失败会自动撤销旧 lease、重新 grant、重新 put（带退避），无需业务干预。
  //
  // 返回值只表示「首轮是否成功」。即使返回 false，后台线程也已经启动并会持续
  // 重试注册，直到成功或调用 unregister()。唯一例外是 key 重复注册：此时
  // 返回 false 且不启动线程。
  bool registerWithLease(const std::string &key, const std::string &value,
                         int64_t ttl);
  // 撤销托管：停止续约线程并撤销 lease（key 随之自动删除）
  void unregister(const std::string &key);

  // ---------- 可恢复 Watch ----------
  // 监听前缀变化；底层 watch 流断开后自动重连。事件以增量（Put/Delete）投递，
  // 首次连接、watch 返回 compacted、或断连超过 watch_disconnect_full_ms 时才投递
  // 一次 Resync（携带全量快照），由上层做全量替换。
  bool watch(const std::string &prefix,
             std::function<void(const EtcdWatchEvent &)> on_event);
  void stopWatch();

  // ---------- 健康监控 ----------
  EtcdHealth health() const { return health_.load(); }
  // 状态变化回调（在后台健康线程中触发，参数为 旧状态 -> 新状态）
  void setHealthCallback(std::function<void(EtcdHealth, EtcdHealth)> cb);


private:
  void init(const EtcdConfig &cfg);

  struct LeaseEntry {
    std::string key;
    std::string value;
    int64_t ttl = 0;
    int64_t leaseId = -1;
    std::shared_ptr<etcd::KeepAlive> keepalive;
    std::atomic<bool> active{true};
    std::thread thread;
    std::mutex cvMutex;
    std::condition_variable cv;
  };

  void leaseLoop(std::shared_ptr<LeaseEntry> e);
  void watchLoop();
  void healthLoop();
  void setHealth(EtcdHealth next);
  std::shared_ptr<etcd::KeepAlive> keepAliveLeaseInternal(int64_t leaseId,
                                                          int64_t ttl);

  // 前缀全量查询并返回 etcd header revision（用于 watch 续传的 last_revision）
  struct LsResult {
    std::vector<std::pair<std::string, std::string>> kvs;
    int64_t revision = 0;
  };
  std::optional<LsResult> lsWithRevision(const std::string &prefix);
  // 单调推进 last_revision_（取 max）
  void advanceRevision(int64_t revision);
  // 取 client_ 的快照（shared_ptr 拷贝，短锁）。所有读方都必须经此拿快照再调用，
  // 这样即便探活线程随后整体重建 client_，旧 client 也会活到本次调用结束，避免
  // use-after-free。
  std::shared_ptr<etcd::SyncClient> clientSnapshot();
  // 连通性探测：投递给单一持久探测线程（cv 唤醒），用一次性 SyncClient 做 limit=1
  // Range，超时返回 false（避免底层库在 etcd 重启选举时阻塞住健康线程）。
  bool probeHealthy();
  void probeLoop();

  // client_ 由探活线程在恢复时整体重建（换新 channel）。读方必须经 clientSnapshot()
  // 拿快照，不能直接解引用。
  std::shared_ptr<etcd::SyncClient> client_;
  std::mutex clientMutex_; // 保护 client_
  EtcdConfig cfg_;

  std::mutex mutex_; // 保护 leases_ / watcher_
  std::unordered_map<std::string, std::shared_ptr<LeaseEntry>> leases_;

  std::string watchPrefix_;
  std::function<void(const EtcdWatchEvent &)> watchCallback_;
  std::shared_ptr<etcd::Watcher> watcher_;
  std::thread watchThread_;
  std::atomic<bool> watchRunning_{false};
  // 0 表示未知/需要全量；否则为已消费到的最大 etcd revision
  std::atomic<int64_t> lastRevision_{0};
  // 断连起点（steady_clock ns，0 = 未断连）。由探活线程在首次探测失败时写入，
  // 探测恢复或收到 watch 事件时清零。watchLoop 检测到流断开时也会补写（若探活
  // 线程尚未记录），确保恢复路径能看到这次断连。
  std::atomic<int64_t> disconnectedSinceNs_{0};
  // watch 流是否已断开且 watchLoop 正在等待恢复。watchLoop 在 Wait() 返回 false 后
  // 置 true 并等在 watchCv_ 上；探活线程判定恢复、重建 client_ 后置 false 放行。
  // 用这个双向握手替代单发信号，避免「快速重启 + 恢复信号丢失」导致 watchLoop 永久等待。
  std::atomic<bool> watchDisconnected_{false};
  std::condition_variable watchCv_; // 配合 mutex_ 使用

  std::atomic<EtcdHealth> health_{EtcdHealth::Disconnected};
  std::function<void(EtcdHealth, EtcdHealth)> healthCb_;
  std::mutex healthMutex_;
  std::condition_variable healthCv_;
  std::thread healthThread_;
  std::atomic<bool> healthRunning_{false};

  // 单一持久探测线程（替代每次探活都 detach 一个线程，避免 etcd 长时间不可用时
  // detach 线程累积）。probeRequestSeq_/probeDoneSeq_ 用单调序号把请求与结果配对，
  // probePrefix_ 记录最近一次请求的 watch 前缀。
  std::thread probeThread_;
  std::mutex probeMutex_;
  std::condition_variable probeCv_;
  std::atomic<bool> probeRunning_{false};
  int64_t probeRequestSeq_ = 0;
  int64_t probeDoneSeq_ = 0;
  bool probeResult_ = false;
  std::string probePrefix_;
};
