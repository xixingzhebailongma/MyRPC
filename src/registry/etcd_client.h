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

  // TLS（留空 = 不启用）
  std::string ca;
  std::string cert;
  std::string privkey;
  std::string target_name_override;

  // 认证（留空 = 不启用）
  std::string username;
  std::string password;
  int auth_token_ttl = 300;

  // 幂等重试
  int max_retries = 3;     // 首次之外最多重试次数
  int retry_base_ms = 100; // 指数退避基数
  int retry_max_ms = 2000; // 退避上限

  // 健康监控
  int health_check_interval_ms = 5000;
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
  // 监听前缀变化；底层 watch 流断开后自动重连。每次（重）连接建立后都会回调一次
  // on_change，用于让上层全量重拉、补齐断连期间可能丢失的事件。
  bool watch(const std::string &prefix, std::function<void()> on_change);
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
  std::unique_ptr<etcd::SyncClient> client_;
  EtcdConfig cfg_;

  std::mutex mutex_; // 保护 leases_ / watcher_
  std::unordered_map<std::string, std::shared_ptr<LeaseEntry>> leases_;

  std::string watchPrefix_;
  std::function<void()> watchCallback_;
  std::shared_ptr<etcd::Watcher> watcher_;
  std::thread watchThread_;
  std::atomic<bool> watchRunning_{false};

  std::atomic<EtcdHealth> health_{EtcdHealth::Disconnected};
  std::function<void(EtcdHealth, EtcdHealth)> healthCb_;
  std::mutex healthMutex_;
  std::condition_variable healthCv_;
  std::thread healthThread_;
  std::atomic<bool> healthRunning_{false};
};
