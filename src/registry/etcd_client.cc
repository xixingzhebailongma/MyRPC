#include "etcd_client.h"

#include "Logger.h"

#include <etcd/KeepAlive.hpp>
#include <etcd/Response.hpp>
#include <etcd/SyncClient.hpp>
#include <etcd/Watcher.hpp>

#include <algorithm>
#include <exception>
namespace {

// 错误分类：只有"网络不可用 / gRPC 超时"这类瞬时错误才值得重试。
// 业务性错误（KEY_NOT_FOUND、PERMISSION_DENIED 等）重试无意义，直接返回失败。
bool isRetriableError(const etcd::Response &resp) {
  return resp.is_network_unavailable() || resp.is_grpc_timeout();
}

// 指数退避：base * 2^attempt，封顶 max_ms
void sleepBackoff(int attempt, int base_ms, int max_ms) {
  int64_t ms = base_ms;
  for (int i = 0; i < attempt && ms < max_ms; ++i)
    ms *= 2;
  if (ms > max_ms)
    ms = max_ms;
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}
// 重试执行一个同步 etcd 操作，成功或不可重试失败时返回最后一次 Response。
//
// idempotent=false 时只执行一次（不重试）。用于 leasegrant 这类「创建语义」的
// 非幂等操作：入参里没有目标标识，服务端每收到一次就新建一个对象、分配一个新
// id。 而 gRPC 超时属于「结果不确定」而非「确定失败」——请求可能已经 raft 提交并
// fsync 落盘，只是响应丢了。此时重试就会凭空多造出一个没挂 key、没人续约、
// 客户端连 id 都不知道因而无法 revoke 的孤儿 lease。
//
// 注：根治办法是客户端自己生成 lease id 传给服务端（etcd 的 LeaseGrantRequest
// 支持 ID 字段，传同一个 id 重试即幂等），但 SyncClient::leasegrant(int ttl)
// 没有暴露该参数。因此退而求其次：不重试，把重试上移给调用方的「整轮
// grant+put」循环（见 leaseLoop）。
template <typename Fn>
etcd::Response retryOp(const EtcdConfig &cfg, Fn &&fn, const char *name,
                       bool idempotent = true) {
  etcd::Response resp;
  for (int attempt = 0;; ++attempt) {
    resp = fn();
    if (resp.is_ok())
      return resp;
    LOG_WARN("EtcdClient::%s attempt %d failed: %s", name, attempt,
             resp.error_message().c_str());
    if (!idempotent || attempt >= cfg.max_retries || !isRetriableError(resp))
      break;
    sleepBackoff(attempt, cfg.retry_base_ms, cfg.retry_max_ms);
  }
  return resp;
}

} // namespace
EtcdClient::EtcdClient(const std::string &endpoints) {
  EtcdConfig cfg;
  cfg.endpoints = endpoints;
  init(cfg);
}

EtcdClient::EtcdClient(const EtcdConfig &config) { init(config); }

void EtcdClient::init(const EtcdConfig &cfg) {
  cfg_ = cfg;
  // 配置自校验：隔离确认窗口必须 <= 全量阈值。若配反了，gap 落在 (full, isolate]
  // 时恢复路径会整体跳过，watchLoop 永久等待；且「续传」档位会被吃空，每次隔离都
  // 强制全量。这里直接夹紧到 full，保证隔离窗口不越过全量阈值。
  if (cfg_.watch_probe_isolate_ms > cfg_.watch_disconnect_full_ms) {
    LOG_WARN("EtcdClient: watch_probe_isolate_ms(%d) > "
             "watch_disconnect_full_ms(%d), clamping isolate to full",
             cfg_.watch_probe_isolate_ms, cfg_.watch_disconnect_full_ms);
    cfg_.watch_probe_isolate_ms = cfg_.watch_disconnect_full_ms;
  }
  const std::string &url = cfg_.endpoints;
  const std::string &lb = cfg_.load_balancer;
  {
    std::lock_guard<std::mutex> lk(clientMutex_);
    client_ = std::make_shared<etcd::SyncClient>(url, lb);
  }

  // 启动后台健康监控线程与持久探测线程
  healthRunning_ = true;
  healthThread_ = std::thread(&EtcdClient::healthLoop, this);
  probeRunning_ = true;
  probeThread_ = std::thread(&EtcdClient::probeLoop, this);
  LOG_INFO("EtcdClient: initialized (endpoints=%s, lb=%s)", url.c_str(),
           lb.c_str());
}

EtcdClient::~EtcdClient() {
  stopWatch();

  // 停掉所有托管 lease 线程（先收集 key，避免在遍历中修改 map）
  std::vector<std::string> keys;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    keys.reserve(leases_.size());
    for (auto &kv : leases_)
      keys.push_back(kv.first);
  }
  for (const auto &k : keys)
    unregister(k);

  // 先停探测线程：置 probeRunning_=false 并 notify，能立刻解开正在 probeHealthy 里
  // 等结果的健康线程（其 wait 谓词含 !probeRunning_），也让探测线程退出。
  {
    std::lock_guard<std::mutex> lk(probeMutex_);
    probeRunning_ = false;
  }
  probeCv_.notify_all();

  // 停健康线程（与谓词同锁，避免丢唤醒）
  {
    std::lock_guard<std::mutex> lk(healthMutex_);
    healthRunning_ = false;
    healthCv_.notify_all();
  }
  if (healthThread_.joinable())
    healthThread_.join();

  if (probeThread_.joinable())
    probeThread_.join();
}

// ---------- KV 高层 API（带重试） ----------

bool EtcdClient::put(const std::string &key, const std::string &value) {
  auto c = clientSnapshot();
  if (!c)
    return false;
  etcd::Response resp = retryOp(
      cfg_, [&] { return c->set(key, value); }, "put");
  return resp.is_ok();
}

bool EtcdClient::putWithLease(const std::string &key, const std::string &value,
                              int64_t leaseId) {
  auto c = clientSnapshot();
  if (!c)
    return false;
  etcd::Response resp = retryOp(
      cfg_, [&] { return c->set(key, value, leaseId); }, "putWithLease");
  return resp.is_ok();
}

std::optional<std::vector<std::pair<std::string, std::string>>>
EtcdClient::ls(const std::string &prefix) {
  auto snap = lsWithRevision(prefix);
  if (!snap)
    return std::nullopt;
  return std::move(snap->kvs);
}

std::optional<EtcdClient::LsResult>
EtcdClient::lsWithRevision(const std::string &prefix) {
  auto c = clientSnapshot();
  if (!c)
    return std::nullopt;
  etcd::Response resp = retryOp(
      cfg_, [&] { return c->ls(prefix); }, "lsWithRevision");
  if (!resp.is_ok())
    return std::nullopt;
  LsResult r;
  r.revision = resp.index(); // etcd header revision
  r.kvs.reserve(resp.keys().size());
  for (size_t i = 0; i < resp.keys().size(); ++i)
    r.kvs.emplace_back(resp.key(i), resp.value(i).as_string());
  return r;
}

bool EtcdClient::del(const std::string &key) {
  auto c = clientSnapshot();
  if (!c)
    return false;
  etcd::Response resp = retryOp(
      cfg_, [&] { return c->rm(key); }, "del");
  return resp.is_ok();
}

int64_t EtcdClient::grantLease(int64_t ttl) {
  // 非幂等：超时可能意味着服务端其实已经建好了 lease，重试会导致孤儿 lease
  // 泄漏。 只试一次，失败交给上层重新走整轮 grant + put。
  auto c = clientSnapshot();
  if (!c)
    return -1;
  etcd::Response resp = retryOp(
      cfg_, [&] { return c->leasegrant(ttl); }, "grantLease",
      /*idempotent=*/false);
  if (!resp.is_ok())
    return -1;
  return resp.value().lease();
}

bool EtcdClient::revokeLease(int64_t leaseId) {
  auto c = clientSnapshot();
  if (!c)
    return false;
  etcd::Response resp = retryOp(
      cfg_, [&] { return c->leaserevoke(leaseId); }, "revokeLease");
  return resp.is_ok();
}

// ---------- 托管注册 ----------
bool EtcdClient::registerWithLease(const std::string &key,
                                   const std::string &value, int64_t ttl) {
  // 首轮同步注册（不持锁，内部可能重试睡眠）
  int64_t leaseId = grantLease(ttl);
  if (leaseId < 0)
    return false;
  if (!putWithLease(key, value, leaseId)) {
    revokeLease(leaseId);
    return false;
  }

  auto e = std::make_shared<LeaseEntry>();
  e->key = key;
  e->value = value;
  e->ttl = ttl;
  e->leaseId = leaseId;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (leases_.count(key)) { // 已存在，回滚
      revokeLease(leaseId);
      return false;
    }
    leases_[key] = e;
  }
  e->thread = std::thread(&EtcdClient::leaseLoop, this, e);
  LOG_INFO("EtcdClient: registered %s (lease=%ld, auto keep-alive)",
           key.c_str(), leaseId);
  return true;
}

void EtcdClient::unregister(const std::string &key) {
  std::shared_ptr<LeaseEntry> e;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto it = leases_.find(key);
    if (it == leases_.end())
      return;
    e = it->second;
    leases_.erase(it);
  }
  {
    std::lock_guard<std::mutex> lk(e->cvMutex);
    e->active = false;
    e->cv.notify_all(); // 唤醒可能在睡眠的 leaseLoop
  }
  if (e->thread.joinable())
    e->thread.join();
  // leaseLoop 退出时已撤销 lease；这里兜底再撤销一次（幂等，失败无害）
  if (e->leaseId >= 0)
    revokeLease(e->leaseId);
}
void EtcdClient::leaseLoop(std::shared_ptr<LeaseEntry> e) {
  int attempt = 0;
  while (e->active) {
    // 若 leaseId 失效（恢复阶段），重新 grant + put
    if (e->leaseId < 0) {
      e->leaseId = grantLease(e->ttl);
      if (e->leaseId < 0) {
        sleepBackoff(attempt++, cfg_.retry_base_ms, cfg_.retry_max_ms);
        continue;
      }
      if (!putWithLease(e->key, e->value, e->leaseId)) {
        revokeLease(e->leaseId);
        e->leaseId = -1;
        sleepBackoff(attempt++, cfg_.retry_base_ms, cfg_.retry_max_ms);
        continue;
      }
    }

    // 建立自动续约对象
    e->keepalive = keepAliveLeaseInternal(e->leaseId, e->ttl);
    if (!e->keepalive) {
      sleepBackoff(attempt++, cfg_.retry_base_ms, cfg_.retry_max_ms);
      continue;
    }
    attempt = 0;

    // 监控续约状态；连续 3 次 Check 失败视为永久失败，走恢复
    int fail = 0;
    while (e->active) {
      std::unique_lock<std::mutex> lk(e->cvMutex);
      e->cv.wait_for(lk, std::chrono::seconds(std::max<int64_t>(1, e->ttl / 3)),
                     [&] { return !e->active; });
      if (!e->active)
        break;
      try {
        e->keepalive->Check();
        fail = 0;
      } catch (const std::exception &ex) {
        LOG_ERROR("EtcdClient: keepalive check #%d failed for lease %ld: %s",
                  ++fail, e->leaseId, ex.what());
        if (fail >= 3)
          break;
      }
    }

    // 清理旧 lease，若仍 active 则回到循环顶部重新注册（恢复）
    e->keepalive.reset();
    if (e->leaseId >= 0) {
      revokeLease(e->leaseId);
      e->leaseId = -1;
    }
    if (e->active)
      LOG_WARN("EtcdClient: lease for %s lost, recovering...", e->key.c_str());
  }
}
std::shared_ptr<etcd::KeepAlive>
EtcdClient::keepAliveLeaseInternal(int64_t leaseId, int64_t ttl) {
  auto c = clientSnapshot();
  if (!c)
    return nullptr;
  try {
    return std::make_shared<etcd::KeepAlive>(*c, static_cast<int>(ttl),
                                             leaseId);
  } catch (const std::exception &e) {
    LOG_ERROR("EtcdClient: keepAliveLease failed for lease %ld: %s", leaseId,
              e.what());
    return nullptr;
  }
}

// ---------- 可恢复 Watch ----------

bool EtcdClient::watch(const std::string &prefix,
                       std::function<void(const EtcdWatchEvent &)> on_event) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (watchRunning_) {
    LOG_WARN("EtcdClient::watch: watcher already running");
    return false;
  }
  watchPrefix_ = prefix;
  watchCallback_ = std::move(on_event);
  watchRunning_ = true;
  watchThread_ = std::thread(&EtcdClient::watchLoop, this);
  return true;
}

void EtcdClient::advanceRevision(int64_t revision) {
  int64_t cur = lastRevision_.load(std::memory_order_relaxed);
  while (revision > cur && !lastRevision_.compare_exchange_weak(
                               cur, revision, std::memory_order_relaxed,
                               std::memory_order_relaxed))
    ;
}

std::shared_ptr<etcd::SyncClient> EtcdClient::clientSnapshot() {
  std::lock_guard<std::mutex> lk(clientMutex_);
  return client_;
}

void EtcdClient::watchLoop() {
  int attempt = 0;

  while (watchRunning_) {
    // ---- 决定本轮是全量还是增量续传 ----
    // 是否全量由 lastRevision_ 决定：0 = 首次/compacted/探活判定隔离（需全量），
    // 否则从 lastRevision_+1 增量续传。探活线程会在「隔离确认」或「恢复且断连过久」
    // 时把 lastRevision_ 置 0，并 cancel 当前 watcher 打断 Wait()。
    bool fullSync = (lastRevision_.load(std::memory_order_relaxed) == 0);

    // ---- 全量：ls + 记录 header revision，再投递 Resync ----
    if (fullSync) {
      auto snap = lsWithRevision(watchPrefix_);
      if (!snap) {
        sleepBackoff(attempt++, cfg_.retry_base_ms, cfg_.retry_max_ms);
        continue;
      }
      lastRevision_.store(snap->revision, std::memory_order_relaxed);
      if (watchCallback_) {
        EtcdWatchEvent e;
        e.type = EtcdWatchEvent::Type::Resync;
        e.revision = snap->revision;
        e.snapshot = std::move(snap->kvs);
        watchCallback_(e);
      }
    }

    // ---- 建立 watcher，从 lastRevision+1 增量续传 ----
    std::shared_ptr<etcd::Watcher> w;
    try {
      int64_t from = lastRevision_.load(std::memory_order_relaxed) + 1;
      std::shared_ptr<etcd::SyncClient> c = clientSnapshot();
      if (!c) {
        LOG_WARN("EtcdClient: client not ready, retrying watch");
        sleepBackoff(attempt++, cfg_.retry_base_ms, cfg_.retry_max_ms);
        continue;
      }
      w = std::make_shared<etcd::Watcher>(
          *c, watchPrefix_, from,
          [this](etcd::Response resp) {
            // compacted：watch 因 revision 被 compact 而取消。正常事件/普通错误时
            // compact_revision 为 0，只有被 compact 时才 > 0，故用 > 0 判断。
            if (resp.compact_revision() > 0) {
              LOG_WARN("EtcdClient: watch %s compacted (compact_rev=%lld), "
                       "forcing full sync",
                       watchPrefix_.c_str(),
                       (long long)resp.compact_revision());
              lastRevision_.store(0, std::memory_order_relaxed);
              return;
            }
            if (!resp.is_ok()) {
              LOG_WARN("EtcdClient: watch event error: %s",
                       resp.error_message().c_str());
              return;
            }
            // 收到事件说明 etcd 仍在服务，清掉「可能隔离」的断连起点（避免探活
            // 偶发超时被误判为隔离）
            disconnectedSinceNs_.store(0, std::memory_order_relaxed);
            for (const auto &ev : resp.events()) {
              EtcdWatchEvent e;
              e.type = ev.event_type() == etcd::Event::EventType::PUT
                           ? EtcdWatchEvent::Type::Put
                           : EtcdWatchEvent::Type::Delete;
              const auto &kv = ev.kv();
              e.key = kv.key();
              if (e.type == EtcdWatchEvent::Type::Put)
                e.value = kv.as_string();
              e.revision = kv.modified_index();
              advanceRevision(e.revision);
              if (watchCallback_)
                watchCallback_(e);
            }
          },
          /*recursive=*/true);
      {
        std::lock_guard<std::mutex> lk(mutex_);
        if (!watchRunning_) {
          // stopWatch 已经跑过（它看不到刚构造、还没赋值的 watcher）。
          // 若继续 Wait() 就会永久阻塞，导致 stopWatch 里的 join 卡死。
          w->Cancel();
          break;
        }
        watcher_ = w;
      }
      attempt = 0;
      LOG_INFO("EtcdClient: watching prefix %s (from rev %lld)",
               watchPrefix_.c_str(), (long long)from);

      // 阻塞直到 watch 结束：true=正常 Cancel（stopWatch 或探活打断），false=服务端断开
      bool cancelled = w->Wait();
      {
        std::lock_guard<std::mutex> lk(mutex_);
        if (watcher_ == w)
          watcher_.reset();
      }
      // 只有 stopWatch 才退出（它先置 watchRunning_=false 再 Cancel）；探活打断
      // 或服务端断开都继续重连。
      if (!watchRunning_)
        break;
      if (!cancelled) {
        // 服务端断开：resume（从 last_revision+1 重建 watcher）在 etcd 宕机时会阻塞在
        // wait_for_ready，且 Cancel() 无法打断。这里改为等待探活线程的恢复判定
        // （恢复时会重建 client_ 并置 watchDisconnected_=false 放行），期间不碰 etcd。
        LOG_WARN("EtcdClient: watch on %s disconnected, waiting for recovery...",
                 watchPrefix_.c_str());
        // 声明「已断开、正在等恢复」，并补写断连起点（若探活线程尚未记录），确保
        // 探活恢复路径能看到这次断连——快速重启（gap < isolate）时也不会漏。
        watchDisconnected_.store(true, std::memory_order_relaxed);
        int64_t zero = 0;
        int64_t now =
            std::chrono::steady_clock::now().time_since_epoch().count();
        disconnectedSinceNs_.compare_exchange_strong(zero, now,
                                                     std::memory_order_relaxed,
                                                     std::memory_order_relaxed);
        std::unique_lock<std::mutex> lk(mutex_);
        watchCv_.wait(lk, [this] {
          return !watchRunning_ ||
                 !watchDisconnected_.load(std::memory_order_relaxed);
        });
        if (!watchRunning_)
          break;
      } else {
        LOG_INFO("EtcdClient: watch %s interrupted, reconnecting...",
                 watchPrefix_.c_str());
      }
      sleepBackoff(attempt++, cfg_.retry_base_ms, cfg_.retry_max_ms);
    } catch (const std::exception &e) {
      LOG_ERROR("EtcdClient: watch loop error: %s", e.what());
      {
        std::lock_guard<std::mutex> lk(mutex_);
        if (watcher_ == w)
          watcher_.reset();
      }
      if (!watchRunning_)
        break;
      sleepBackoff(attempt++, cfg_.retry_base_ms, cfg_.retry_max_ms);
    }
  }
}

void EtcdClient::stopWatch() {
  std::shared_ptr<etcd::Watcher> w;
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (!watchRunning_)
      return;
    watchRunning_ = false;
    w = watcher_;
  }
  if (w)
    w->Cancel(); // 触发 Wait() 返回 true，watchLoop 退出
  if (watchThread_.joinable())
    watchThread_.join();
  watchCallback_ = nullptr;
}

// ---------- 健康监控 + watch 探活 ----------

void EtcdClient::probeLoop() {
  while (true) {
    int64_t seq;
    std::string prefix;
    {
      std::unique_lock<std::mutex> lk(probeMutex_);
      probeCv_.wait(lk, [this] {
        return probeRequestSeq_ > probeDoneSeq_ ||
               !probeRunning_.load(std::memory_order_relaxed);
      });
      if (!probeRunning_.load(std::memory_order_relaxed))
        return;
      seq = probeRequestSeq_;
      prefix = probePrefix_; // 拷贝，避免后面无锁读成员
    }
    // 一次性 SyncClient + 重试做 limit=1 的轻量 Range：
    // - 一次性客户端：每次探活都是全新 channel，避免长生命周期 client 在 etcd 重启后
    //   channel 退避导致长时间阻塞；
    // - 重试：gRPC channel 懒连接，首次 RPC 常因「尚未连上后端」而失败，重试让连接建立。
    bool ok = false;
    try {
      etcd::SyncClient probe(cfg_.endpoints, cfg_.load_balancer);
      probe.set_grpc_timeout(
          std::chrono::milliseconds(cfg_.watch_probe_timeout_ms));
      for (int i = 0; i < 3 && !ok; ++i) {
        ok = probe.ls(prefix, 1).is_ok();
        if (!ok)
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    } catch (const std::exception &) {
      ok = false;
    }
    {
      std::lock_guard<std::mutex> lk(probeMutex_);
      probeResult_ = ok;
      probeDoneSeq_ = seq;
    }
    probeCv_.notify_all();
  }
}

bool EtcdClient::probeHealthy() {
  int64_t seq;
  {
    std::lock_guard<std::mutex> lk(probeMutex_);
    probePrefix_ = watchPrefix_;
    seq = ++probeRequestSeq_;
  }
  probeCv_.notify_all();
  // wait_for 硬超时：兜底底层库不按 deadline 返回的场景。探测线程若仍在阻塞在旧的
  // 探测上，本次就按失败返回，但线程只有一个，不会累积。
  std::unique_lock<std::mutex> lk(probeMutex_);
  if (!probeCv_.wait_for(
          lk, std::chrono::milliseconds(cfg_.watch_probe_timeout_ms + 1500),
          [this, seq] {
            return probeDoneSeq_ >= seq ||
                   !probeRunning_.load(std::memory_order_relaxed);
          }))
    return false;
  return probeResult_;
}

void EtcdClient::healthLoop() {
  while (healthRunning_) {
    {
      std::unique_lock<std::mutex> lk(healthMutex_);
      healthCv_.wait_for(
          lk, std::chrono::milliseconds(cfg_.health_check_interval_ms),
          [&] { return !healthRunning_; });
      if (!healthRunning_)
        break;
    }

    // 有 watch 时：用带硬超时的轻量 Range 探活。没 watch 时退回 head() 维持健康状态。
    if (!watchRunning_.load(std::memory_order_relaxed)) {
      auto c = clientSnapshot();
      if (!c) {
        setHealth(EtcdHealth::Disconnected);
        continue;
      }
      etcd::Response resp = c->head();
      if (resp.is_ok())
        setHealth(EtcdHealth::Connected);
      else
        setHealth(health_.load() == EtcdHealth::Connected
                      ? EtcdHealth::Degraded
                      : EtcdHealth::Disconnected);
      continue;
    }

    bool healthy = probeHealthy();
    if (healthy) {
      // 探活成功：若之前处于断连，走「恢复」逻辑
      int64_t ds = disconnectedSinceNs_.exchange(0, std::memory_order_relaxed);
      if (ds != 0) {
        std::chrono::steady_clock::time_point since{
            std::chrono::steady_clock::duration(ds)};
        auto gap = std::chrono::steady_clock::now() - since;
        // 只有 watch 流确已断开，或断连超过隔离确认窗口，才需要重建 client_。
        // 前者无论断连多短都必须放行 watchLoop 续传，否则「快速重启」会把 watch
        // 卡死在等待恢复上。
        if (watchDisconnected_.load(std::memory_order_relaxed)) {
          if (gap > std::chrono::milliseconds(cfg_.watch_disconnect_full_ms)) {
            LOG_WARN("EtcdClient: watch %s was isolated for %lld ms (> %d ms), "
                     "forcing full sync",
                     watchPrefix_.c_str(),
                     (long long)std::chrono::duration_cast<
                         std::chrono::milliseconds>(gap)
                         .count(),
                     cfg_.watch_disconnect_full_ms);
            lastRevision_.store(0, std::memory_order_relaxed);
          }
          // etcd 重启后旧 channel 已陈旧：重建 client_，让后续全量 ls / 新 watch 用新连接。
          {
            std::lock_guard<std::mutex> lk(clientMutex_);
            client_ = std::make_shared<etcd::SyncClient>(cfg_.endpoints,
                                                         cfg_.load_balancer);
          }
          // 预热：新 channel 懒连接，首次 RPC 常失败，重试让连接建立
          auto c = clientSnapshot();
          if (c) {
            for (int i = 0; i < 3; ++i) {
              if (c->ls(watchPrefix_, 1).is_ok())
                break;
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
          }
          // 放行 watchLoop（它已在 Wait() 返回 false 后等在 watchCv_ 上）
          {
            std::lock_guard<std::mutex> lk(mutex_);
            watchDisconnected_.store(false, std::memory_order_relaxed);
          }
          watchCv_.notify_all();
        } else if (gap > std::chrono::milliseconds(cfg_.watch_probe_isolate_ms)) {
          // watch 未断但探测长期失败（罕见：探活与 watch 走了不同网络路径）。仅重建
          // client_，避免后续 watch 续传时阻塞在陈旧的 channel 上；不 notify（watchLoop
          // 没在等）。
          std::lock_guard<std::mutex> lk(clientMutex_);
          client_ = std::make_shared<etcd::SyncClient>(cfg_.endpoints,
                                                       cfg_.load_balancer);
        }
      }
      setHealth(EtcdHealth::Connected);
    } else {
      // 探活失败：只记录断连起点（仅第一次）。是否全量、是否打断 watch，都等到
      // 恢复时（或收到事件）再决定，避免误伤。
      int64_t zero = 0;
      int64_t now =
          std::chrono::steady_clock::now().time_since_epoch().count();
      disconnectedSinceNs_.compare_exchange_strong(zero, now,
                                                   std::memory_order_relaxed,
                                                   std::memory_order_relaxed);
      EtcdHealth prev = health_.load();
      setHealth(prev == EtcdHealth::Connected ? EtcdHealth::Degraded
                                              : EtcdHealth::Disconnected);
    }
  }
}

void EtcdClient::setHealth(EtcdHealth next) {
  EtcdHealth prev = health_.exchange(next);
  if (prev == next)
    return;
  std::function<void(EtcdHealth, EtcdHealth)> cb;
  {
    std::lock_guard<std::mutex> lk(healthMutex_);
    cb = healthCb_;
  }
  if (cb)
    cb(prev, next);
}

void EtcdClient::setHealthCallback(
    std::function<void(EtcdHealth, EtcdHealth)> cb) {
  std::lock_guard<std::mutex> lk(healthMutex_);
  healthCb_ = std::move(cb);
}
