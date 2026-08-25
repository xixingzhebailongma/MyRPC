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
  const std::string &url = cfg_.endpoints;
  const std::string &lb = cfg_.load_balancer;
  if (!cfg_.ca.empty()) {
    // TLS：ca/cert/privkey/target_name_override + load_balancer
    client_.reset(new etcd::SyncClient(url, cfg_.ca, cfg_.cert, cfg_.privkey,
                                       cfg_.target_name_override, lb));
  } else if (!cfg_.username.empty()) {
    // 用户名密码认证
    client_.reset(new etcd::SyncClient(url, cfg_.username, cfg_.password,
                                       cfg_.auth_token_ttl, lb));
  } else {
    // 仅 endpoints + 负载均衡策略
    client_.reset(new etcd::SyncClient(url, lb));
  }

  // 启动后台健康监控线程
  healthRunning_ = true;
  healthThread_ = std::thread(&EtcdClient::healthLoop, this);
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

  // 停健康线程（与谓词同锁，避免丢唤醒）
  {
    std::lock_guard<std::mutex> lk(healthMutex_);
    healthRunning_ = false;
    healthCv_.notify_all();
  }
  if (healthThread_.joinable())
    healthThread_.join();
}

// ---------- KV 高层 API（带重试） ----------

bool EtcdClient::put(const std::string &key, const std::string &value) {
  etcd::Response resp = retryOp(
      cfg_, [&] { return client_->set(key, value); }, "put");
  return resp.is_ok();
}

bool EtcdClient::putWithLease(const std::string &key, const std::string &value,
                              int64_t leaseId) {
  etcd::Response resp = retryOp(
      cfg_, [&] { return client_->set(key, value, leaseId); }, "putWithLease");
  return resp.is_ok();
}

std::optional<std::vector<std::pair<std::string, std::string>>>
EtcdClient::ls(const std::string &prefix) {
  etcd::Response resp = retryOp(
      cfg_, [&] { return client_->ls(prefix); }, "ls");
  if (!resp.is_ok())
    return std::nullopt;
  std::vector<std::pair<std::string, std::string>> result;
  result.reserve(resp.keys().size());
  for (size_t i = 0; i < resp.keys().size(); ++i)
    result.emplace_back(resp.key(i), resp.value(i).as_string());
  return result;
}

bool EtcdClient::del(const std::string &key) {
  etcd::Response resp = retryOp(
      cfg_, [&] { return client_->rm(key); }, "del");
  return resp.is_ok();
}

int64_t EtcdClient::grantLease(int64_t ttl) {
  // 非幂等：超时可能意味着服务端其实已经建好了 lease，重试会导致孤儿 lease
  // 泄漏。 只试一次，失败交给上层重新走整轮 grant + put。
  etcd::Response resp = retryOp(
      cfg_, [&] { return client_->leasegrant(ttl); }, "grantLease",
      /*idempotent=*/false);
  if (!resp.is_ok())
    return -1;
  return resp.value().lease();
}

bool EtcdClient::revokeLease(int64_t leaseId) {
  etcd::Response resp = retryOp(
      cfg_, [&] { return client_->leaserevoke(leaseId); }, "revokeLease");
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
  try {
    return std::make_shared<etcd::KeepAlive>(*client_, static_cast<int>(ttl),
                                             leaseId);
  } catch (const std::exception &e) {
    LOG_ERROR("EtcdClient: keepAliveLease failed for lease %ld: %s", leaseId,
              e.what());
    return nullptr;
  }
}

// ---------- 可恢复 Watch ----------

bool EtcdClient::watch(const std::string &prefix,
                       std::function<void()> on_change) {
  std::lock_guard<std::mutex> lk(mutex_);
  if (watchRunning_) {
    LOG_WARN("EtcdClient::watch: watcher already running");
    return false;
  }
  watchPrefix_ = prefix;
  watchCallback_ = std::move(on_change);
  watchRunning_ = true;
  watchThread_ = std::thread(&EtcdClient::watchLoop, this);
  return true;
}

void EtcdClient::watchLoop() {
  int attempt = 0;
  while (watchRunning_) {
    std::shared_ptr<etcd::Watcher> w;
    try {
      w = std::make_shared<etcd::Watcher>(
          *client_, watchPrefix_,
          [this](etcd::Response resp) {
            if (resp.is_ok()) {
              if (watchCallback_)
                watchCallback_();
            } else {
              LOG_WARN("EtcdClient: watch event error: %s",
                       resp.error_message().c_str());
            }
          },
          /*recursive=*/true);
      {
        std::lock_guard<std::mutex> lk(mutex_);
        if (!watchRunning_) {
          // stopWatch 已经跑过（它看不到刚构造、还没赋值的 watcher）。
          // 若继续 Wait() 就会永久阻塞，导致 stopWatch 里的 join 卡死。
          // 这里自己取消，并直接退出，绝不进入 Wait()。
          w->Cancel();
          break;
        }
        watcher_ = w;
      }
      attempt = 0;
      LOG_INFO("EtcdClient: watching prefix %s", watchPrefix_.c_str());
      // 连接建立后立即回调一次，让上层全量重拉（补齐断连期间可能丢失的事件）
      if (watchCallback_)
        watchCallback_();
      // 阻塞直到 watch 结束：true=正常 Cancel，false=服务端断开
      bool cancelled = w->Wait();
      {
        std::lock_guard<std::mutex> lk(mutex_);
        if (watcher_ == w)
          watcher_.reset();
      }
      if (cancelled || !watchRunning_)
        break;
      LOG_WARN("EtcdClient: watch on %s disconnected, reconnecting...",
               watchPrefix_.c_str());
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

// ---------- 健康监控 ----------

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
    // 探活：HEAD 一次即可判断连通性
    etcd::Response resp = client_->head();
    if (resp.is_ok()) {
      setHealth(EtcdHealth::Connected);
    } else {
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
