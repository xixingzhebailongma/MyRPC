#include "metrics_registry.h"
#include <algorithm>

namespace {
// 8 桶对数延迟直方图边界（微秒），严格递增；+Inf 桶用 total 表示、放在最后。
constexpr uint64_t kLatencyBucketsUs[8] = {100,  250,  500,  1000,
                                           2500, 5000, 10000, 25000};
constexpr int kBucketCount = 8;

// Prometheus label 转义：反斜杠、双引号、换行。
std::string escapeLabel(const std::string &s) {
  std::string o;
  o.reserve(s.size() + 8);
  for (char c : s) {
    switch (c) {
    case '\\':
      o += "\\\\";
      break;
    case '"':
      o += "\\\"";
      break;
    case '\n':
      o += "\\n";
      break;
    default:
      o += c;
      break;
    }
  }
  return o;
}
} // namespace

MetricsRegistry &MetricsRegistry::instance() {
  static MetricsRegistry inst;
  return inst;
}

void MetricsRegistry::recordServer(const std::string &service,
                                   const std::string &method,
                                   uint64_t latency_us, bool failed) {
  std::string svc = service.empty() ? "unknown" : service;
  std::string mth = method.empty() ? "unknown" : method;

  std::lock_guard<std::mutex> lk(server_mu_);
  auto &slot = server_[MetricKey{svc, mth}];
  if (!slot)
    slot = std::make_unique<ServerMetric>();
  ServerMetric *m = slot.get();

  m->total.fetch_add(1, std::memory_order_relaxed);
  if (failed)
    m->failed.fetch_add(1, std::memory_order_relaxed);
  m->latency_sum.fetch_add(latency_us, std::memory_order_relaxed);

  uint64_t cur = m->latency_max.load(std::memory_order_relaxed);
  while (latency_us > cur &&
         !m->latency_max.compare_exchange_weak(cur, latency_us,
                                               std::memory_order_relaxed))
    ;

  for (int i = 0; i < kBucketCount; ++i) {
    if (latency_us <= kLatencyBucketsUs[i])
      m->buckets[i].fetch_add(1, std::memory_order_relaxed);
  }
}

uint64_t MetricsRegistry::registerClient(ClientKind kind,
                                         std::function<ClientCounters()> reader) {
  std::lock_guard<std::mutex> lk(client_mu_);
  uint64_t token = next_client_id_++;
  clients_[token] = ClientEntry{kind, std::move(reader)};
  return token;
}

void MetricsRegistry::unregisterClient(uint64_t token) {
  std::lock_guard<std::mutex> lk(client_mu_);
  auto it = clients_.find(token);
  if (it == clients_.end())
    return;
  // 沉淀最终快照：连接池回收 channel 后，其累计计数不回退。
  ClientCounters c = it->second.reader();
  switch (it->second.kind) {
  case ClientKind::kSync:
    sync_accum_.total += c.total;
    sync_accum_.failed += c.failed;
    sync_accum_.latency_sum_us += c.latency_sum_us;
    sync_accum_.latency_max_us = std::max(sync_accum_.latency_max_us,
                                          c.latency_max_us);
    break;
  case ClientKind::kAsync:
    async_accum_.total += c.total;
    async_accum_.failed += c.failed;
    break;
  case ClientKind::kFailover:
    failover_accum_.failover += c.failover;
    break;
  }
  clients_.erase(it);
}

std::string MetricsRegistry::renderPrometheus() {
  std::string out;

  // ---- 服务端 ----
  if (std::unique_lock<std::mutex> lk(server_mu_, std::try_to_lock);
      lk.owns_lock()) {
    out += "# HELP rpc_server_requests_total Total RPC requests received, by "
           "service and method.\n";
    out += "# TYPE rpc_server_requests_total counter\n";
    out += "# HELP rpc_server_requests_failed_total Failed RPC requests, by "
           "service and method.\n";
    out += "# TYPE rpc_server_requests_failed_total counter\n";
    out += "# HELP rpc_server_latency_us End-to-end server request latency in "
           "microseconds.\n";
    out += "# TYPE rpc_server_latency_us histogram\n";
    out += "# HELP rpc_server_latency_us_max Max server request latency in "
           "microseconds.\n";
    out += "# TYPE rpc_server_latency_us_max gauge\n";

    for (const auto &kv : server_) {
      const MetricKey &key = kv.first;
      const ServerMetric *m = kv.second.get();
      std::string lbl = "service=\"" + escapeLabel(key.service) +
                        "\",method=\"" + escapeLabel(key.method) + "\"";
      out += "rpc_server_requests_total{" + lbl + "} " +
             std::to_string(m->total.load(std::memory_order_relaxed)) + "\n";
      out += "rpc_server_requests_failed_total{" + lbl + "} " +
             std::to_string(m->failed.load(std::memory_order_relaxed)) + "\n";
      out += "rpc_server_latency_us_max{" + lbl + "} " +
             std::to_string(m->latency_max.load(std::memory_order_relaxed)) +
             "\n";
      for (int i = 0; i < kBucketCount; ++i) {
        out += "rpc_server_latency_us_bucket{" + lbl + ",le=\"" +
               std::to_string(kLatencyBucketsUs[i]) + "\"} " +
               std::to_string(m->buckets[i].load(std::memory_order_relaxed)) +
               "\n";
      }
      out += "rpc_server_latency_us_bucket{" + lbl + ",le=\"+Inf\"} " +
             std::to_string(m->total.load(std::memory_order_relaxed)) + "\n";
      out += "rpc_server_latency_us_sum{" + lbl + "} " +
             std::to_string(m->latency_sum.load(std::memory_order_relaxed)) +
             "\n";
      out += "rpc_server_latency_us_count{" + lbl + "} " +
             std::to_string(m->total.load(std::memory_order_relaxed)) + "\n";
    }
  }

  // ---- 客户端（聚合：累加器 + 在册项）----
  if (std::unique_lock<std::mutex> lk(client_mu_, std::try_to_lock);
      lk.owns_lock()) {
    ClientCounters sync = sync_accum_;
    ClientCounters async = async_accum_;
    ClientCounters failover = failover_accum_;
    for (const auto &kv : clients_) {
      ClientCounters c = kv.second.reader();
      switch (kv.second.kind) {
      case ClientKind::kSync:
        sync.total += c.total;
        sync.failed += c.failed;
        sync.latency_sum_us += c.latency_sum_us;
        sync.latency_max_us = std::max(sync.latency_max_us, c.latency_max_us);
        break;
      case ClientKind::kAsync:
        async.total += c.total;
        async.failed += c.failed;
        break;
      case ClientKind::kFailover:
        failover.failover += c.failover;
        break;
      }
    }

    out += "# HELP rpc_client_requests_total Total synchronous client RPC "
           "calls.\n";
    out += "# TYPE rpc_client_requests_total counter\n";
    out += "rpc_client_requests_total " + std::to_string(sync.total) + "\n";
    out += "# HELP rpc_client_requests_failed_total Failed synchronous client "
           "RPC calls.\n";
    out += "# TYPE rpc_client_requests_failed_total counter\n";
    out += "rpc_client_requests_failed_total " + std::to_string(sync.failed) +
           "\n";
    out += "# HELP rpc_client_latency_us_sum Sum of synchronous client call "
           "latency in microseconds.\n";
    out += "# TYPE rpc_client_latency_us_sum counter\n";
    out += "rpc_client_latency_us_sum " + std::to_string(sync.latency_sum_us) +
           "\n";
    out += "# HELP rpc_client_latency_us_max Max synchronous client call "
           "latency in microseconds.\n";
    out += "# TYPE rpc_client_latency_us_max gauge\n";
    out += "rpc_client_latency_us_max " + std::to_string(sync.latency_max_us) +
           "\n";
    out += "# HELP rpc_async_client_requests_total Total asynchronous client "
           "RPC calls.\n";
    out += "# TYPE rpc_async_client_requests_total counter\n";
    out += "rpc_async_client_requests_total " + std::to_string(async.total) +
           "\n";
    out += "# HELP rpc_async_client_requests_failed_total Failed asynchronous "
           "client RPC calls.\n";
    out += "# TYPE rpc_async_client_requests_failed_total counter\n";
    out += "rpc_async_client_requests_failed_total " +
           std::to_string(async.failed) + "\n";
    out += "# HELP rpc_client_failover_total Total client failover attempts.\n";
    out += "# TYPE rpc_client_failover_total counter\n";
    out += "rpc_client_failover_total " + std::to_string(failover.failover) +
           "\n";
  }

  return out;
}

ServerCounters MetricsRegistry::serverSnapshot(const std::string &service,
                                               const std::string &method) {
  std::string svc = service.empty() ? "unknown" : service;
  std::string mth = method.empty() ? "unknown" : method;
  ServerCounters out;
  std::lock_guard<std::mutex> lk(server_mu_);
  auto it = server_.find(MetricKey{svc, mth});
  if (it != server_.end()) {
    const ServerMetric *m = it->second.get();
    out.total = m->total.load(std::memory_order_relaxed);
    out.failed = m->failed.load(std::memory_order_relaxed);
    out.latency_sum = m->latency_sum.load(std::memory_order_relaxed);
    out.latency_max = m->latency_max.load(std::memory_order_relaxed);
  }
  return out;
}

ClientCounters MetricsRegistry::clientSnapshot() {
  std::lock_guard<std::mutex> lk(client_mu_);
  ClientCounters sync = sync_accum_;
  ClientCounters async = async_accum_;
  ClientCounters failover = failover_accum_;
  for (const auto &kv : clients_) {
    ClientCounters c = kv.second.reader();
    switch (kv.second.kind) {
    case ClientKind::kSync:
      sync.total += c.total;
      sync.failed += c.failed;
      sync.latency_sum_us += c.latency_sum_us;
      sync.latency_max_us = std::max(sync.latency_max_us, c.latency_max_us);
      break;
    case ClientKind::kAsync:
      async.total += c.total;
      async.failed += c.failed;
      break;
    case ClientKind::kFailover:
      failover.failover += c.failover;
      break;
    }
  }
  ClientCounters agg;
  agg.total = sync.total + async.total;
  agg.failed = sync.failed + async.failed;
  agg.latency_sum_us = sync.latency_sum_us;
  agg.latency_max_us = sync.latency_max_us;
  agg.failover = failover.failover;
  return agg;
}
