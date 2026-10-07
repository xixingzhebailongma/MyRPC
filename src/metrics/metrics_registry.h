#pragma once
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// 单调时钟微秒：服务端延迟计时的统一来源（不受系统时间回拨影响）。
inline uint64_t monotonicUs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// 客户端计数器的即时快照。注册的是「读值回调」而非裸指针，
// 渲染时由回调当场读各客户端对象的原子量，杜绝野指针。
struct ClientCounters {
  uint64_t total = 0;
  uint64_t failed = 0;
  uint64_t latency_sum_us = 0;
  uint64_t latency_max_us = 0;
  uint64_t failover = 0;
};

// 服务端计数快照（测试/观测用）。
struct ServerCounters {
  uint64_t total = 0;
  uint64_t failed = 0;
  uint64_t latency_sum = 0;
  uint64_t latency_max = 0;
};

// 进程级指标注册表（单例，镜像 Logger::instance() 的写法）。
// - 服务端：RpcServer::dispatch 记录 (service, method) 维度计数/延迟/直方图。
// - 客户端：RpcChannel / AsyncRpcChannel / LbRpcClient 注册读值回调。
class MetricsRegistry {
public:
  static MetricsRegistry &instance();

  // 服务端：记录一次请求。latency_us 由调用方从入口 t0 到完成算好；
  // service/method 为空时统一归一化为 "unknown"。
  void recordServer(const std::string &service, const std::string &method,
                    uint64_t latency_us, bool failed);

  enum class ClientKind { kSync, kAsync, kFailover };
  // 客户端：注册读值回调（lambda 捕获 this 读成员原子量），返回 token 供注销。
  // 注销时会把该客户端的最终快照沉淀进单调累加器，保证 _total 不因连接回收而
  // 回退。
  uint64_t registerClient(ClientKind kind,
                          std::function<ClientCounters()> reader);
  void unregisterClient(uint64_t token);

  // 渲染 Prometheus 文本格式；任一把内部锁 try_lock 失败就跳过对应段（不阻塞、
  // 不崩，宁可少报也不卡住 scrape）。
  std::string renderPrometheus();

  // 结构化快照（测试/观测用，阻塞取真值；service/method 空值归一化为 "unknown"）。
  ServerCounters serverSnapshot(const std::string &service,
                                const std::string &method);
  ClientCounters clientSnapshot(); // 聚合：累加器 + 在册项

private:
  MetricsRegistry() = default;
  MetricsRegistry(const MetricsRegistry &) = delete;
  MetricsRegistry &operator=(const MetricsRegistry &) = delete;

  struct ServerMetric {
    std::atomic<uint64_t> total{0};
    std::atomic<uint64_t> failed{0};
    std::atomic<uint64_t> latency_sum{0};
    std::atomic<uint64_t> latency_max{0};
    // 累计直方图桶：buckets[i] = 延迟 <= bound[i] 的观测数；+Inf 桶 == total。
    std::atomic<uint64_t> buckets[8]{};
  };
  struct MetricKey {
    std::string service;
    std::string method;
    bool operator<(const MetricKey &o) const {
      if (service != o.service)
        return service < o.service;
      return method < o.method;
    }
  };
  struct ClientEntry {
    ClientKind kind;
    std::function<ClientCounters()> reader;
  };

  std::mutex server_mu_;
  std::map<MetricKey, std::unique_ptr<ServerMetric>> server_;

  std::mutex client_mu_;
  std::unordered_map<uint64_t, ClientEntry> clients_;
  uint64_t next_client_id_{1};
  // 已注销客户端的最终快照沉淀（client_mu_ 保护），渲染时 = 累加器 + 在册项。
  ClientCounters sync_accum_;
  ClientCounters async_accum_;
  ClientCounters failover_accum_;
};
