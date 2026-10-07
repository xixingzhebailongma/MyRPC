#pragma once
#include "rpc_client_config.h"
#include "rpc_protocol.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>

// 单调时钟毫秒时间戳，用于空闲回收（不受系统时间回拨影响）。
// 供 RpcChannel 记录 last_used、RpcChannelPool 计算空闲时长共用。
inline uint64_t nowMs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

inline uint64_t nowUs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

class RpcChannel {
public:
  // 连接状态机：kIdle=未连过，kConnecting=连接中，kConnected=健康，kBroken=已知断
  enum class State { kIdle, kConnecting, kConnected, kBroken };
  // timeout_ms: 单次 RPC 调用的响应超时(毫秒)，默认 3000ms
  // connect_timeout_ms: 建连超时(毫秒)，与响应超时分离，默认 3000ms
  RpcChannel(const std::string &server_ip, uint16_t server_port,
             int timeout_ms = 3000, int connect_timeout_ms = 3000);

  // 从集中配置构造（P2）：超时/心跳/熔断参数全部来自 RpcClientConfig
  RpcChannel(const std::string &server_ip, uint16_t server_port,
             const RpcClientConfig &cfg);
  ~RpcChannel();

  // 同步 RPC 调用，阻塞直到收到响应。多线程可并发复用同一 channel：
  // 后台收帧线程按 sequence_id 分发响应，不再整体串行。
  // 返回 true 表示成功，response_body 和 error_code 是返回值。
  bool Call(const std::string &service_name, const std::string &method_name,
            const std::string &request_body, std::string &response_body,
            int32_t &error_code, int timeout_ms = -1,
            const std::string &request_id = "",
            const std::string &trace_id = "");

  // 发送调用方预构造的 RpcMessage（Gateway 用来注入 header 字段）。
  // 只覆盖 sequence_id 为自增 seq，其余字段（service/method/gateway_id
  // 等）保留。
  bool CallMessage(const RpcMessage &request, std::string &response_body,
                   int32_t &error_code, int timeout_ms = -1);

  // 连接到服务器（首次 Call 自动调用），并发安全
  bool connect();
  void close();
  bool isConnected() const { return sockfd_.load() >= 0; }

  // 动态修改超时时间（毫秒），作用于之后每次 Call 的 cv 等待
  void setTimeout(int timeout_ms) { timeout_ms_.store(timeout_ms); }
  void setConnectTimeout(int timeout_ms) {
    connect_timeout_ms_.store(timeout_ms);
  }

  void setHeartbeat(int interval_ms, int miss_threshold) {
    heartbeat_interval_ms_.store(interval_ms);
    heartbeat_miss_threshold_.store(miss_threshold);
  }
  // 最近一次 Call 开始的时间戳（nowMs() 单调时钟），空闲回收依据
  uint64_t lastUsedMs() const { return last_used_ms_.load(); }

  // 熔断相关：连续失败达到阈值后进入熔断态，isCircuitOpen() 期间 Call
  // 快速失败。
  void recordFailure();
  void recordSuccess();
  bool isCircuitOpen() const;
  State state() const { return state_.load(); }
  int consecutiveFailures() const { return consecutive_failures_.load(); }

  uint64_t totalCalls() const { return total_calls_.load(); }
  uint64_t successCalls() const { return success_calls_.load(); }
  uint64_t failCalls() const { return fail_calls_.load(); }
  uint64_t totalLatencyUs() const { return total_latency_us_.load(); }
  uint64_t maxLatencyUs() const { return max_latency_us_.load(); }

private:
  // 熔断参数：连续失败 failure_threshold_ 次进入熔断；指数退避从
  // backoff_base_ms_ 起每次翻倍，封顶
  // backoff_max_ms_。改为实例成员，便于从配置注入。
  int failure_threshold_{3};
  uint64_t backoff_base_ms_{1000};
  uint64_t backoff_max_ms_{30000};
  // 仅供 tests/test_rpc_channel_frame.cc 直接驱动帧长度校验逻辑做坏帧回归
  friend struct RpcChannelFrameTest;

  void readLoop();
  // 发一帧心跳请求；try_lock send_mutex_，拿不到就跳过（见 .cc 死锁说明）。
  bool sendHeartbeat(int fd);
  // 从 fd 读一帧（4 字节 BE 长度前缀 + payload），校验长度合法性。
  // 成功返回 true 并把 payload（不含长度前缀）写入 payload；非法长度不触碰
  // payload。
  bool readFrame(int fd, std::string &payload);
  // 前提：调用前必须已持有 send_mutex_。读满 n 字节（EINTR 重试）。
  bool sendAll(const char *data, size_t n);
  bool readFull(int fd, char *buf, size_t n);
  // Call/CallMessage 的公共实现：懒连接 + 分配 seq + 发送 + 等待 + 取响应。
  bool callImpl(const RpcMessage &request, std::string &response_body,
                int32_t &error_code, int timeout_ms);
  // 记一次调用结果：累计调用数/成功数/延迟（微秒）与最大延迟。
  void recordOutcome(bool ok, uint64_t t0_us);

  std::string server_ip_;
  uint16_t server_port_;
  std::atomic<int> sockfd_{-1}; // -1 表示未连接；close/readLoop 并发访问
  std::atomic<uint64_t> last_used_ms_{0}; // 最近一次 Call 开始时间（单调 ms）
  std::atomic<State> state_{State::kIdle}; // 连接状态机
  std::atomic<int> consecutive_failures_{0}; // 连续失败计数（熔断依据）
  std::atomic<uint64_t> next_retry_ms_{0}; // 熔断后可重试的时间点（单调 ms）
  std::atomic<int> timeout_ms_{3000};         // 每 Call 的 cv 等待超时
  std::atomic<int> connect_timeout_ms_{3000}; // 建连超时（connect poll）
  std::atomic<int> heartbeat_interval_ms_{5000}; // 空闲多久发一次心跳
  std::atomic<int> heartbeat_miss_threshold_{3}; // 连续 miss 多少次判死
  std::atomic<uint64_t> total_calls_{0};
  std::atomic<uint64_t> success_calls_{0};
  std::atomic<uint64_t> fail_calls_{0};
  std::atomic<uint64_t> total_latency_us_{0}; //所有已记录调用的延迟累加和
  std::atomic<uint64_t> max_latency_us_{0}; //已记录调用中单次延迟的最大值
  std::atomic<uint64_t> next_seq_id_{0}; // 请求序列号，每次调用自增
  std::atomic<bool> running_{false};     // readLoop 循环开关
  std::thread thread_;

  // 生命周期锁：串行化 connect()/close()，避免并发懒连接与关闭竞争。
  std::mutex connect_mutex_;

  // 串行化整帧发送，防止并发 Call 的帧在 socket 上交错。
  // 必须与 mutex_ 分开：readLoop 投递响应要拿 mutex_，若发送方持 mutex_
  // 阻塞在 ::send，readLoop 就会停止收帧，进而和对端形成死锁。
  std::mutex send_mutex_;

  std::mutex mutex_; // 护 responses_/closed_/cv_
  std::condition_variable cv_;
  std::map<uint64_t, RpcMessage> responses_; // seq -> 完整响应（含 error_code）
  std::set<uint64_t> pending_; // seq -> 等待响应的请求（Call 内部 cv 等待）
  bool closed_ = false; // 连接已断开，由 mutex_ 保护

  // metrics：客户端计数器注册（读值回调，token 用于析构注销）
  void registerMetrics();
  void unregisterMetrics();
  uint64_t metrics_token_{0};
};