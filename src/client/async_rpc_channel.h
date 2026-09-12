#pragma once
#include "Timer.h"
#include "rpc_client_config.h"
#include "rpc_protocol.h"
#include <atomic>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

class EventLoop;
class TcpClient;
class Connection;
class Buffer;

// 异步调用失败（error_code != 0）时 future 抛出的异常。
class AsyncRpcError : public std::runtime_error {
public:
  explicit AsyncRpcError(int32_t code)
      : std::runtime_error("async rpc error"), code_(code) {}
  int32_t code() const { return code_; }

private:
  int32_t code_;
};

// 单连接异步 RPC 会话：一条 TCP 连接上复用 sequence_id 多路复用并发请求。
// 所有收发/状态都在所属 EventLoop 线程内完成，任意线程可调 Call。
// 生命周期约定：对象须由 shared_ptr 持有；使用期间 loop 保持运行。
class AsyncRpcChannel : public std::enable_shared_from_this<AsyncRpcChannel> {
public:
  using ResponseCallback =
      std::function<void(std::string body, int32_t error_code)>;
  using PushHandler = std::function<void(const RpcMessage &msg)>;

  // server_ip 为点分十进制 IP（与 RpcChannel 一致，不含域名）。
  AsyncRpcChannel(EventLoop *loop, const std::string &server_ip,
                  uint16_t server_port, const RpcClientConfig &cfg = {});
  ~AsyncRpcChannel();

  void connect(); // 建连（首次 Call 会自动调用）
  void close();   // 断开并停止重连，失败所有在途请求
  // 同步关闭连接（不投递 loop）。供 AsyncRpcClient::stop() 在 join 后调用：
  // 极端竞态下 close() 排队的异步 teardown 可能没被 loop 执行，此方法直接
  // reset conn_/client_，确保对端 socket 一定被关闭（避免对端 recv 永久阻塞）。
  void shutdown();

  std::future<std::string> Call(const std::string &service,
                                const std::string &method,
                                const std::string &request_body,
                                int timeout_ms = -1,
                                const std::string &request_id = "");

  void Call(const std::string &service, const std::string &method,
            const std::string &request_body, ResponseCallback cb,
            int timeout_ms = -1, const std::string &request_id = "");

  bool connected() const { return connected_.load(); }
  void setPushHandler(PushHandler h) { push_handler_ = std::move(h); }

  // 熔断状态（供 AsyncLbRpcClient 跳过熔断节点）
  bool isCircuitOpen() const;
  int consecutiveFailures() const { return consecutive_failures_.load(); }
  // 空闲回收：ttl_ms 内无调用则关闭并从连接池剔除；0 = 禁用。
  void setIdleTimeout(uint64_t ttl_ms);
  // 空闲到期回调（由 AsyncRpcClient 绑定：从池中 erase + close）。
  void setIdleExpiredCallback(
      std::function<void(std::shared_ptr<AsyncRpcChannel>)> cb);

private:
  enum class State { kIdle, kConnected, kBroken };

  struct PendingCall {
    std::shared_ptr<std::promise<std::string>> promise;
    ResponseCallback callback;
    TimerId timeout_timer;
    std::string wire; // 连接未就绪时暂存的待发请求帧，onConnection 统一 flush
  };

  void startInLoop();
  void onConnection(std::shared_ptr<Connection> conn);
  void onClose(std::shared_ptr<Connection> conn);
  void onMessage(std::shared_ptr<Connection> conn, Buffer &buf);
  void handleFrame(const std::string &payload);
  void onTimeout(uint64_t seq);
  void failCall(uint64_t seq, int32_t code);
  void failPending(int32_t code);
  void deliver(PendingCall &pc, std::string body, int32_t code);
  void callImpl(const std::string &service, const std::string &method,
                const std::string &body, int timeout_ms,
                const std::string &request_id,
                std::shared_ptr<std::promise<std::string>> promise,
                ResponseCallback cb);
  // 熔断
  void recordFailure();
  void recordSuccess();
  // 心跳
  void heartbeatTick();
  void sendHeartbeatPing();
  // 空闲回收（只在 loop 线程访问）
  void armIdleTimerInLoop();
  bool idleExpired() const;

  EventLoop *loop_;
  std::unique_ptr<TcpClient> client_;
  std::string server_ip_;
  uint16_t server_port_;
  RpcClientConfig cfg_;

  // 熔断参数（构造时从 cfg 拷贝，只读）
  int failure_threshold_;
  uint64_t backoff_base_ms_;
  uint64_t backoff_max_ms_;

  std::unordered_map<uint64_t, PendingCall> pending_; // 只在 loop 线程访问
  uint64_t next_seq_id_{0};          // 只在 loop 线程访问
  std::shared_ptr<Connection> conn_; // 只在 loop 线程访问
  bool started_{false};              // 只在 loop 线程访问

  // 心跳/判活（只在 loop 线程访问）
  TimerId heartbeat_timer_;
  uint64_t last_active_ms_{0};
  int heartbeat_miss_{0};
  // 空闲回收
  TimerId idle_timer_;   // 只在 loop 线程访问
  uint64_t idle_ttl_ms_; // 构造时从 cfg 拷贝，之后只读
  std::function<void(std::shared_ptr<AsyncRpcChannel>)> idle_expired_cb_;

  std::atomic<State> state_{State::kIdle};
  std::atomic<int> consecutive_failures_{0};
  std::atomic<uint64_t> next_retry_ms_{0};
  std::atomic<bool> connected_{false};
  std::atomic<bool> closed_{false};

  PushHandler push_handler_;

  std::atomic<uint64_t> total_calls_{0};
  std::atomic<uint64_t> success_calls_{0};
  std::atomic<uint64_t> fail_calls_{0};
  std::atomic<uint64_t> last_used_ms_{0}; // 最近一次 Call 时间（单调 ms）
};