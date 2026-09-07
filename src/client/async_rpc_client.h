#pragma once
#include "rpc_client_config.h"
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

class EventLoop;
class AsyncRpcChannel;

// N 个 EventLoop 线程 + 按 ip:port 分发连接的异步客户端。
// 用少量线程（默认 = CPU 核数）epoll 承载海量连接，解决"每连接一线程"问题。
class AsyncRpcClient {
public:
  using ResponseCallback =
      std::function<void(std::string body, int32_t error_code)>;

  // loop_threads: 事件循环线程数，<=0 时取 hardware_concurrency（至少 1）。
  explicit AsyncRpcClient(int loop_threads = 0,
                          const RpcClientConfig &cfg = {});
  ~AsyncRpcClient();

  std::future<std::string>
  Call(const std::string &ip, uint16_t port, const std::string &service,
       const std::string &method, const std::string &request_body,
       int timeout_ms = -1, const std::string &request_id = "");

  void Call(const std::string &ip, uint16_t port, const std::string &service,
            const std::string &method, const std::string &request_body,
            ResponseCallback cb, int timeout_ms = -1,
            const std::string &request_id = "");

  void stop(); // 关闭所有连接并停止所有 loop 线程（析构也会调用）

  // 查询某 ip:port 连接的熔断状态（不存在则返回 false）。
  // key 形如 "1.2.3.4:8080"，与内部连接的键一致。
  bool isCircuitOpen(const std::string &key);

private:
  std::shared_ptr<AsyncRpcChannel> getOrCreate(const std::string &ip,
                                               uint16_t port);

  RpcClientConfig cfg_;
  std::vector<std::unique_ptr<EventLoop>> loops_;
  std::vector<std::thread> threads_;

  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<AsyncRpcChannel>> channels_;
};