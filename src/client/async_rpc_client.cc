#include "async_rpc_client.h"
#include "EventLoop.h"
#include "async_rpc_channel.h"

AsyncRpcClient::AsyncRpcClient(int loop_threads, const RpcClientConfig &cfg)
    : cfg_(cfg) {
  if (loop_threads <= 0) {
    loop_threads = static_cast<int>(std::thread::hardware_concurrency());
    if (loop_threads <= 0)
      loop_threads = 1;
  }
  loops_.reserve(loop_threads);
  for (int i = 0; i < loop_threads; ++i)
    loops_.emplace_back(new EventLoop(false));
  for (auto &loop : loops_)
    threads_.emplace_back([loop = loop.get()] { loop->run(); });
}

AsyncRpcClient::~AsyncRpcClient() { stop(); }

std::future<std::string>
AsyncRpcClient::Call(const std::string &ip, uint16_t port,
                     const std::string &service, const std::string &method,
                     const std::string &request_body, int timeout_ms,
                     const std::string &request_id) {
  auto ch = getOrCreate(ip, port);
  return ch->Call(service, method, request_body, timeout_ms, request_id);
}

void AsyncRpcClient::Call(const std::string &ip, uint16_t port,
                          const std::string &service, const std::string &method,
                          const std::string &request_body, ResponseCallback cb,
                          int timeout_ms, const std::string &request_id) {
  auto ch = getOrCreate(ip, port);
  ch->Call(service, method, request_body, std::move(cb), timeout_ms,
           request_id);
}

std::shared_ptr<AsyncRpcChannel>
AsyncRpcClient::getOrCreate(const std::string &ip, uint16_t port) {
  std::string key = ip + ":" + std::to_string(port);
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = channels_.find(key);
  if (it != channels_.end())
    return it->second;
  EventLoop *loop = loops_[std::hash<std::string>{}(key) % loops_.size()].get();
  auto ch = std::make_shared<AsyncRpcChannel>(loop, ip, port, cfg_);
  channels_[key] = ch;
  return ch;
}

bool AsyncRpcClient::isCircuitOpen(const std::string &key) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = channels_.find(key);
  return it != channels_.end() && it->second->isCircuitOpen();
}

void AsyncRpcClient::stop() {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    for (auto &kv : channels_)
      kv.second->close(); // 失败所有在途请求
    channels_.clear();
  }
  for (auto &loop : loops_)
    loop->stop();
  for (auto &t : threads_)
    if (t.joinable())
      t.join();
}