#pragma once
#include "idempotency_store.h"
#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

// 后台租约续期器：对执行中的 in-flight 租约周期性 renew，防止慢 worker 的
// 固定 TTL 到期后被重试请求抢占（Q1 核心）。同时对每个租约带一个「执行
// Deadline（软截止）」：到期即 abort 释放租约，让重试立即接管；过期 worker
// 的 complete 因 fencing token 不匹配被丢弃。
//
// 说明：软截止只能释放租约 + 丢弃过期响应，无法中断正在运行的 handler。
// 线程安全：add/remove 由 worker 线程调用，run 由内部后台线程调用，互斥保护。
class LeaseRenewer {
public:
  explicit LeaseRenewer(IdempotencyStore *store,
                        uint64_t renew_interval_ms = 1000)
      : store_(store), renew_interval_ms_(renew_interval_ms) {}

  ~LeaseRenewer() { stop(); }

  void start() {
    if (running_.exchange(true))
      return;
    thread_ = std::thread([this] { run(); });
  }

  void stop() {
    if (!running_.exchange(false))
      return;
    if (thread_.joinable())
      thread_.join();
  }

  // claim kExecute 后登记。deadline 为软截止时刻；传 time_point::max() 表示
  // 无限期（禁用 deadline，只续租）。
  void add(const std::string &key, IdemLease lease,
           std::chrono::steady_clock::time_point deadline) {
    std::lock_guard<std::mutex> lock(mu_);
    leases_[key] = Item{lease, deadline};
  }

  // complete/abort 后注销。
  void remove(const std::string &key) {
    std::lock_guard<std::mutex> lock(mu_);
    leases_.erase(key);
  }

private:
  struct Item {
    IdemLease lease;
    std::chrono::steady_clock::time_point deadline;
  };

  void run() {
    while (running_.load()) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(renew_interval_ms_));
      std::vector<std::pair<std::string, IdemLease>> to_abort;
      std::vector<std::pair<std::string, IdemLease>> to_renew;
      {
        std::lock_guard<std::mutex> lock(mu_);
        auto now = std::chrono::steady_clock::now();
        for (auto &kv : leases_) {
          if (now >= kv.second.deadline)
            to_abort.push_back({kv.first, kv.second.lease});
          else
            to_renew.push_back({kv.first, kv.second.lease});
        }
      }
      // 在锁外做 store 操作（renew 可能走网络），避免阻塞 worker 的 add/remove
      for (auto &it : to_abort)
        store_->abort(it.first, it.second);
      std::vector<std::string> superseded;
      for (auto &it : to_renew)
        if (!store_->renew(it.first, it.second))
          superseded.push_back(it.first);
      if (!to_abort.empty() || !superseded.empty()) {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto &it : to_abort)
          leases_.erase(it.first);
        for (auto &k : superseded)
          leases_.erase(k);
      }
    }
  }

  IdempotencyStore *store_;
  uint64_t renew_interval_ms_;
  std::mutex mu_;
  std::unordered_map<std::string, Item> leases_;
  std::atomic<bool> running_{false};
  std::thread thread_;
};