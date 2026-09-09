#pragma once
#include <cstdint>
#include <string>

enum class IdemResult {
  kExecute, // 本调用获得 key 所有权（已插入 in-flight 占位），必须
            // complete/abort
  kReplay,   // 已有完成态响应，*cached_body 已回填
  kInFlight, // 另一请求正在执行该 key，调用方应丢弃本次请求
};

// 一次 claim 成功（kExecute）时发放的 fencing token。complete/abort/renew
// 必须携带，存储据此判断该 worker 是否仍持有 key 所有权：若已被新请求抢占，
// token 不匹配，旧 worker 的 complete 会被丢弃、renew 返回 false。
struct IdemLease {
  uint64_t token = 0;
};

class IdempotencyStore {
public:
  virtual ~IdempotencyStore() = default;
  virtual IdemResult claim(const std::string &key, std::string *cached_body,
                           IdemLease *lease) = 0;
  // 续租：执行期间刷新 in-flight 租约。返回 false 表示已被接管（应停止执行）。
  virtual bool renew(const std::string &key, const IdemLease &lease) = 0;
  // 完成：仅当 token 仍为当前时才写入完成态响应。返回 false 表示已被接管，
  // 结果应丢弃。
  virtual bool complete(const std::string &key, const IdemLease &lease,
                        const std::string &body) = 0;
  // 释放占位：仅当 token 仍为当前时才释放。
  virtual void abort(const std::string &key, const IdemLease &lease) = 0;
};