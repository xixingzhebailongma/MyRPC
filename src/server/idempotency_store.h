#pragma once
#include <string>

enum class IdemResult {
  kExecute, // 本调用获得 key 所有权（已插入 in-flight 占位），必须
            // complete/abort
  kReplay,   // 已有完成态响应，*cached_body 已回填
  kInFlight, // 另一请求正在执行该 key，调用方应丢弃本次请求
};

class IdempotencyStore {
public:
  virtual ~IdempotencyStore() = default;
  virtual IdemResult claim(const std::string &key,
                           std::string *cached_body) = 0;
  virtual void complete(const std::string &key, const std::string &body) = 0;
  virtual void abort(const std::string &key) = 0;
};