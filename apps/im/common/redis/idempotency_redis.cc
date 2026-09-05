#include "idempotency_redis.h"
#include "Logger.h"

IdempotencyRedis::IdempotencyRedis(int done_ttl_sec, int in_flight_ttl_sec)
    : done_ttl_sec_(done_ttl_sec), in_flight_ttl_sec_(in_flight_ttl_sec) {}

bool IdempotencyRedis::connect(const std::string &redis_ip, int redis_port) {
  return redis_.connect(redis_ip, redis_port);
}

IdemResult IdempotencyRedis::claim(const std::string &key,
                                   std::string *cached_body) {
  // 1) 原子抢占：仅当 key 不存在时写入 in-flight 占位（带租约 TTL）
  std::string inflight(1, kInFlightPrefix);
  SetResult r = redis_.setNxEx(key, inflight, in_flight_ttl_sec_);
  if (r == SetResult::kSet) {
    return IdemResult::kExecute; // 拿到所有权，执行后必须 complete/abort
  }
  if (r == SetResult::kError) {
    // Redis 不可用：fail-open——照常执行（本次不缓存），避免 Redis
    // 挂导致服务不可用
    LOG_ERROR("IdempotencyRedis::claim: redis error, fail-open key=%s",
              key.c_str());
    return IdemResult::kExecute;
  }
  // 2) kExists：key 已存在，GET 读状态区分 in-flight / done
  std::string val = redis_.get(key);
  if (val.empty()) {
    // 抢占失败与 GET 之间 key 恰好过期（罕见竞态）：重试抢占一次
    SetResult r2 = redis_.setNxEx(key, inflight, in_flight_ttl_sec_);
    if (r2 == SetResult::kError) {
      LOG_ERROR("IdempotencyRedis::claim: redis error on retry, key=%s",
                key.c_str());
    }
    return IdemResult::kExecute;
  }
  if (val[0] == kInFlightPrefix) {
    return IdemResult::kInFlight; // 另一请求正在执行，丢弃本次
  }
  if (val[0] == kDonePrefix) {
    if (cached_body) {
      cached_body->assign(val.begin() + 1, val.end()); // 回填缓存响应体
    }
    return IdemResult::kReplay;
  }
  // 防御：未知前缀（理论不会出现），按可执行处理
  LOG_WARN("IdempotencyRedis::claim: unknown prefix, key=%s", key.c_str());
  return IdemResult::kExecute;
}

void IdempotencyRedis::complete(const std::string &key,
                                const std::string &body) {
  // 转完成态：值 = "\x01" + body，用 setex 原子设值+过期
  std::string val;
  val.reserve(1 + body.size());
  val.push_back(kDonePrefix);
  val.append(body);
  if (!redis_.setex(key, val, done_ttl_sec_)) {
    LOG_ERROR("IdempotencyRedis::complete: setex failed, key=%s", key.c_str());
  }
}

void IdempotencyRedis::abort(const std::string &key) { redis_.del(key); }