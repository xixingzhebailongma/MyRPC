#include "idempotency_redis.h"
#include "Logger.h"
#include <atomic>
#include <chrono>

// —— Lua 脚本（值编码见头文件注释）——
// claim：key 不存在→写入 EXECUTING 并返回 {1,""}（EXECUTE）；
//        值为 DONE→返回 {2, body}（REPLAY）；否则 {3,""}（IN_FLIGHT）。
const char *IdempotencyRedis::kClaimScript = R"lua(
  local v = redis.call('GET', KEYS[1])
  if not v then
    redis.call('SET', KEYS[1], '\001' .. ARGV[1] .. '\0' .. ARGV[2], 'PX', ARGV[3])
    return {1, ''}
  end
  if string.byte(v, 1) == 0x02 then
    return {2, string.sub(v, 2)}
  end
  return {3, ''}
  )lua";

// renew：owner+token 匹配才 PEXPIRE 续租，返回 1；否则 0（已被接管）。
const char *IdempotencyRedis::kRenewScript = R"lua(
  local v = redis.call('GET', KEYS[1])
  if not v then return 0 end
  if string.byte(v, 1) ~= 0x01 then return 0 end
  local rest = string.sub(v, 2)
  local sep = string.find(rest, '\0', 1, true)
  if not sep then return 0 end
  if string.sub(rest, 1, sep - 1) == ARGV[1] and string.sub(rest, sep + 1) == ARGV[2] then
    redis.call('PEXPIRE', KEYS[1], ARGV[3])
    return 1
  end
  return 0
  )lua";

// complete：owner+token 匹配才写入 DONE，返回 1；否则 0（结果被丢弃）。
const char *IdempotencyRedis::kCompleteScript = R"lua(
  local v = redis.call('GET', KEYS[1])
  if not v then return 0 end
  if string.byte(v, 1) ~= 0x01 then return 0 end
  local rest = string.sub(v, 2)
  local sep = string.find(rest, '\0', 1, true)
  if not sep then return 0 end
  if string.sub(rest, 1, sep - 1) == ARGV[1] and string.sub(rest, sep + 1) == ARGV[2] then
    redis.call('SET', KEYS[1], '\002' .. ARGV[4], 'PX', ARGV[3])
    return 1
  end
  return 0
  )lua";

// abort：owner+token 匹配才 DEL（释放占位），否则空操作。
const char *IdempotencyRedis::kAbortScript = R"lua(
  local v = redis.call('GET', KEYS[1])
  if not v then return 1 end
  if string.byte(v, 1) ~= 0x01 then return 1 end
  local rest = string.sub(v, 2)
  local sep = string.find(rest, '\0', 1, true)
  if not sep then return 1 end
  if string.sub(rest, 1, sep - 1) == ARGV[1] and string.sub(rest, sep + 1) == ARGV[2] then
    redis.call('DEL', KEYS[1])
  end
  return 1
  )lua";

// —— 进程内唯一 fencing token（时间戳高位 + 计数器低位）——
static uint64_t mintToken() {
  static std::atomic<uint64_t> counter{0};
  uint64_t ts = static_cast<uint64_t>(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
  return (ts << 20) ^ seq;
}

IdempotencyRedis::IdempotencyRedis(const std::string &owner_id,
                                   int lease_ttl_ms, int result_ttl_ms)
    : owner_id_(owner_id), lease_ttl_ms_(lease_ttl_ms),
      result_ttl_ms_(result_ttl_ms) {}

bool IdempotencyRedis::connect(const std::string &redis_ip, int redis_port) {
  return redis_.connect(redis_ip, redis_port);
}

IdemResult IdempotencyRedis::claim(const std::string &key,
                                   std::string *cached_body, IdemLease *lease) {
  uint64_t token = mintToken();
  if (lease)
    lease->token = token;
  std::vector<std::string> r = redis_.evalRead(
      kClaimScript, {key},
      {owner_id_, std::to_string(token), std::to_string(lease_ttl_ms_)});
  if (r.empty()) {
    // Redis 不可用：fail-open，照常执行（本次不缓存）
    LOG_ERROR("IdempotencyRedis::claim: redis error, fail-open key=%s",
              key.c_str());
    return IdemResult::kExecute;
  }
  if (r[0] == "1")
    return IdemResult::kExecute;
  if (r[0] == "2") {
    if (cached_body && r.size() > 1)
      *cached_body = r[1];
    return IdemResult::kReplay;
  }
  // "3" 及其它：in-flight，丢弃本次请求
  return IdemResult::kInFlight;
}

bool IdempotencyRedis::renew(const std::string &key, const IdemLease &lease) {
  std::vector<std::string> r = redis_.evalRead(
      kRenewScript, {key},
      {owner_id_, std::to_string(lease.token), std::to_string(lease_ttl_ms_)});
  if (r.empty()) {
    LOG_ERROR("IdempotencyRedis::renew: redis error, key=%s", key.c_str());
    return false; // 无法续租，停止续期
  }
  return r[0] == "1";
}

bool IdempotencyRedis::complete(const std::string &key, const IdemLease &lease,
                                const std::string &body) {
  std::vector<std::string> r =
      redis_.evalRead(kCompleteScript, {key},
                      {owner_id_, std::to_string(lease.token),
                       std::to_string(result_ttl_ms_), body});
  if (r.empty()) {
    LOG_ERROR("IdempotencyRedis::complete: redis error, key=%s", key.c_str());
    return false;
  }
  return r[0] == "1";
}

void IdempotencyRedis::abort(const std::string &key, const IdemLease &lease) {
  if (!redis_.eval(kAbortScript, {key},
                   {owner_id_, std::to_string(lease.token)})) {
    LOG_ERROR("IdempotencyRedis::abort: eval failed, key=%s", key.c_str());
  }
}