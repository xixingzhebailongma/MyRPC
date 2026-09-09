#pragma once
#include "idempotency_store.h"
#include "redis_client.h"
#include <cstdint>
#include <string>

// 共享 Redis 幂等存储（Lua 原子状态机版，解决 Q2 跨节点 failover 幂等）：
//   - claim/renew/complete/abort 均为 Lua 原子操作，owner + fencing token
//   校验；
//   - 值编码：'\x01' + owner + '\0' + token（EXECUTING），'\x02' +
//   body（DONE）；
//   - 用 key 自身 TTL 作租约（PEXPIRE 续租），不额外存 lease_expire 字段。
// Redis 故障时 fail-open：claim 返回 kExecute（照常执行但不缓存），保证可用性，
// 绝不因 Redis 挂而误拒绝/误丢弃请求。
class IdempotencyRedis final : public IdempotencyStore {
public:
  // owner_id 须节点间唯一（如 server_id / ip:port）；lease_ttl_ms 为 in-flight
  // 租约，result_ttl_ms 为完成态响应缓存。
  IdempotencyRedis(const std::string &owner_id = "node",
                   int lease_ttl_ms = 5000, int result_ttl_ms = 120000);
  ~IdempotencyRedis() override = default;

  IdempotencyRedis(const IdempotencyRedis &) = delete;
  IdempotencyRedis &operator=(const IdempotencyRedis &) = delete;

  bool connect(const std::string &redis_ip, int redis_port);

  IdemResult claim(const std::string &key, std::string *cached_body,
                   IdemLease *lease) override;
  bool renew(const std::string &key, const IdemLease &lease) override;
  bool complete(const std::string &key, const IdemLease &lease,
                const std::string &body) override;
  void abort(const std::string &key, const IdemLease &lease) override;

private:
  RedisClient redis_;
  std::string owner_id_;
  int lease_ttl_ms_;
  int result_ttl_ms_;

  static const char *kClaimScript;
  static const char *kRenewScript;
  static const char *kCompleteScript;
  static const char *kAbortScript;
};