#pragma once
#include "idempotency_store.h"
#include "redis_client.h"
#include <string>

// 共享 Redis 幂等存储：把 RpcServer 的 request_id 去重从进程内 LRU 换成 Redis，
// 使跨节点 failover 的去重生效。
//
// 值编码（单 key，前缀区分状态）：
//   "\x00"          in-flight 租约（EX in_flight_ttl_sec）
//   "\x01" + body   完成态响应（EX done_ttl_sec）
// claim 用原子 SET NX EX 抢占；抢不到再 GET 区分 in-flight / done。
//
// Redis 故障时 fail-open：返回
// kExecute（照常执行但不缓存），幂等降级为「无去重」， 绝不因 Redis
// 挂而误拒绝/误丢弃请求。
class IdempotencyRedis final : public IdempotencyStore {
public:
  IdempotencyRedis(int done_ttl_sec = 600, int in_flight_ttl_sec = 10);
  ~IdempotencyRedis() override = default;

  IdempotencyRedis(const IdempotencyRedis &) = delete;
  IdempotencyRedis &operator=(const IdempotencyRedis &) = delete;

  bool connect(const std::string &redis_ip, int redis_port);

  IdemResult claim(const std::string &key, std::string *cached_body) override;
  void complete(const std::string &key, const std::string &body) override;
  void abort(const std::string &key) override;

private:
  RedisClient redis_;
  int done_ttl_sec_;
  int in_flight_ttl_sec_;

  static constexpr char kInFlightPrefix = '\x00';
  static constexpr char kDonePrefix = '\x01';
};