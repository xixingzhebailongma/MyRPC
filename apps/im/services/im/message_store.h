#pragma once
#include "im.pb.h"
#include "redis_client.h"
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>
class MessageStore {
public:
  //清理/保留期常量（秒）
  static constexpr int kOfflineTtlSec = 604800; //离线消息保留 7 天
  static constexpr int kRequestDedupTtlSec =
      120; // 幂等去重键 TTL：覆盖重试窗口

  explicit MessageStore(const std::string &server_id, uint64_t worker_id);

  //连接Redis
  bool connect(const std::string &redis_ip, int redis_port);
  // Pub/Sub 发布（转发到内部 RedisClient）
  bool publish(const std::string &channel, const std::string &msg);

  // tryClaimRequest 的返回值：区分「首次」「重复」「出错(Redis 不可用)」
  enum class ClaimResult {
    kFirst,     // 首次处理成功
    kDuplicate, // 已存在（重复请求）
    kError,     // Redis 不可用 / 出错
  };

  //幂等去重（按发送者隔离）：首次处理返回 kFirst 并记录 msg_id；
  //重复请求返回 kDuplicate，并把之前分配的 msg_id 填到 *existing_msg_id；
  // Redis 不可用返回 kError（上层应显式返回失败，而非误判为重复）。
  ClaimResult tryClaimRequest(const std::string &from_user_id,
                              const std::string &client_request_id,
                              const std::string &msg_id,
                              std::string *existing_msg_id,
                              int ttl_sec = kRequestDedupTtlSec);

  // 回滚去重键（投递/持久化失败时调用，允许客户端重试重投）
  bool releaseRequestClaim(const std::string &from_user_id,
                           const std::string &client_request_id);

  //离线消息：存储/拉取/清除
  bool storeOfflineMessage(const std::string &user_id,
                           const im::ChatMessage &msg,
                           int ttl_sec = kOfflineTtlSec);
  std::vector<im::ChatMessage> fetchOfflineMessages(const std::string &user_id);
  void clearOfflineMessages(const std::string &user_id);

  //状态追踪
  void markStatus(const std::string &msg_id, im::MessageStatus status);
  im::MessageStatus getStatus(const std::string &msg_id);

  // ID生成
  std::string generateMsgId();

  // 重试计数器（deliver_server 按 stream entry_id 计数），返回自增后的值
  int64_t incrementRetryCount(const std::string &entry_id, int ttl_sec = 3600);

private:
  RedisClient redis_;
  std::string server_id_;
  // ===== Snowflake 64-bit ID 生成 =====
  // [1 位符号=0][41 位毫秒时间戳][10 位 worker_id][12 位毫秒内序列]
  // worker_id 仅为唯一性，绝不从 msg_id 反解，因此 ID 与节点身份解耦。
  static constexpr uint64_t kEpochMs =
      1577836800000ULL; // 2020-01-01 00:00:00 UTC
  static constexpr uint64_t kWorkerIdBits = 10;
  static constexpr uint64_t kSequenceBits = 12;
  static constexpr uint64_t kMaxWorkerId = (1ULL << kWorkerIdBits) - 1;  // 1023
  static constexpr uint64_t kSequenceMask = (1ULL << kSequenceBits) - 1; // 4095
  static constexpr uint64_t kWorkerIdShift = kSequenceBits;              // 12
  static constexpr uint64_t kTimestampShift =
      kWorkerIdBits + kSequenceBits; // 22

  uint64_t worker_id_;
  std::mutex id_mutex_; // 保护 last_timestamp_ms_ + sequence_
  uint64_t last_timestamp_ms_{0};
  uint64_t sequence_{0};

  uint64_t nowMs() const;

  // Redis Key 构造辅助方法
  std::string requestKey(const std::string &from_user_id,
                         const std::string &client_request_id) const;
  std::string
  retryKey(const std::string &entry_id) const; // "msg:retry:" + entry_id
  std::string offlineKey(const std::string &user_id) const;
  std::string statusKey(const std::string &msg_id) const;
};