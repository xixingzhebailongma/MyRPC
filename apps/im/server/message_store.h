#pragma once
#include "im.pb.h"
#include "redis_client.h"
#include <string>
#include <vector>

class MessageStore {
public:
  //清理/保留期常量（秒）
  static constexpr int kOfflineTtlSec = 604800;    //离线消息保留 7 天
  static constexpr int kPendingRecordTtlSec = 300; // pending 记录兜底 5 分钟

  explicit MessageStore(const std::string &server_id);

  //连接Redis
  bool connect(const std::string &redis_ip, int redis_port);
  // Pub/Sub 发布（转发到内部 RedisClient）
  bool publish(const std::string &channel, const std::string &msg);

  //幂等去重（按发送者隔离）：首次处理返回 true 并记录 msg_id；
  //重复请求返回 false，并把之前分配的 msg_id 填到 *existing_msg_id。
  bool tryClaimRequest(const std::string &from_user_id,
                       const std::string &client_request_id,
                       const std::string &msg_id, std::string *existing_msg_id,
                       int ttl_sec = 3600);

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
  // 待确认队列（pending）：ZSET 存到期时间，String 存 PendingMessageRecord
  bool addPending(const im::ChatMessage &msg, int64_t next_retry_ms,
                  int ttl_sec = kPendingRecordTtlSec);
  std::vector<im::PendingMessageRecord> fetchDuePending(int64_t now_ms,
                                                        int limit = 100);
  bool updatePending(const im::ChatMessage &msg, int retry_count,
                     int64_t next_retry_ms, int ttl_sec = kPendingRecordTtlSec);
  bool removePending(const std::string &msg_id);

private:
  RedisClient redis_;
  std::string server_id_;

  // Redis Key 构造辅助方法
  std::string requestKey(const std::string &from_user_id,
                         const std::string &client_request_id) const;
  std::string pendingQueueKey() const; // "msg:pending:" + server_id_
  std::string pendingRecordKey(const std::string &msg_id)
      const; // "msg:pending:rec:" + server_id_ + ":" + msg_id
  std::string offlineKey(const std::string &user_id) const;
  std::string statusKey(const std::string &msg_id) const;

  static constexpr const char *kIdCounterKey = "msg:id:counter";
};