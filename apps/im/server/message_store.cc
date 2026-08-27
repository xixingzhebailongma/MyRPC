#include "message_store.h"
#include "Logger.h"
#include "im.pb.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>
// ========== 私有辅助：Redis Key 构造 ==========
/*Key 都集中在 message_store.cc 顶部拼接，命名规范统一：

  ┌──────┬────────────┬──────┬──────┐
  │    方法    │     生成的 Key     │  类型  │     用途     │
  ├────────────┼────────────────────┼────────┼──────────────┤
   │ requestKey │ msg:req:{from}:{req_id} │ String │
  按发送者隔离的幂等标记（值=真实msg_id）│
  ├────────────┼───────────────────────┼────────┼──────────────────────┤
  │ offlineKey │ msg:offline:{user_id} │ List   │ 某用户的离线消息队列 │
  ├────────────┼───────────────────────┼────────┼──────────────────────┤
  │ statusKey        │ msg:status:{msg_id}     │ String │ 某消息的投递状态 │
  ├──────────────────┼─────────────────────────┼────────┼─────────────────────────────────────────────────────────────────┤
  │ pendingQueueKey  │ msg:pending:{server_id}              │ ZSet   │
  按服务器划分的待重试队列（member=msg_id，score=下次重试时间戳） │
  ├──────────────────┼──────────────────────────────────────┼────────┼─────────────────────────────────────────────────────────────────┤
  │ pendingRecordKey │ msg:pending:rec:{server_id}:{msg_id} │ String │ 序列化的
  PendingMessageRecord 详情                              │
  ├──────────────────┼──────────────────────────────────────┼────────┼─────────────────────────────────────────────────────────────────┤
  │ kIdCounterKey    │ msg:id:counter                       │ String │ 全局 ID
  计数器  */
std::string
MessageStore::requestKey(const std::string &from_user_id,
                         const std::string &client_request_id) const {
  return "msg:req:" + from_user_id + ":" + client_request_id;
}
std::string MessageStore::pendingQueueKey() const {
  return "msg:pending:" + server_id_;
}

std::string MessageStore::pendingRecordKey(const std::string &msg_id) const {
  return "msg:pending:rec:" + server_id_ + ":" + msg_id;
}
std::string MessageStore::offlineKey(const std::string &user_id) const {
  return "msg:offline:" + user_id;
}

std::string MessageStore::statusKey(const std::string &msg_id) const {
  return "msg:status:" + msg_id;
}

// ========== 构造 & 连接 ==========
MessageStore::MessageStore(const std::string &server_id)
    : server_id_(server_id) {}

bool MessageStore::connect(const std::string &redis_ip, int redis_port) {
  bool ok = redis_.connect(redis_ip, redis_port);
  if (!ok) {
    LOG_ERROR("MessageStore: failed to connect to Redis at %s:%d",
              redis_ip.c_str(), redis_port);
  } else {
    LOG_INFO("MessageStore: connected to Redis at %s:%d", redis_ip.c_str(),
             redis_port);
  }
  return ok;
}

bool MessageStore::publish(const std::string &channel, const std::string &msg) {
  return redis_.publish(channel, msg);
}

//========== 去重 ==========

bool MessageStore::tryClaimRequest(const std::string &from_user_id,
                                   const std::string &client_request_id,
                                   const std::string &msg_id,
                                   std::string *existing_msg_id, int ttl_sec) {
  std::string key = requestKey(from_user_id, client_request_id);
  // 首次处理：SETNX 成功，值存真实 msg_id，供重复请求取回
  bool first = redis_.setnx(key, msg_id);
  if (first) {
    redis_.expire(key, ttl_sec);
    return true;
  }
  // 重复请求：取回之前分配的 msg_id，保证响应幂等
  if (existing_msg_id) {
    *existing_msg_id = redis_.get(key);
  }
  return false;
}

// ========== 离线消息 ==========
bool MessageStore::storeOfflineMessage(const std::string &user_id,
                                       const im::ChatMessage &msg,
                                       int ttl_sec) {
  std::string data;
  if (!msg.SerializeToString(&data)) {
    LOG_ERROR("MessageStore: failed to serialize ChatMessage for user %s",
              user_id.c_str());
    return false;
  }
  std::string key = offlineKey(user_id);
  bool ok = redis_.lpush(key, data);
  // 滑动窗口：每次写入刷新 TTL，用户长期不登录时整条列表被回收
  if (ok && !redis_.expire(key, ttl_sec)) {
    LOG_ERROR("MessageStore: failed to set TTL on offline list for user %s",
              user_id.c_str());
  }
  return ok;
}

std::vector<im::ChatMessage>
MessageStore::fetchOfflineMessages(const std::string &user_id) {
  std::string key = offlineKey(user_id);
  std::vector<std::string> raw = redis_.lrange(key, 0, -1);
  std::vector<im::ChatMessage> result;
  result.reserve(raw.size());

  // LPUSH 是后进先出，反向遍历恢复为 FIFO（先进先出）
  for (auto it = raw.rbegin(); it != raw.rend(); ++it) {
    im::ChatMessage msg;
    if (msg.ParseFromString(*it)) {
      result.push_back(std::move(msg));
    } else {
      LOG_ERROR("MessageStore: failed to parse offline message for user %s",
                user_id.c_str());
    }
  }
  return result;
}

void MessageStore::clearOfflineMessages(const std::string &user_id) {
  std::string key = offlineKey(user_id);
  redis_.del(key);
}

//========== 状态追踪 ==========

void MessageStore::markStatus(const std::string &msg_id,
                              im::MessageStatus status) {
  std::string key = statusKey(msg_id);
  std::string value = std::to_string(static_cast<int>(status));
  redis_.set(key, value);
  redis_.expire(key, 86400);
}

im::MessageStatus MessageStore::getStatus(const std::string &msg_id) {
  std::string key = statusKey(msg_id);
  std::string value = redis_.get(key);
  if (value.empty()) {
    return im::MessageStatus::SENDING; //未找到，返回默认值
  }
  int int_val = std::stoi(value);
  //范围校验
  if (int_val >= static_cast<int>(im::MessageStatus::SENDING) &&
      int_val <= static_cast<int>(im::MessageStatus::FAILED)) {
    return static_cast<im::MessageStatus>(int_val);
  }
  LOG_ERROR("MessageStore: invalid status value '%s' for msg_id %s",
            value.c_str(), msg_id.c_str());
  return im::MessageStatus::SENDING;
}

// ========== ID 生成 ==========

std::string MessageStore::generateMsgId() {
  int64_t counter = redis_.incr(kIdCounterKey);
  if (counter < 0) {
    LOG_ERROR("MessageStore: INCR failed for message ID counter");
    return ""; // 失败返回空字符串，调用方需检查
  }
  return server_id_ + "_" + std::to_string(counter);
}

// ========== 待确认队列（pending） ==========
bool MessageStore::addPending(const im::ChatMessage &msg, int64_t next_retry_ms,
                              int ttl_sec) {
  im::PendingMessageRecord record;
  *record.mutable_msg() = msg;
  record.set_retry_count(0); // 首次加入，重试次数为 0

  std::string data;
  if (!record.SerializeToString(&data)) {
    LOG_ERROR("MessageStore: failed to serialize pending record for %s",
              msg.msg_id().c_str());
    return false;
  }
  // ZADD：member=msg_id，score=下次重试时间（覆盖语义，重复加入也没关系）
  if (!redis_.zadd(pendingQueueKey(), static_cast<double>(next_retry_ms),
                   msg.msg_id())) {
    return false;
  }
  if (!redis_.set(pendingRecordKey(msg.msg_id()), data)) {
    return false;
  }
  redis_.expire(pendingRecordKey(msg.msg_id()),
                ttl_sec); // 兜底：防止孤儿 record 永久残留
  return true;
}

std::vector<im::PendingMessageRecord>
MessageStore::fetchDuePending(int64_t now_ms, int limit) {
  // 拉出所有 score <= now_ms 的 member（即 msg_id）
  std::vector<std::string> due_ids = redis_.zrangebyscore(
      pendingQueueKey(), 0, static_cast<double>(now_ms), limit);
  std::vector<im::PendingMessageRecord> result;
  result.reserve(due_ids.size());

  for (const auto &id : due_ids) {
    std::string data = redis_.get(pendingRecordKey(id));
    if (data.empty()) {
      // record 已丢失，member 成为僵尸：从 zset 移除，避免永久泄漏
      redis_.zrem(pendingQueueKey(), id);
      LOG_INFO("MessageStore: removed zombie pending member %s", id.c_str());
      continue;
    }
    im::PendingMessageRecord record;
    if (record.ParseFromString(data)) {
      result.push_back(std::move(record));
    } else {
      // 解析失败：坏 record 同样会永久卡住，一并清理
      LOG_ERROR("MessageStore: failed to parse pending record for %s",
                id.c_str());
      redis_.zrem(pendingQueueKey(), id);
      redis_.del(pendingRecordKey(id));
    }
  }
  return result;
}

bool MessageStore::updatePending(const im::ChatMessage &msg, int retry_count,
                                 int64_t next_retry_ms, int ttl_sec) {
  im::PendingMessageRecord record;
  *record.mutable_msg() = msg;
  record.set_retry_count(retry_count);

  std::string data;
  if (!record.SerializeToString(&data)) {
    LOG_ERROR("MessageStore: failed to serialize pending record for %s",
              msg.msg_id().c_str());
    return false;
  }

  // ZADD 用同一 member 重新写入 = 覆盖 score（延期到新的到期时间）
  if (!redis_.zadd(pendingQueueKey(), static_cast<double>(next_retry_ms),
                   msg.msg_id())) {
    return false;
  }
  if (!redis_.set(pendingRecordKey(msg.msg_id()), data)) {
    return false;
  }
  redis_.expire(pendingRecordKey(msg.msg_id()), ttl_sec); // 每次重试刷新兜底
  return true;
}

bool MessageStore::removePending(const std::string &msg_id) {
  bool ok1 = redis_.zrem(pendingQueueKey(), msg_id);
  bool ok2 = redis_.del(pendingRecordKey(msg_id));
  return ok1 && ok2;
}