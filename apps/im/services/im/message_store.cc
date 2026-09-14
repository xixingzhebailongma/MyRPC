#include "message_store.h"
#include "Logger.h"
#include "im.pb.h"
#include "mq_constants.h"
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
std::string MessageStore::retryKey(const std::string &entry_id) const {
  return "msg:retry:" + entry_id;
}
std::string MessageStore::offlineKey(const std::string &user_id) const {
  return "msg:offline:" + user_id;
}

std::string MessageStore::statusKey(const std::string &msg_id) const {
  return "msg:status:" + msg_id;
}

// ========== 构造 & 连接 ==========
MessageStore::MessageStore(const std::string &server_id, uint64_t worker_id)
    : server_id_(server_id), worker_id_(worker_id & kMaxWorkerId) {}

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

//========== 去重 ==========

MessageStore::ClaimResult MessageStore::tryClaimRequest(
    const std::string &from_user_id, const std::string &client_request_id,
    const std::string &msg_id, std::string *existing_msg_id, int ttl_sec) {
  std::string key = requestKey(from_user_id, client_request_id);
  // 首次处理：原子 SET NX EX，值存真实 msg_id，供重复请求取回
  SetResult r = redis_.setNxEx(key, msg_id, ttl_sec);
  if (r == SetResult::kSet) {
    return ClaimResult::kFirst;
  }
  if (r == SetResult::kError) {
    // Redis 不可用：不当作重复，交由上层返回失败，避免静默丢消息
    return ClaimResult::kError;
  }
  // kExists：重复请求，取回之前分配的 msg_id，保证响应幂等
  if (existing_msg_id) {
    *existing_msg_id = redis_.get(key);
  }
  return ClaimResult::kDuplicate;
}

bool MessageStore::releaseRequestClaim(const std::string &from_user_id,
                                       const std::string &client_request_id) {
  return redis_.del(requestKey(from_user_id, client_request_id));
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
  // LPUSH + 滑动窗口刷新 TTL，合并成 1 次 RTT
  auto pipe = redis_.pipeline();
  pipe.lpush(key, data).expire(key, ttl_sec);
  return pipe.flush();
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
  auto pipe = redis_.pipeline();
  pipe.set(key, value).expire(key, 86400);
  pipe.flush();
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

uint64_t MessageStore::nowMs() const {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

std::string MessageStore::generateMsgId() {
  std::lock_guard<std::mutex> lock(id_mutex_);
  uint64_t now = nowMs();

  // 时钟回拨保护：等待系统时间追平上一次生成时间戳，避免产生重复 ID
  while (now < last_timestamp_ms_) {
    now = nowMs();
  }

  if (now == last_timestamp_ms_) {
    sequence_ = (sequence_ + 1) & kSequenceMask;
    if (sequence_ == 0) {
      // 同一毫秒内序列耗尽（>4096/ms），自旋等到下一毫秒
      do {
        now = nowMs();
      } while (now <= last_timestamp_ms_);
    }
  } else {
    sequence_ = 0;
  }
  last_timestamp_ms_ = now;

  uint64_t id = ((now - kEpochMs) << kTimestampShift) |
                ((worker_id_ & kMaxWorkerId) << kWorkerIdShift) | sequence_;
  return std::to_string(id);
}

// ========== 重试计数（deliver_server 用，key 按 stream entry_id 隔离）
// ==========
int64_t MessageStore::incrementRetryCount(const std::string &entry_id,
                                          int ttl_sec) {
  std::string key = retryKey(entry_id);
  int64_t n = redis_.incr(key);
  if (n > 0) {
    // 每次自增刷新 TTL：消息收尾后 side key 会随 TTL 自动过期，不残留
    redis_.expire(key, ttl_sec);
  }
  return n;
}