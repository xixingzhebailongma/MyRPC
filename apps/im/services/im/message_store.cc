#include "message_store.h"
#include "Logger.h"
#include "im.pb.h"
#include "mq_constants.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <mutex>
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
  │ snowflakeKey     │ snowflake:last_ts:{worker_id}       │ String │ Snowflake
  最大时间戳（新毫秒发号前同步 SET）  */
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
MessageStore::MessageStore(const std::string &server_id, uint64_t worker_id,
                           bool enable_snowflake)
    : server_id_(server_id), gen_(worker_id),
      snowflake_enabled_(enable_snowflake) {
  // 新毫秒发号前同步持久化最大时间戳；RedisClient 内部 4 连接池、acquire 有互斥，跨线程安全。
  gen_.SetPersistFn([this](uint64_t ts) {
    return redis_.set(snowflakeKey(), std::to_string(ts));
  });
}

bool MessageStore::connect(const std::string &redis_ip, int redis_port) {
  bool ok = redis_.connect(redis_ip, redis_port);
  if (!ok) {
    LOG_ERROR("MessageStore: failed to connect to Redis at %s:%d",
              redis_ip.c_str(), redis_port);
    return false;
  }
  LOG_INFO("MessageStore: connected to Redis at %s:%d", redis_ip.c_str(),
           redis_port);

  // 仅发号方（im_server）初始化 Snowflake 逻辑时钟；deliver_server 等不发号方跳过，
  // 避免污染 snowflake:last_ts:{worker_id} 的首次部署判定。
  if (snowflake_enabled_) {
    // 初始化 Snowflake 逻辑时钟（只执行一次）；失败则拒绝启动，避免重启后产生重复 ID
    bool inited = false;
    std::call_once(snowflake_init_once_,
                   [&] { inited = initSnowflakeState(); });
    if (!inited) {
      LOG_ERROR("MessageStore: snowflake state init failed, refusing to start");
      return false;
    }
  }
  return true;
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

std::string MessageStore::snowflakeKey() const {
  return "snowflake:last_ts:" + std::to_string(gen_.WorkerId());
}

// 启动时从 Redis 读历史最大时间戳 seed 逻辑时钟。
// 核心原则：拿不到可信历史最大时间戳就不发号（返回 false 拒绝启动），避免重启后产生重复 ID。
bool MessageStore::initSnowflakeState() {
  std::string key = snowflakeKey();
  uint64_t now = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());

  std::string v = redis_.get(key);
  if (!v.empty()) {
    uint64_t persisted = 0;
    try {
      persisted = std::stoull(v);
    } catch (const std::exception &) {
      LOG_ERROR("MessageStore: invalid snowflake last_ts '%s'", v.c_str());
      return false;
    }
    // 防止 worker_id 复用导致时间戳虚高：persisted 远超当前时间（>1 天）时拒绝启动，
    // 需人工确认（清/改 snowflake:last_ts:{worker_id} 后再启动），避免长期等待/故障。
    constexpr uint64_t kMaxForwardDriftMs = 86400000ULL; // 1 天
    if (persisted > now + kMaxForwardDriftMs) {
      LOG_ERROR("MessageStore: snowflake last_ts %llu is %llums ahead of now %llu (worker_id possibly reused); refusing to start, manual confirmation required",
                static_cast<unsigned long long>(persisted),
                static_cast<unsigned long long>(persisted - now),
                static_cast<unsigned long long>(now));
      return false;
    }
    // seed = max(now, persisted + 1)：跳过可能已部分使用的毫秒
    uint64_t seed = std::max(now, persisted + 1);
    gen_.SetLastTimestampMs(seed);
    if (persisted >= now) {
      LOG_WARN("MessageStore: snowflake clock rollback on startup: persisted=%llu now=%llu seed=%llu",
               static_cast<unsigned long long>(persisted),
               static_cast<unsigned long long>(now),
               static_cast<unsigned long long>(seed));
    }
    return true;
  }

  // key 缺失：用 setnx 区分「首次部署」与「key 已存在但 GET 异常」
  if (redis_.setnx(key, std::to_string(now))) {
    gen_.SetLastTimestampMs(now);
    return true;
  }
  LOG_ERROR("MessageStore: snowflake last_ts key exists but could not be read, refusing to start");
  return false;
}

std::string MessageStore::generateMsgId() { return gen_.Next(); }

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