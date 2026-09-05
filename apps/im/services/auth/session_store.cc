#include "session_store.h"
#include "Logger.h"
#include "auth.pb.h"
#include <algorithm>
#include <chrono>
#include <exception>
#include <random>
#include <unordered_map>
namespace {
// 当前 unix 时间戳（秒）。和 user_manager.cc 取 login_time 的口径一致。
int64_t nowSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// ---------- token 生成 ----------
// 复用 user_dao.cc::generateSalt() 的手法：thread_local mt19937_64，每线程
// 独立种子，产出小写 hex。这里做成变长，供 64 hex（at/rt）与 32
// hex（sid/ticket）共用。

std::string generateToken(int hex_len) {
  thread_local std::mt19937_64 gen{[] {
    std::random_device rd;
    return (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
  }()};
  static const char hex[] = "0123456789abcdef";
  std::uniform_int_distribution<uint64_t> dist;

  std::string out;
  out.reserve(hex_len);
  int remaining_chars = hex_len;
  while (remaining_chars > 0) {
    uint64_t v = dist(gen);
    // 本次最多使用 8 个字节（16 个字符）
    int bytes_to_use = std::min(8, (remaining_chars + 1) / 2);
    for (int i = 0; i < bytes_to_use; ++i) {
      // 从高位到低位取字节
      unsigned char byte =
          static_cast<unsigned char>((v >> (56 - i * 8)) & 0xff);
      // 判断是否还需要完整两个字符，处理奇数 hex_len 时最后一个字节只取高 4 位
      if (remaining_chars >= 2) {
        out.push_back(hex[byte >> 4]);
        out.push_back(hex[byte & 0x0f]);
        remaining_chars -= 2;
      } else {
        // 只差一个字符，取高 4 位
        out.push_back(hex[byte >> 4]);
        remaining_chars -= 1;
      }
    }
  }
  return out;
}
// ---------- createSession 的原子写 Lua 脚本 ----------
// KEYS[1]=session hash, KEYS[2]=access key, KEYS[3]=refresh key,
// KEYS[4]=dev 索引,  KEYS[5]=all 索引
// ARGV[1..14] 布局见 createSession 里 args 向量（必须一一对应）
const char *kCreateSessionScript = R"lua(
  redis.call('HSET', KEYS[1],
    'session_id', ARGV[1], 'user_id', ARGV[2], 'username', ARGV[3],
    'device_id', ARGV[4], 'device_type', ARGV[5], 'client_ip', ARGV[6],
    'created_at', ARGV[7], 'refresh_expires_at', ARGV[8],
    'access_token', ARGV[9], 'refresh_token', ARGV[10])
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[11]))
  redis.call('SETEX', KEYS[2], tonumber(ARGV[12]), ARGV[1])
  redis.call('SETEX', KEYS[3], tonumber(ARGV[13]), ARGV[1])
  redis.call('ZADD', KEYS[4], tonumber(ARGV[14]), ARGV[1])
  redis.call('ZADD', KEYS[5], tonumber(ARGV[14]), ARGV[1])
  redis.call('EXPIRE', KEYS[4], tonumber(ARGV[11]))
  redis.call('EXPIRE', KEYS[5], tonumber(ARGV[11]))
  return 1
  )lua";
} // namespace

bool SessionStore::connect(const std::string &redis_ip, int redis_port) {
  bool ok = redis_.connect(redis_ip, redis_port);
  if (!ok) {
    LOG_ERROR("SessionStore: failed to connect to Redis at %s:%d",
              redis_ip.c_str(), redis_port);
  } else {
    LOG_INFO("SessionStore: connected to Redis at %s:%d", redis_ip.c_str(),
             redis_port);
  }
  return ok;
}

// ---------- Redis key 拼装 ----------
std::string SessionStore::sessionKey(const std::string &sid) const {
  return "auth:session:" + sid;
}
std::string SessionStore::accessKey(const std::string &at) const {
  return "auth:access:" + at;
}
std::string SessionStore::refreshKey(const std::string &rt) const {
  return "auth:refresh:" + rt;
}
std::string SessionStore::ticketKey(const std::string &tk) const {
  return "auth:ticket:" + tk;
}
std::string SessionStore::userSessionsKey(const std::string &uid) const {
  return "auth:user:" + uid + ":sessions";
}
std::string SessionStore::userDeviceKey(const std::string &uid,
                                        int device_type) const {
  return "auth:user:" + uid + ":device:" + std::to_string(device_type);
}

// ---------- 字段表 -> SessionInfo ----------
// 刻意 *不* 回填 access_token / refresh_token：token 签发后，除了登录/续期
// 响应体外，任何接口都不该再把它回传一遍。

auth::SessionInfo SessionStore::mapToSessionInfo(
    const std::unordered_map<std::string, std::string> &f) {
  auth::SessionInfo info;
  auto get = [&f](const std::string &k) -> const std::string & {
    static const std::string empty;
    auto it = f.find(k);
    return it == f.end() ? empty : it->second;
  };
  info.set_session_id(get("session_id"));
  info.set_user_id(get("user_id"));
  info.set_username(get("username"));
  info.set_device_id(get("device_id"));
  info.set_device_type(static_cast<auth::DeviceType>(static_cast<int>(
      std::stoi(get("device_type"))))); // 越界值 proto 会接受，不抛
  info.set_client_ip(get("client_ip"));
  try {
    info.set_created_at(std::stoll(get("created_at")));
    info.set_refresh_expires_at(std::stoll(get("refresh_expires_at")));
  } catch (const std::exception &) {
    // 时间字段损坏按 0 处理；不阻断整个 session 的读取。
  }
  return info;
}

// ---------- 创建会话（含同端互踢） ----------
bool SessionStore::createSession(const auth::LoginRequest &req,
                                 auth::LoginResponse *resp) {
  // username 即 user_id（项目约定，见 im.proto 注释）
  const std::string &user_id = req.username();
  int64_t now = nowSeconds();

  std::string sid = generateToken(32);
  std::string at = generateToken(64);
  std::string rt = generateToken(64);
  int64_t refresh_expires_at = now + kRefreshTtlSec;

  int dtype = static_cast<int>(req.device_type());

  // ①+②+④ 原子写：session hash + 两个 token key + 两个索引，一次 EVAL（1 次
  // RTT）
  std::vector<std::string> keys = {
      sessionKey(sid),          accessKey(at),
      refreshKey(rt),           userDeviceKey(user_id, dtype),
      userSessionsKey(user_id),
  };
  std::vector<std::string> args = {
      sid,                                // ARGV[1]
      user_id,                            // ARGV[2]
      req.username(),                     // ARGV[3]
      req.device_id(),                    // ARGV[4]
      std::to_string(dtype),              // ARGV[5]
      req.client_ip(),                    // ARGV[6]
      std::to_string(now),                // ARGV[7]
      std::to_string(refresh_expires_at), // ARGV[8]
      at,                                 // ARGV[9]
      rt,                                 // ARGV[10]
      std::to_string(kRefreshTtlSec),     // ARGV[11] session_ttl
      std::to_string(kAccessTtlSec),      // ARGV[12] access_ttl
      std::to_string(kRefreshTtlSec),     // ARGV[13] refresh_ttl
      std::to_string(refresh_expires_at), // ARGV[14] score
  };
  if (!redis_.eval(kCreateSessionScript, keys, args)) {
    LOG_ERROR("SessionStore::createSession: eval write failed, sid=%s",
              sid.c_str());
    return false;
  }

  // ③ 同端互踢（读结果决定动作，保留在 C++）
  std::string dev_key = userDeviceKey(user_id, dtype);
  int64_t removed =
      redis_.zremrangebyscore(dev_key, -1.0, static_cast<double>(now));
  if (removed < 0) {
    LOG_ERROR("SessionStore::createSession: zremrangebyscore failed, key=%s",
              dev_key.c_str());
  }
  for (const std::string &old_sid :
       redis_.zrangebyscore(dev_key, now, now + kRefreshTtlSec)) {
    if (old_sid != sid) {
      revokeSession(old_sid);
    }
  }

  // ⑤ 回填响应
  resp->set_success(true);
  resp->set_access_token(at);
  resp->set_refresh_token(rt);
  resp->set_expires_in(kAccessTtlSec);
  *resp->mutable_session() = mapToSessionInfo({
      {"session_id", sid},
      {"user_id", user_id},
      {"username", req.username()},
      {"device_id", req.device_id()},
      {"device_type", std::to_string(dtype)},
      {"client_ip", req.client_ip()},
      {"created_at", std::to_string(now)},
      {"refresh_expires_at", std::to_string(refresh_expires_at)},
  });
  return true;
}

// ---------- 校验 access token ----------
bool SessionStore::verifyAccessToken(const std::string &access_token,
                                     auth::SessionInfo *out) {
  std::string sid = redis_.get(accessKey(access_token));
  if (sid.empty()) {
    return false; // token 不存在或已过期
  }
  auto fields = redis_.hgetall(sessionKey(sid));
  if (fields.empty()) {
    // 孤儿 token key：session hash 已消失。顺手清掉，防止越积越多。
    redis_.del(accessKey(access_token));
    return false;
  }
  // 纵深防御：token key 指向的 session 里，存的 access_token 也必须一致
  auto it = fields.find("access_token");
  if (it == fields.end() || it->second != access_token) {
    return false;
  }
  if (out) {
    *out = mapToSessionInfo(fields);
  }
  return true;
}

// ---------- 续期 + 轮换 ----------
bool SessionStore::refresh(const std::string &refresh_token,
                           auth::RefreshResponse *resp) {
  // 1. 定位 sid
  std::string sid = redis_.get(refreshKey(refresh_token));
  if (sid.empty()) {
    return false;
  }

  // 2. 读 session + 校验
  auto fields = redis_.hgetall(sessionKey(sid));
  if (fields.empty()) {
    redis_.del(refreshKey(refresh_token)); // 孤儿 refresh key，顺手清掉
    return false;
  }
  auto rt_it = fields.find("refresh_token");
  if (rt_it == fields.end() || rt_it->second != refresh_token) {
    return false; // 请求的 rt 和 session 里存的 rt 不一致
  }

  int64_t now = nowSeconds();
  int64_t old_refresh_expires_at = 0;
  try {
    auto it = fields.find("refresh_expires_at");
    if (it != fields.end()) {
      old_refresh_expires_at = std::stoll(it->second);
    }
  } catch (const std::exception &) {
    old_refresh_expires_at = 0;
  }
  if (now >= old_refresh_expires_at) {
    revokeSession(sid); // 已过期，清理掉
    return false;
  }

  // 3. 旧 token 立即失效
  auto at_it = fields.find("access_token");
  if (at_it != fields.end()) {
    redis_.del(accessKey(at_it->second));
  }
  redis_.del(refreshKey(refresh_token));

  // 4. 生成新 token 并写入
  std::string new_at = generateToken(64);
  std::string new_rt = generateToken(64);
  int64_t new_refresh_expires_at = now + kRefreshTtlSec;
  if (!redis_.setex(accessKey(new_at), sid, kAccessTtlSec) ||
      !redis_.setex(refreshKey(new_rt), sid, kRefreshTtlSec)) {
    LOG_ERROR("SessionStore::refresh: setex new token key failed, sid=%s",
              sid.c_str());
    return false;
  }

  // 5. 回写 session hash（滑动 7 天窗口）
  bool ok = true;
  ok = ok && redis_.hset(sessionKey(sid), "access_token", new_at);
  ok = ok && redis_.hset(sessionKey(sid), "refresh_token", new_rt);
  ok = ok && redis_.hset(sessionKey(sid), "refresh_expires_at",
                         std::to_string(new_refresh_expires_at));
  ok = ok && redis_.expire(sessionKey(sid), kRefreshTtlSec);
  if (!ok) {
    LOG_ERROR("SessionStore::refresh: rewrite session hash failed, sid=%s",
              sid.c_str());
    return false;
  }

  // 6. 更新索引（派生数据，尽力而为）
  std::string uid = fields["user_id"];
  std::string dtype_str = fields["device_type"];
  std::string all_key = userSessionsKey(uid);
  std::string dev_key = userDeviceKey(uid, std::stoi(dtype_str));
  double score = static_cast<double>(new_refresh_expires_at);
  redis_.zadd(all_key, score, sid);
  redis_.zadd(dev_key, score, sid);
  redis_.expire(all_key, kRefreshTtlSec);
  redis_.expire(dev_key, kRefreshTtlSec);

  // 7. 回填响应
  resp->set_success(true);
  resp->set_access_token(new_at);
  resp->set_refresh_token(new_rt);
  resp->set_expires_in(kAccessTtlSec);
  return true;
}

// ---------- 票据（长连接握手，一次性） ----------
bool SessionStore::issueTicket(const std::string &access_token,
                               std::string *out_ticket) {
  std::string sid = redis_.get(accessKey(access_token));
  if (sid.empty()) {
    return false;
  }
  // 纵深防御：确认 session 仍有效且 access_token 与 hash 内一致
  auto fields = redis_.hgetall(sessionKey(sid));
  if (fields.empty()) {
    return false;
  }
  auto it = fields.find("access_token");
  if (it == fields.end() || it->second != access_token) {
    return false;
  }
  std::string tk = generateToken(32);
  if (!redis_.setex(ticketKey(tk), sid, kTicketTtlSec)) {
    LOG_ERROR("SessionStore::issueTicket: setex ticket key failed, sid=%s",
              sid.c_str());
    return false;
  }
  if (out_ticket) {
    *out_ticket = tk;
  }
  return true;
}

bool SessionStore::resolveTicket(const std::string &ticket,
                                 const std::string &gateway_id,
                                 uint64_t conn_id, auth::SessionInfo *out) {
  // 原子"取值+删除"：GETDEL 一次性消费，并发下只有一个请求能拿到 sid，
  std::string sid = redis_.getdel(ticketKey(ticket));
  if (sid.empty()) {
    return false; // 票不存在、已被消费或已过期
  }
  // 记录票 → 连接绑定（短 TTL，审计/加固用；真正的防重放靠上面 GETDEL）
  redis_.setex("auth:ticket:bind:" + ticket,
               gateway_id + ":" + std::to_string(conn_id), kTicketTtlSec);
  auto fields = redis_.hgetall(sessionKey(sid));
  if (fields.empty()) {
    return false;
  }
  if (out) {
    *out = mapToSessionInfo(fields);
  }
  return true;
}
// ---------- 多端会话管理 ----------
std::vector<auth::SessionInfo>
SessionStore::listSessions(const std::string &user_id) {
  std::vector<auth::SessionInfo> result;
  std::string all_key = userSessionsKey(user_id);
  int64_t now = nowSeconds();
  // 索引自清理：先清掉已过期的成员，避免读到悬垂 sid
  redis_.zremrangebyscore(all_key, -1.0, static_cast<double>(now));
  auto sids = redis_.zrangebyscore(all_key, now, now + kRefreshTtlSec);
  for (const auto &sid : sids) {
    auto fields = redis_.hgetall(sessionKey(sid));
    if (!fields.empty()) {
      result.push_back(mapToSessionInfo(fields));
    }
  }
  return result;
}

bool SessionStore::kickSession(const std::string &user_id,
                               const std::string &session_id) {
  // 校验 sid 确实属于该 user，防止跨用户踢
  auto fields = redis_.hgetall(sessionKey(session_id));
  if (fields.empty()) {
    return false;
  }
  if (fields["user_id"] != user_id) {
    return false;
  }
  return revokeSession(session_id);
}

int SessionStore::revokeAllSessions(const std::string &user_id) {
  std::string all_key = userSessionsKey(user_id);
  int64_t now = nowSeconds();
  redis_.zremrangebyscore(all_key, -1.0, static_cast<double>(now));
  auto sids = redis_.zrangebyscore(all_key, now, now + kRefreshTtlSec);
  int kicked = 0;
  for (const auto &sid : sids) {
    if (revokeSession(sid)) {
      ++kicked;
    }
  }
  // 清掉总的索引 key（device 索引由 revokeSession 逐个 zrem + 自身 7 天 TTL
  // 兜底）
  redis_.del(all_key);
  return kicked;
}

bool SessionStore::revokeSession(const std::string &session_id) {
  // 1. 读 session hash
  auto fields = redis_.hgetall(sessionKey(session_id));
  if (fields.empty()) {
    return false;
  }

  std::string user_id = fields["user_id"];
  std::string at = fields["access_token"];
  std::string rt = fields["refresh_token"];

  // 2. 删 session hash + 两个 token key
  redis_.del(sessionKey(session_id));
  if (!at.empty())
    redis_.del(accessKey(at));
  if (!rt.empty())
    redis_.del(refreshKey(rt));

  // 3. 从两个 ZSET 索引里 zrem 掉这个 sid
  redis_.zrem(userSessionsKey(user_id), session_id);
  // device_type 转 int 用 try/catch 兜底（和 mapToSessionInfo 口径一致）
  try {
    int dtype = std::stoi(fields["device_type"]);
    redis_.zrem(userDeviceKey(user_id, dtype), session_id);
  } catch (const std::exception &) {
    // 索引是派生数据，出错不影响主删除
  }
  return true;
}

bool SessionStore::logout(const std::string &access_token) {
  std::string sid = redis_.get(accessKey(access_token));
  if (sid.empty()) {
    return true; // token 不存在也算成功（幂等）
  }
  return revokeSession(sid);
}