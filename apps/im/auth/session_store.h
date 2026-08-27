#pragma once

#include "auth.pb.h"
#include "redis_client.h"
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
// 会话存储：整个 Token 体系的唯一 Redis 事实来源。
// 对标 apps/im/server/message_store.h 的地位 —— 值持有 RedisClient，
// Redis 连接通过 connect() 单独建立，业务逻辑不感知连接细节。
//
// Redis 数据模型（全部 auth: 前缀）：
//   auth:session:{sid}              Hash   9 字段，唯一事实来源
//   auth:access:{at}                String -> sid
//   auth:refresh:{rt}               String -> sid
//   auth:ticket:{tk}                String -> sid（一次性，60s）
//   auth:user:{uid}:sessions        ZSET   member=sid, score=refresh_expires_at
//   auth:user:{uid}:device:{dtype}  ZSET   member=sid, score=refresh_expires_at
//
// session hash 的 9 个字段：user_id / username / device_id / device_type /
//   client_ip / created_at / refresh_expires_at / access_token /
//   refresh_token。
// 后两个是刻意冗余：revokeSession(sid) 靠它们直接找到并 DEL 两个 token key，
// 不需要反向索引。
class SessionStore {
public:
  // Token / Session TTL（秒）。全项目唯一的定义点。
  static constexpr int kAccessTtlSec = 1800;    // Access Token：30 分钟
  static constexpr int kRefreshTtlSec = 604800; // Refresh Token：7 天
  static constexpr int kTicketTtlSec = 60; // Conn Ticket：60 秒，一次性

  SessionStore() = default;

  // 连接 Redis；失败返回 false 并打日志。
  bool connect(const std::string &redis_ip, int redis_port);

  // 创建会话：生成 sid/at/rt，写 session hash + 两个 token key + 两个索引。
  // 含同端互踢：先清理该 user 同 device_type 的过期成员，再 revoke
  // 掉活跃旧会话。 成功时把 at/rt/expires_in/session 填进 *resp。
  bool createSession(const auth::LoginRequest &req, auth::LoginResponse *resp);

  // 校验 access token；合法则把 session 填进 *out。
  bool verifyAccessToken(const std::string &access_token,
                         auth::SessionInfo *out);

  // 用 access token 换一张 60 秒一次性握手票。
  bool issueTicket(const std::string &access_token, std::string *out_ticket);

  // 消费握手票：GET → 立即 DEL（一次性）→ 返回 session。
  bool resolveTicket(const std::string &ticket, auth::SessionInfo *out);

  // 用 refresh token 续期并轮换：旧 at/rt 立即失效，返回新 at/rt。
  bool refresh(const std::string &refresh_token, auth::RefreshResponse *resp);

  // 登出：定位 sid 后整体吊销。幂等，token 不存在也返回 true。
  bool logout(const std::string &access_token);

  // 列出某用户全部活跃会话（读路径会先做索引自清理）。
  std::vector<auth::SessionInfo> listSessions(const std::string &user_id);

  // 吊销某用户的指定会话。
  bool kickSession(const std::string &user_id, const std::string &session_id);

  // 吊销单个会话（sid 级原子操作）。
  bool revokeSession(const std::string &session_id);

  // 吊销某用户全部会话，返回实际吊销数。
  int revokeAllSessions(const std::string &user_id);

private:

  // Redis key 拼装（全部收敛在此，避免魔法字符串散落各处）。
  std::string sessionKey(const std::string &sid) const; // "auth:session:" + sid
  std::string accessKey(const std::string &at) const;   // "auth:access:"  + at
  std::string refreshKey(const std::string &rt) const;  // "auth:refresh:" + rt
  std::string ticketKey(const std::string &tk) const;   // "auth:ticket:"  + tk
  std::string userSessionsKey(const std::string &uid) const;
  std::string userDeviceKey(const std::string &uid, int device_type) const;

  // 把 hgetall 的 9 字段 map 转成 SessionInfo；字段缺失时该字段留默认值。
  static auth::SessionInfo
  mapToSessionInfo(const std::unordered_map<std::string, std::string> &fields);

  RedisClient redis_;
};