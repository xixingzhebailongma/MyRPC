#include "redis_client.h"
#include "Logger.h"
#include "redis_reply.h"
#include <hiredis/hiredis.h>
#include <read.h>
#include <string>
RedisClient::RedisClient() = default;
RedisClient::~RedisClient() = default;

bool RedisClient::connect(const std::string &ip, int port) {
  return pool_.init(ip, port, kDefaultPoolSize);
}

// ---------- String 操作 ----------
bool RedisClient::set(const std::string &key, const std::string &value) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::set: no available connection");
    return false;
  }

  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "SET %b %b", key.data(), key.size(),
                   value.data(), value.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::set: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_STATUS &&
             std::string(reply->str, reply->len) == "OK");
  if (!ok) {
    LOG_ERROR("RedisClient::set: unexpected reply type %d", reply->type);
  }
  return ok; // RedisReply 析构 free；Guard 析构归还/丢弃
}

std::string RedisClient::get(const std::string &key) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::get: no available connection");
    return "";
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "GET %b", key.data(), key.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::get: connection lost");
    return "";
  }
  std::string result;
  if (reply->type == REDIS_REPLY_STRING) {
    result.assign(reply->str, reply->len);
  } else if (reply->type == REDIS_REPLY_NIL) {
    // key 不存在，返回空字符串
  } else {
    LOG_ERROR("RedisClient::get: unexpected reply type %d", reply->type);
  }
  return result;
}
std::string RedisClient::getdel(const std::string &key) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::getdel: no available connection");
    return "";
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "GETDEL %b", key.data(), key.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::getdel: connection lost");
    return "";
  }
  std::string result;
  if (reply->type == REDIS_REPLY_STRING) {
    result.assign(reply->str, reply->len);
  } else if (reply->type == REDIS_REPLY_NIL) {
    // key 不存在（已被消费或已过期），返回空字符串
  } else {
    LOG_ERROR("RedisClient::getdel: unexpected reply type %d", reply->type);
  }
  return result;
}
bool RedisClient::del(const std::string &key) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::del: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "DEL %b", key.data(), key.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::del: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_INTEGER);
  if (!ok) {
    LOG_ERROR("RedisClient::del: unexpected reply type %d", reply->type);
  }
  return ok;
}

bool RedisClient::exists(const std::string &key) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::exists: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "EXISTS %b", key.data(), key.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::exists: connection lost");
    return false;
  }
  bool exists_flag = false;
  if (reply->type == REDIS_REPLY_INTEGER) {
    exists_flag = (reply->integer == 1);
  } else {
    LOG_ERROR("RedisClient::exists: unexpected reply type %d", reply->type);
  }
  return exists_flag;
}

//----Hash操作----
bool RedisClient::hset(const std::string &key, const std::string &field,
                       const std::string &value) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::hset: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "HSET %b %b %b", key.data(), key.size(),
                   field.data(), field.size(), value.data(), value.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::hset: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_INTEGER);
  if (!ok) {
    LOG_ERROR("RedisClient::hset: unexpected reply type %d", reply->type);
  }
  return ok;
}

std::string RedisClient::hget(const std::string &key,
                              const std::string &field) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::hget: no available connection");
    return "";
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "HGET %b %b", key.data(), key.size(),
                   field.data(), field.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::hget: connection lost");
    return "";
  }
  std::string result;
  if (reply->type == REDIS_REPLY_STRING) {
    result.assign(reply->str, reply->len);
  } else if (reply->type == REDIS_REPLY_NIL) {
    // field 不存在，返回空字符串
  } else {
    LOG_ERROR("RedisClient::hget: unexpected reply type %d", reply->type);
  }
  return result;
}

bool RedisClient::hdel(const std::string &key, const std::string &field) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::hdel: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "HDEL %b %b", key.data(), key.size(),
                   field.data(), field.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::hdel: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_INTEGER);
  if (!ok) {
    LOG_ERROR("RedisClient::hdel: unexpected reply type %d", reply->type);
  }
  return ok;
}

//去重与过期
bool RedisClient::setnx(const std::string &key, const std::string &value) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::setnx: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "SETNX %b %b", key.data(), key.size(),
                   value.data(), value.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::setnx: connection lost");
    return false;
  }
  bool success = false;
  if (reply->type == REDIS_REPLY_INTEGER) {
    success = (reply->integer == 1); // 1 = key 不存在，设置成功；0 = key 已存在
  } else {
    LOG_ERROR("RedisClient::setnx: unexpected reply type %d", reply->type);
  }
  return success;
}
SetResult RedisClient::setNxEx(const std::string &key, const std::string &value,
                               int seconds) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::setNxEx: no available connection");
    return SetResult::kError;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "SET %b %b NX EX %d", key.data(), key.size(),
                   value.data(), value.size(), seconds)));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::setNxEx: connection lost");
    return SetResult::kError;
  }
  if (reply->type == REDIS_REPLY_STATUS) {
    return SetResult::kSet; // 回复 "OK"：首次设置成功
  }
  if (reply->type == REDIS_REPLY_NIL) {
    return SetResult::kExists; // NX 阻止：key 已存在
  }
  LOG_ERROR("RedisClient::setNxEx: unexpected reply type %d", reply->type);
  return SetResult::kError;
}
bool RedisClient::expire(const std::string &key, int seconds) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::expire: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(redisCommand(
      conn.get(), "EXPIRE %b %d", key.data(), key.size(), seconds)));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::expire: connection lost");
    return false;
  }
  bool ok = false;
  if (reply->type == REDIS_REPLY_INTEGER) {
    ok = (reply->integer == 1); // 1=成功设置过期时间;0 = key 不存在
  } else {
    LOG_ERROR("RedisClient::expire: unexpected reply type %d", reply->type);
  }
  return ok;
}

// List操作(离线消息队列)
bool RedisClient::lpush(const std::string &key, const std::string &value) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::lpush: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "LPUSH %b %b", key.data(), key.size(),
                   value.data(), value.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::lpush: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_INTEGER);
  if (!ok) {
    LOG_ERROR("RedisClient::lpush: unexpected reply type %d", reply->type);
  }
  return ok;
}

std::vector<std::string> RedisClient::lrange(const std::string &key, int start,
                                             int stop) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::lrange: no available connection");
    return {};
  }
  RedisReply reply(static_cast<redisReply *>(redisCommand(
      conn.get(), "LRANGE %b %d %d", key.data(), key.size(), start, stop)));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::lrange: connection lost");
    return {};
  }
  std::vector<std::string> result;
  if (reply->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < reply->elements; ++i) {
      if (reply->element[i]->type == REDIS_REPLY_STRING) {
        result.emplace_back(reply->element[i]->str, reply->element[i]->len);
      }
    }
  } else {
    LOG_ERROR("RedisClient::lrange: unexpected reply type %d", reply->type);
  }
  return result;
}
//计数器(消息ID生成)
int64_t RedisClient::incr(const std::string &key) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::incr: no available connection");
    return -1;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "INCR %b", key.data(), key.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::incr: connection lost");
    return -1;
  }
  int64_t result = -1;
  if (reply->type == REDIS_REPLY_INTEGER) {
    result = reply->integer;
  } else {
    LOG_ERROR("RedisClient::incr: unexpected reply type %d", reply->type);
  }
  return result;
}

// ---------- ZSet 操作 ----------
bool RedisClient::zadd(const std::string &key, double score,
                       const std::string &member) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::zadd: no available connection");
    return false;
  }
  std::string score_str = std::to_string(score); // 避免 %f 精度/格式问题
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "ZADD %b %s %b", key.data(), key.size(),
                   score_str.c_str(), member.data(), member.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::zadd: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_INTEGER);
  if (!ok) {
    LOG_ERROR("RedisClient::zadd: unexpected reply type %d", reply->type);
  }
  return ok;
}

std::vector<std::string> RedisClient::zrangebyscore(const std::string &key,
                                                    double min, double max,
                                                    int limit) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::zrangebyscore: no available connection");
    return {};
  }
  std::string min_str = std::to_string(min);
  std::string max_str = std::to_string(max);

  redisReply *raw = nullptr;
  if (limit > 0) {
    raw = static_cast<redisReply *>(redisCommand(
        conn.get(), "ZRANGEBYSCORE %b %s %s LIMIT 0 %d", key.data(), key.size(),
        min_str.c_str(), max_str.c_str(), limit));
  } else {
    raw = static_cast<redisReply *>(
        redisCommand(conn.get(), "ZRANGEBYSCORE %b %s %s", key.data(),
                     key.size(), min_str.c_str(), max_str.c_str()));
  }
  RedisReply reply(raw);
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::zrangebyscore: connection lost");
    return {};
  }
  std::vector<std::string> result;
  if (reply->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < reply->elements; ++i) {
      if (reply->element[i]->type == REDIS_REPLY_STRING) {
        result.emplace_back(reply->element[i]->str, reply->element[i]->len);
      }
    }
  } else {
    LOG_ERROR("RedisClient::zrangebyscore: unexpected reply type %d",
              reply->type);
  }
  return result;
}
std::optional<ZScoreItem>
RedisClient::zrangeFirstWithScore(const std::string &key) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::zrangeFirstWithScore: no available connection");
    return std::nullopt;
  }
  // ZRANGE key 0 0 WITHSCORES → 扁平数组 [member, score]
  RedisReply reply(static_cast<redisReply *>(redisCommand(
      conn.get(), "ZRANGE %b 0 0 WITHSCORES", key.data(), key.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::zrangeFirstWithScore: connection lost");
    return std::nullopt;
  }
  // 空队列返回空数组（elements == 0）；有元素时 [member, score] 都是 STRING
  if (reply->type != REDIS_REPLY_ARRAY) {
    LOG_ERROR("RedisClient::zrangeFirstWithScore: unexpected reply type %d",
              reply->type);
    return std::nullopt;
  }
  if (reply->elements < 2) {
    return std::nullopt; // 空队列
  }
  const redisReply *m = reply->element[0];
  const redisReply *s = reply->element[1];
  if (m->type != REDIS_REPLY_STRING || s->type != REDIS_REPLY_STRING) {
    LOG_ERROR("RedisClient::zrangeFirstWithScore: bad element type");
    return std::nullopt;
  }
  ZScoreItem item;
  item.member.assign(m->str, m->len);
  try {
    item.score = std::stod(std::string(s->str, s->len));
  } catch (...) {
    // score 一定是我们用 std::to_string 写入的合法 double；防御坏数据
    return std::nullopt;
  }
  return item;
}
bool RedisClient::zrem(const std::string &key, const std::string &member) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::zrem: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "ZREM %b %b", key.data(), key.size(),
                   member.data(), member.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::zrem: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_INTEGER);
  if (!ok) {
    LOG_ERROR("RedisClient::zrem: unexpected reply type %d", reply->type);
  }
  return ok;
}

// ---------- Token 体系新增：setex / hgetall / zremrangebyscore ----------
bool RedisClient::setex(const std::string &key, const std::string &value,
                        int seconds) {
  if (seconds <= 0) {
    // SETEX 的 TTL 必须为正，否则 Redis 直接回错误。提前拦掉，日志更清楚。
    LOG_ERROR("RedisClient::setex: invalid ttl %d", seconds);
    return false;
  }
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::setex: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "SETEX %b %d %b", key.data(), key.size(),
                   seconds, value.data(), value.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::setex: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_STATUS &&
             std::string(reply->str, reply->len) == "OK");
  if (!ok) {
    LOG_ERROR("RedisClient::setex: unexpected reply type %d", reply->type);
  }
  return ok;
}

std::unordered_map<std::string, std::string>
RedisClient::hgetall(const std::string &key) {
  std::unordered_map<std::string, std::string> result;
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::hgetall: no available connection");
    return result;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "HGETALL %b", key.data(), key.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::hgetall: connection lost");
    return result;
  }
  if (reply->type == REDIS_REPLY_ARRAY) {
    // HGETALL 回复是扁平数组：[field1, value1, field2, value2, ...]
    // key 不存在时是空数组（elements == 0），自然返回空 map
    for (size_t i = 0; i + 1 < reply->elements; i += 2) {
      const redisReply *f = reply->element[i];
      const redisReply *v = reply->element[i + 1];
      if (f->type == REDIS_REPLY_STRING && v->type == REDIS_REPLY_STRING) {
        result.emplace(std::string(f->str, f->len),
                       std::string(v->str, v->len));
      }
    }
  } else {
    LOG_ERROR("RedisClient::hgetall: unexpected reply type %d", reply->type);
  }
  return result;
}

int64_t RedisClient::zremrangebyscore(const std::string &key, double min,
                                      double max) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::zremrangebyscore: no available connection");
    return -1;
  }
  std::string min_str = std::to_string(min); // 与 zadd/zrangebyscore 一致，
  std::string max_str = std::to_string(max); // 避免 %f 的精度/格式问题
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "ZREMRANGEBYSCORE %b %s %s", key.data(),
                   key.size(), min_str.c_str(), max_str.c_str())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::zremrangebyscore: connection lost");
    return -1;
  }
  int64_t removed = -1;
  if (reply->type == REDIS_REPLY_INTEGER) {
    removed = reply->integer;
  } else {
    LOG_ERROR("RedisClient::zremrangebyscore: unexpected reply type %d",
              reply->type);
  }
  return removed;
}

// ---------- Pipeline 实现 ----------
RedisClient::Pipeline RedisClient::pipeline() { return Pipeline(*this); }

RedisClient::Pipeline::Pipeline(RedisClient &client)
    : client_(client), conn_(client.pool_.acquire()) {}

RedisClient::Pipeline::Pipeline(Pipeline &&o) noexcept
    : client_(o.client_), conn_(std::move(o.conn_)), pending_(o.pending_),
      failed_(o.failed_) {
  o.pending_ = 0;
  o.failed_ = true; // 源失去连接，禁止其再 flush
}

RedisClient::Pipeline::~Pipeline() {
  // 关键：还有未 flush 的命令时，连接输出缓冲区里有残留命令，
  // 直接归还会被下一次 acquire 的 PING 一起 flush 导致串包，必须标记 broken
  // 丢弃。
  if (pending_ > 0) {
    conn_.markBroken();
  }
}

bool RedisClient::Pipeline::flush() {
  if (failed_ || !conn_) {
    return false;
  }
  for (int i = 0; i < pending_; ++i) {
    redisReply *reply = nullptr;
    if (redisGetReply(conn_.get(), reinterpret_cast<void **>(&reply)) !=
            REDIS_OK ||
        reply == nullptr || conn_.get()->err != 0) {
      if (reply) {
        freeReplyObject(reply);
      }
      conn_.markBroken();
      failed_ = true;
      LOG_ERROR("RedisClient::Pipeline::flush: connection lost at reply %d/%d",
                i, pending_);
      return false;
    }
    if (reply->type == REDIS_REPLY_ERROR) {
      LOG_ERROR("RedisClient::Pipeline::flush: server error: %s",
                std::string(reply->str, reply->len).c_str());
      freeReplyObject(reply);
      conn_.markBroken();
      failed_ = true;
      return false;
    }
    freeReplyObject(reply);
  }
  pending_ = 0;
  return true;
}

RedisClient::Pipeline &RedisClient::Pipeline::set(const std::string &key,
                                                  const std::string &value) {
  if (failed_ || !conn_) {
    return *this;
  }
  if (redisAppendCommand(conn_.get(), "SET %b %b", key.data(), key.size(),
                         value.data(), value.size()) != REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &RedisClient::Pipeline::hmset(
    const std::string &key,
    const std::vector<std::pair<std::string, std::string>> &fields) {
  if (failed_ || !conn_) {
    return *this;
  }
  // HSET key f1 v1 f2 v2 ... （Redis 2.4+ 支持多 field-value 对，原子写入整张
  // hash）
  int argc = 2 + static_cast<int>(fields.size()) * 2;
  std::vector<const char *> argv;
  std::vector<size_t> argvlen;
  argv.reserve(argc);
  argvlen.reserve(argc);
  argv.push_back("HSET");
  argvlen.push_back(4);
  argv.push_back(key.data());
  argvlen.push_back(key.size());
  for (const auto &kv : fields) {
    argv.push_back(kv.first.data());
    argvlen.push_back(kv.first.size());
    argv.push_back(kv.second.data());
    argvlen.push_back(kv.second.size());
  }
  if (redisAppendCommandArgv(conn_.get(), argc, argv.data(), argvlen.data()) !=
      REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &RedisClient::Pipeline::expire(const std::string &key,
                                                     int seconds) {
  if (failed_ || !conn_) {
    return *this;
  }
  if (redisAppendCommand(conn_.get(), "EXPIRE %b %d", key.data(), key.size(),
                         seconds) != REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &RedisClient::Pipeline::setex(const std::string &key,
                                                    const std::string &value,
                                                    int seconds) {
  if (failed_ || !conn_) {
    return *this;
  }
  if (redisAppendCommand(conn_.get(), "SETEX %b %d %b", key.data(), key.size(),
                         seconds, value.data(), value.size()) != REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &RedisClient::Pipeline::setnx(const std::string &key,
                                                    const std::string &value) {
  if (failed_ || !conn_) {
    return *this;
  }
  if (redisAppendCommand(conn_.get(), "SETNX %b %b", key.data(), key.size(),
                         value.data(), value.size()) != REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &RedisClient::Pipeline::del(const std::string &key) {
  if (failed_ || !conn_) {
    return *this;
  }
  if (redisAppendCommand(conn_.get(), "DEL %b", key.data(), key.size()) !=
      REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &RedisClient::Pipeline::lpush(const std::string &key,
                                                    const std::string &value) {
  if (failed_ || !conn_) {
    return *this;
  }
  if (redisAppendCommand(conn_.get(), "LPUSH %b %b", key.data(), key.size(),
                         value.data(), value.size()) != REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &RedisClient::Pipeline::zadd(const std::string &key,
                                                   double score,
                                                   const std::string &member) {
  if (failed_ || !conn_) {
    return *this;
  }
  std::string score_str =
      std::to_string(score); // 与单条 zadd 一致，避免 %f 精度问题
  if (redisAppendCommand(conn_.get(), "ZADD %b %s %b", key.data(), key.size(),
                         score_str.c_str(), member.data(),
                         member.size()) != REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &RedisClient::Pipeline::zrem(const std::string &key,
                                                   const std::string &member) {
  if (failed_ || !conn_) {
    return *this;
  }
  if (redisAppendCommand(conn_.get(), "ZREM %b %b", key.data(), key.size(),
                         member.data(), member.size()) != REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

RedisClient::Pipeline &
RedisClient::Pipeline::zremrangebyscore(const std::string &key, double min,
                                        double max) {
  if (failed_ || !conn_) {
    return *this;
  }
  std::string min_str = std::to_string(min);
  std::string max_str = std::to_string(max);
  if (redisAppendCommand(conn_.get(), "ZREMRANGEBYSCORE %b %s %s", key.data(),
                         key.size(), min_str.c_str(),
                         max_str.c_str()) != REDIS_OK) {
    conn_.markBroken();
    failed_ = true;
  } else {
    ++pending_;
  }
  return *this;
}

bool RedisClient::eval(const std::string &script,
                       const std::vector<std::string> &keys,
                       const std::vector<std::string> &args) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::eval: no available connection");
    return false;
  }
  // EVAL script numkeys key1 ... keyN arg1 ... argM
  int argc = 3 + static_cast<int>(keys.size()) + static_cast<int>(args.size());
  std::vector<const char *> argv;
  std::vector<size_t> argvlen;
  argv.reserve(argc);
  argvlen.reserve(argc);
  argv.push_back("EVAL");
  argvlen.push_back(4);
  argv.push_back(script.data());
  argvlen.push_back(script.size());
  std::string numkeys_str = std::to_string(keys.size());
  argv.push_back(
      numkeys_str.c_str()); // numkeys_str 存活到 redisCommandArgv 返回，安全
  argvlen.push_back(numkeys_str.size());
  for (const auto &k : keys) {
    argv.push_back(k.data());
    argvlen.push_back(k.size());
  }
  for (const auto &a : args) {
    argv.push_back(a.data());
    argvlen.push_back(a.size());
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommandArgv(conn.get(), argc, argv.data(), argvlen.data())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::eval: connection lost");
    return false;
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    LOG_ERROR("RedisClient::eval: server error: %s",
              std::string(reply->str, reply->len).c_str());
    return false;
  }
  return true;
}
std::vector<std::string>
RedisClient::evalRead(const std::string &script,
                      const std::vector<std::string> &keys,
                      const std::vector<std::string> &args) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::evalRead: no available connection");
    return {};
  }
  // EVAL script numkeys key1 ... keyN arg1 ... argM
  int argc = 3 + static_cast<int>(keys.size()) + static_cast<int>(args.size());
  std::vector<const char *> argv;
  std::vector<size_t> argvlen;
  argv.reserve(argc);
  argvlen.reserve(argc);
  argv.push_back("EVAL");
  argvlen.push_back(4);
  argv.push_back(script.data());
  argvlen.push_back(script.size());
  std::string numkeys_str = std::to_string(keys.size());
  argv.push_back(numkeys_str.c_str());
  argvlen.push_back(numkeys_str.size());
  for (const auto &k : keys) {
    argv.push_back(k.data());
    argvlen.push_back(k.size());
  }
  for (const auto &a : args) {
    argv.push_back(a.data());
    argvlen.push_back(a.size());
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommandArgv(conn.get(), argc, argv.data(), argvlen.data())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::evalRead: connection lost");
    return {};
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    LOG_ERROR("RedisClient::evalRead: server error: %s",
              std::string(reply->str, reply->len).c_str());
    return {};
  }
  std::vector<std::string> result;
  auto push = [&result](const redisReply *e) {
    if (!e)
      return;
    if (e->type == REDIS_REPLY_STRING)
      result.emplace_back(e->str, e->len);
    else if (e->type == REDIS_REPLY_INTEGER)
      result.emplace_back(std::to_string(e->integer));
  };
  if (reply->type == REDIS_REPLY_ARRAY) {
    for (size_t i = 0; i < reply->elements; ++i)
      push(reply->element[i]);
  } else {
    push(reply.get());
  }
  return result;
}
// ---------- Stream 操作（消息队列） ----------
// 解析单个 Stream 条目回复 [id, [field, value, ...]] → StreamEntry
static bool parseStreamEntry(const redisReply *entry_reply, StreamEntry *out) {
  if (!entry_reply || entry_reply->type != REDIS_REPLY_ARRAY ||
      entry_reply->elements < 2) {
    return false;
  }
  const redisReply *id = entry_reply->element[0];
  const redisReply *fields = entry_reply->element[1];
  if (!id || id->type != REDIS_REPLY_STRING || !fields ||
      fields->type != REDIS_REPLY_ARRAY) {
    return false;
  }
  out->id.assign(id->str, id->len);
  for (size_t i = 0; i + 1 < fields->elements; i += 2) {
    const redisReply *f = fields->element[i];
    const redisReply *v = fields->element[i + 1];
    if (f->type == REDIS_REPLY_STRING && v->type == REDIS_REPLY_STRING) {
      out->fields.emplace_back(std::string(f->str, f->len),
                               std::string(v->str, v->len));
    }
  }
  return true;
}

std::string RedisClient::xadd(const std::string &stream,
                              const std::string &field,
                              const std::string &value) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::xadd: no available connection");
    return "";
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "XADD %b * %b %b", stream.data(), stream.size(),
                   field.data(), field.size(), value.data(), value.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::xadd: connection lost");
    return "";
  }
  std::string id;
  if (reply->type == REDIS_REPLY_STRING) {
    id.assign(reply->str, reply->len);
  } else {
    LOG_ERROR("RedisClient::xadd: unexpected reply type %d", reply->type);
  }
  return id;
}

std::string RedisClient::xaddTrimmed(const std::string &stream,
                                     const std::string &field,
                                     const std::string &value, int64_t maxlen) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::xaddTrimmed: no available connection");
    return "";
  }
  std::string maxlen_str = std::to_string(maxlen); // 避免 %lld 与 int64_t 类型不匹配
  RedisReply reply(static_cast<redisReply *>(redisCommand(
      conn.get(), "XADD %b MAXLEN ~ %s * %b %b", stream.data(), stream.size(),
      maxlen_str.c_str(), field.data(), field.size(), value.data(),
      value.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::xaddTrimmed: connection lost");
    return "";
  }
  std::string id;
  if (reply->type == REDIS_REPLY_STRING) {
    id.assign(reply->str, reply->len);
  } else {
    LOG_ERROR("RedisClient::xaddTrimmed: unexpected reply type %d", reply->type);
  }
  return id;
}

bool RedisClient::xgroupCreate(const std::string &stream,
                               const std::string &group) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::xgroupCreate: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "XGROUP CREATE %b %b 0 MKSTREAM", stream.data(),
                   stream.size(), group.data(), group.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::xgroupCreate: connection lost");
    return false;
  }
  if (reply->type == REDIS_REPLY_STATUS) {
    return true; // "OK"
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    std::string msg(reply->str, reply->len);
    if (msg.find("BUSYGROUP") != std::string::npos) {
      return true; // 组已存在，幂等视为成功
    }
    LOG_ERROR("RedisClient::xgroupCreate: server error: %s", msg.c_str());
    return false;
  }
  LOG_ERROR("RedisClient::xgroupCreate: unexpected reply type %d", reply->type);
  return false;
}

bool RedisClient::xgroupCreateFromNow(const std::string &stream,
                                      const std::string &group) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::xgroupCreateFromNow: no available connection");
    return false;
  }
  // 从 $ 起建组：新节点只读未来事件，不重放历史（与投递用的 xgroupCreate 从 0 起不同）
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "XGROUP CREATE %b %b $ MKSTREAM", stream.data(),
                   stream.size(), group.data(), group.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::xgroupCreateFromNow: connection lost");
    return false;
  }
  if (reply->type == REDIS_REPLY_STATUS) {
    return true; // "OK"
  }
  if (reply->type == REDIS_REPLY_ERROR) {
    std::string msg(reply->str, reply->len);
    if (msg.find("BUSYGROUP") != std::string::npos) {
      return true; // 组已存在，幂等视为成功
    }
    LOG_ERROR("RedisClient::xgroupCreateFromNow: server error: %s", msg.c_str());
    return false;
  }
  LOG_ERROR("RedisClient::xgroupCreateFromNow: unexpected reply type %d",
            reply->type);
  return false;
}

std::vector<StreamEntry> RedisClient::xreadgroup(const std::string &group,
                                                 const std::string &consumer,
                                                 const std::string &stream,
                                                 int count, int block_ms) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::xreadgroup: no available connection");
    return {};
  }
  redisReply *raw = nullptr;
  if (block_ms > 0) {
    raw = static_cast<redisReply *>(redisCommand(
        conn.get(), "XREADGROUP GROUP %b %b COUNT %d BLOCK %d STREAMS %b >",
        group.data(), group.size(), consumer.data(), consumer.size(), count,
        block_ms, stream.data(), stream.size()));
  } else {
    raw = static_cast<redisReply *>(
        redisCommand(conn.get(), "XREADGROUP GROUP %b %b COUNT %d STREAMS %b >",
                     group.data(), group.size(), consumer.data(),
                     consumer.size(), count, stream.data(), stream.size()));
  }
  RedisReply reply(raw);
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::xreadgroup: connection lost");
    return {};
  }
  std::vector<StreamEntry> result;
  // BLOCK 超时 / 无新消息都返回 NIL
  if (reply->type == REDIS_REPLY_NIL) {
    return result;
  }
  if (reply->type != REDIS_REPLY_ARRAY) {
    LOG_ERROR("RedisClient::xreadgroup: unexpected reply type %d", reply->type);
    return result;
  }
  // 顶层：[ [stream_name, [entry, entry, ...]], ... ]；我们只查单个 stream
  for (size_t s = 0; s < reply->elements; ++s) {
    const redisReply *stream_reply = reply->element[s];
    if (!stream_reply || stream_reply->type != REDIS_REPLY_ARRAY ||
        stream_reply->elements < 2) {
      continue;
    }
    const redisReply *entries = stream_reply->element[1];
    if (!entries || entries->type != REDIS_REPLY_ARRAY) {
      continue;
    }
    for (size_t i = 0; i < entries->elements; ++i) {
      StreamEntry e;
      if (parseStreamEntry(entries->element[i], &e)) {
        result.push_back(std::move(e));
      }
    }
  }
  return result;
}

bool RedisClient::xack(const std::string &stream, const std::string &group,
                       const std::string &id) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::xack: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "XACK %b %b %b", stream.data(), stream.size(),
                   group.data(), group.size(), id.data(), id.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::xack: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_INTEGER);
  if (!ok) {
    LOG_ERROR("RedisClient::xack: unexpected reply type %d", reply->type);
  }
  return ok;
}

XAutoClaimResult RedisClient::xautoclaim(const std::string &stream,
                                         const std::string &group,
                                         const std::string &consumer,
                                         int64_t min_idle_ms, int count,
                                         const std::string &start) {
  XAutoClaimResult result;
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::xautoclaim: no available connection");
    return result;
  }
  std::string min_idle_str = std::to_string(min_idle_ms);
  std::string count_str = std::to_string(count);
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "XAUTOCLAIM %b %b %b %s %s COUNT %s",
                   stream.data(), stream.size(), group.data(), group.size(),
                   consumer.data(), consumer.size(), min_idle_str.c_str(),
                   start.c_str(), count_str.c_str())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::xautoclaim: connection lost");
    return result;
  }
  if (reply->type == REDIS_REPLY_ARRAY && reply->elements >= 3) {
    // [0] = 下一游标, [1] = 认领的条目, [2] = 已删除的 id（不再在 PEL）
    const redisReply *cursor = reply->element[0];
    if (cursor && cursor->type == REDIS_REPLY_STRING) {
      result.next_cursor.assign(cursor->str, cursor->len);
    }
    const redisReply *claimed = reply->element[1];
    if (claimed && claimed->type == REDIS_REPLY_ARRAY) {
      for (size_t i = 0; i < claimed->elements; ++i) {
        StreamEntry e;
        if (parseStreamEntry(claimed->element[i], &e)) {
          result.claimed.push_back(std::move(e));
        }
      }
    }
    const redisReply *deleted = reply->element[2];
    if (deleted && deleted->type == REDIS_REPLY_ARRAY) {
      for (size_t i = 0; i < deleted->elements; ++i) {
        if (deleted->element[i]->type == REDIS_REPLY_STRING) {
          result.deleted_ids.emplace_back(deleted->element[i]->str,
                                          deleted->element[i]->len);
        }
      }
    }
  } else {
    LOG_ERROR("RedisClient::xautoclaim: unexpected reply type %d", reply->type);
  }
  return result;
}