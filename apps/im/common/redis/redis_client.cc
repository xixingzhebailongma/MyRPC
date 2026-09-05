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

// ---------- Pub/Sub 发布 ----------
bool RedisClient::publish(const std::string &channel, const std::string &msg) {
  auto conn = pool_.acquire();
  if (!conn) {
    LOG_ERROR("RedisClient::publish: no available connection");
    return false;
  }
  RedisReply reply(static_cast<redisReply *>(
      redisCommand(conn.get(), "PUBLISH %b %b", channel.data(), channel.size(),
                   msg.data(), msg.size())));
  if (!reply || conn.get()->err != 0) {
    conn.markBroken();
    LOG_ERROR("RedisClient::publish: connection lost");
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_INTEGER);
  if (!ok) {
    LOG_ERROR("RedisClient::publish: unexpected reply type %d", reply->type);
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