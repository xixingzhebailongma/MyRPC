#pragma once
#include "redis_pool.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
// 前向声明 hiredis C 类型，避免在头文件中暴露 hiredis.h
struct redisContext;
// 单个 ZSET 成员及其 score（用于读取队头死线）
struct ZScoreItem {
  std::string member;
  double score;
};
// 单个 Stream 条目：id + 字段列表（fields 是扁平的 field/value 对）
struct StreamEntry {
  std::string id;
  std::vector<std::pair<std::string, std::string>> fields;
};

// XAUTOCLAIM 的返回：下一游标 + 认领到的条目 + 已删除（不再在 PEL）的 id
struct XAutoClaimResult {
  std::string next_cursor;
  std::vector<StreamEntry> claimed;
  std::vector<std::string> deleted_ids;
};

// 原子 SET NX EX 的结果：区分「首次设置成功」「key 已存在」「出错(连接失败等)」
enum class SetResult {
  kSet,    // 首次设置成功
  kExists, // key 已存在（NX 阻止）
  kError,  // 连接失败 / 回复异常
};

class RedisClient {
public:
  RedisClient();
  ~RedisClient();

  RedisClient(const RedisClient &) = delete;
  RedisClient &operator=(const RedisClient &) = delete;

  bool connect(const std::string &ip, int port);

  // String操作
  bool set(const std::string &key, const std::string &value);
  std::string get(const std::string &key);
  // 原子"取值+删除"（GETDEL）：一次性票据消费用，消除 GET-then-DEL 竞态。
  // key 不存在返回空字符串。需 Redis 6.2+。
  std::string getdel(const std::string &key);
  bool del(const std::string &key);
  bool exists(const std::string &key);

  // Hash操作
  bool hset(const std::string &key, const std::string &field,
            const std::string &value);
  std::string hget(const std::string &key, const std::string &field);
  std::unordered_map<std::string, std::string> hgetall(const std::string &key);
  bool hdel(const std::string &key, const std::string &field);

  //去重与过期
  bool setnx(const std::string &key, const std::string &value);
  bool expire(const std::string &key, int seconds);
  // 原子"仅当不存在时设值+设过期"：单条 SET ... NX EX，把「连接失败」与
  // 「key 已存在」显式区分开（原先 setnx+expire 两步会丢失这个区分）。
  SetResult setNxEx(const std::string &key, const std::string &value,
                    int seconds);
  // 原子的"设值+设过期"：token key 绝不能出现无 TTL 的中间状态
  bool setex(const std::string &key, const std::string &value, int seconds);
  // List操作(离线消息队列)
  bool lpush(const std::string &key, const std::string &value);
  std::vector<std::string> lrange(const std::string &key, int start, int stop);

  //计数器
  int64_t incr(const std::string &key);

  // zset操作
  bool zadd(const std::string &key, double score, const std::string &member);
  std::vector<std::string> zrangebyscore(const std::string &key, double min,
                                         double max, int limit = 0);
  bool zrem(const std::string &key, const std::string &member);
  // 读 ZSET 头元素（最小 score）及其 score；空队列返回 std::nullopt
  std::optional<ZScoreItem> zrangeFirstWithScore(const std::string &key);
  // 按 score 区间批量删除成员，返回删除个数；出错返回 -1
  int64_t zremrangebyscore(const std::string &key, double min, double max);
  // Pub/Sub发布（一次性，复用连接池）
  bool publish(const std::string &channel, const std::string &msg);

  // —— 批量 Pipeline：多条写命令攒在同一条连接上一次性提交 ——
  // 用 redisAppendCommand 排队、flush() 时 redisGetReply 逐个收回复，
  // 把 N 次 RTT 合并成 1 次。只覆盖写命令（业务里需要读结果的分支仍走单条）。
  class Pipeline {
  public:
    explicit Pipeline(RedisClient &client);
    ~Pipeline(); // 未 flush 的命令直接丢弃并标记连接 broken
    Pipeline(const Pipeline &) = delete;
    Pipeline &operator=(const Pipeline &) = delete;
    Pipeline(Pipeline &&o) noexcept;

    // 追加命令（只排队不发送不读回复），链式调用；出错后自动短路为 no-op。
    Pipeline &set(const std::string &key, const std::string &value);
    Pipeline &
    hmset(const std::string &key,
          const std::vector<std::pair<std::string, std::string>> &fields);
    Pipeline &expire(const std::string &key, int seconds);
    Pipeline &setex(const std::string &key, const std::string &value,
                    int seconds);
    Pipeline &setnx(const std::string &key, const std::string &value);
    Pipeline &del(const std::string &key);
    Pipeline &lpush(const std::string &key, const std::string &value);
    Pipeline &zadd(const std::string &key, double score,
                   const std::string &member);
    Pipeline &zrem(const std::string &key, const std::string &member);
    Pipeline &zremrangebyscore(const std::string &key, double min, double max);

    // 提交：一次发出去并逐条读回，全部成功返回 true；任一失败返回 false
    // 并断连。
    bool flush();
    // 是否拿到了可用连接（acquire 失败时 false，后续 appends 全是 no-op）
    explicit operator bool() const { return static_cast<bool>(conn_); }

  private:
    RedisClient &client_;
    RedisPool::Guard conn_;
    int pending_ = 0;
    bool failed_ = false;
  };

  // 新建一个 pipeline（内部 acquire 一条连接）
  Pipeline pipeline();
  // 执行 Lua 脚本（EVAL），返回是否成功。keys 是 KEYS 数组，args 是 ARGV
  // 数组。
  bool eval(const std::string &script, const std::vector<std::string> &keys,
            const std::vector<std::string> &args);
  // ---------- Stream 操作（消息队列） ----------
  // XADD stream * field value → 返回新条目 id；失败返回空串
  std::string xadd(const std::string &stream, const std::string &field,
                   const std::string &value);
  // XGROUP CREATE stream group 0 MKSTREAM；组已存在(BUSYGROUP)视为成功
  bool xgroupCreate(const std::string &stream, const std::string &group);
  // XREADGROUP GROUP group consumer COUNT count STREAMS stream >
  // BLOCK block_ms>0 时阻塞等待；无消息/超时返回空
  std::vector<StreamEntry> xreadgroup(const std::string &group,
                                      const std::string &consumer,
                                      const std::string &stream, int count,
                                      int block_ms);
  // XACK stream group id → 确认一条；true 表示命令成功（0 也算成功）
  bool xack(const std::string &stream, const std::string &group,
            const std::string &id);
  // XAUTOCLAIM stream group consumer min_idle_ms COUNT count [start 游标]
  XAutoClaimResult xautoclaim(const std::string &stream,
                              const std::string &group,
                              const std::string &consumer, int64_t min_idle_ms,
                              int count, const std::string &start = "0-0");

private:
  static constexpr size_t kDefaultPoolSize = 4;
  RedisPool pool_;
};