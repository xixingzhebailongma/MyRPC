#include "redis_pool.h"
#include "Logger.h"
#include <hiredis/hiredis.h>

void RedisPool::ContextDeleter::operator()(redisContext *ctx) const {
  if (ctx) {
    redisFree(ctx);
  }
}

RedisPool::RedisPool() = default;

RedisPool::~RedisPool() {
  // idle_ 里的连接由 ConnPtr 的 ContextDeleter 自动 redisFree
}

bool RedisPool::init(const std::string &ip, int port, size_t pool_size) {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    ip_ = ip;
    port_ = port;
    pool_size_ = pool_size;
    initialized_ = true;
  }
  // 急切拨 1 条验证连通；其余按需惰性建
  ConnPtr c = dial();
  if (!c) {
    LOG_ERROR("RedisPool::init: initial connect to %s:%d failed", ip.c_str(),
              port);
    return false; // 保留 initialized_=true，后续 acquire() 会再次尝试拨号自愈
  }
  std::lock_guard<std::mutex> lk(mutex_);
  idle_.push_back(std::move(c));
  return true;
}

size_t RedisPool::size() const { return pool_size_; } // 配置的连接池容量

RedisPool::Guard RedisPool::acquire() {
  for (;;) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (!initialized_) {
      return Guard(*this, nullptr);
      // 从未 connect，等价旧 isConnected()==false
    }
    if (!idle_.empty()) {
      ConnPtr c = std::move(idle_.back());
      idle_.pop_back();
      ++active_;
      lk.unlock(); // 探活是阻塞网络操作，不持锁
      if(ping(c.get())){
      return Guard(*this, std::move(c));
      }
      // 空闲连接已断：释放死连接，回到循环走下方拨号分支（重连）
        LOG_ERROR("RedisPool::acquire: stale connection to %s:%d, reconnecting",
                  ip_.c_str(), port_);
        c.reset();
        lk.lock();
        --active_;
        continue;
    }
    if (active_ < pool_size_) {
      ++active_; // 先占位，防并发过度拨号
      lk.unlock();
      ConnPtr c = dial(); // 阻塞式 connect，不持锁
      if (!c) {
        lk.lock();
        --active_;
        cv_.notify_one();
        return Guard(*this, nullptr);
      }
      return Guard(*this, std::move(c));
    }
    cv_.wait(lk); // 池耗尽 → 等 release 唤醒
  }
}

void RedisPool::release(ConnPtr conn, bool broken) {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    // 关键：只有真正持有连接的 Guard 才占用 active_ 名额；null Guard（未初始化
    // /
    // 拨号失败）不递减，否则 size_t 会下溢。
    if (conn) {
      --active_;
      if (!broken) {
        idle_.push_back(std::move(conn)); // 健康归还
      }
      // broken：conn 出作用域 → ContextDeleter → redisFree
    }
  }
  cv_.notify_one();
}

RedisPool::ConnPtr RedisPool::dial() {
  redisContext *ctx = redisConnect(ip_.c_str(), port_);
  if (ctx == nullptr) {
    LOG_ERROR("RedisPool::dial: redisConnect failed (null)");
    return nullptr;
  }
  if (ctx->err != 0) { // 连接被拒等：返回非空但 err != 0
    LOG_ERROR("RedisPool::dial: redisConnect to %s:%d failed: %s", ip_.c_str(),
              port_, ctx->errstr);
    redisFree(ctx);
    return nullptr;
  }
  return ConnPtr(ctx);
}

bool RedisPool::ping(redisContext *ctx) {
  redisReply *reply = static_cast<redisReply *>(redisCommand(ctx, "PING"));
  if (!reply || ctx->err != 0) {
    return false;
  }
  bool ok = (reply->type == REDIS_REPLY_STATUS &&
             std::string(reply->str, reply->len) == "PONG");
  freeReplyObject(reply);
  return ok;
}

//---- Guard 实现 ----

RedisPool::Guard::Guard(RedisPool &pool, ConnPtr conn)
    : pool_(pool), conn_(std::move(conn)) {}

RedisPool::Guard::~Guard() {
  if (armed_) {
    pool_.release(std::move(conn_), broken_);
  }
}

RedisPool::Guard::Guard(Guard &&o) noexcept
    : pool_(o.pool_), conn_(std::move(o.conn_)), broken_(o.broken_),
      armed_(o.armed_) {
  o.armed_ = false; // 源 disarm，析构不再归还
}
