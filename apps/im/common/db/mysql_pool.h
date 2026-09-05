#pragma once
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace sql {
class Connection;
}

// MySQL 连接池：与 im/redis/redis_pool.h 的 RedisPool 同一套设计。
// 线程安全；acquire() 借连接，Guard 析构自动归还；连接断了用 markBroken()
// 丢弃。
class MysqlPool {
public:
  struct ConnDeleter {
    void
    operator()(sql::Connection *conn) const; // delete conn（析构自动 close）
  };
  using ConnPtr = std::unique_ptr<sql::Connection, ConnDeleter>;

  MysqlPool();
  ~MysqlPool(); // idle_ 里的连接由 ConnPtr 析构自动 delete

  MysqlPool(const MysqlPool &) = delete;
  MysqlPool &operator=(const MysqlPool &) = delete;

  bool init(const std::string &host, unsigned int port, const std::string &user,
            const std::string &passwd, const std::string &db, size_t pool_size);

  class Guard {
  public:
    Guard(MysqlPool &pool, ConnPtr conn);
    ~Guard(); // 析构时归还/丢弃连接
    Guard(const Guard &) = delete;
    Guard &operator=(const Guard &) = delete;
    Guard(Guard &&o) noexcept;

    sql::Connection *get() const noexcept { return conn_.get(); }
    explicit operator bool() const noexcept { return static_cast<bool>(conn_); }
    void markBroken() noexcept { broken_ = true; }

  private:
    MysqlPool &pool_;
    ConnPtr conn_;
    bool broken_ = false;
    bool armed_ = true;
  };
  Guard acquire();

private:
  friend class Guard;
  void release(ConnPtr conn, bool broken);
  ConnPtr dial(); // driver->connect，失败返回 nullptr
  bool validate(sql::Connection *conn); // 借出前探活：isClosed/isValid

  std::string host_;
  unsigned int port_ = 3306;
  std::string user_;
  std::string passwd_;
  std::string db_;
  size_t pool_size_ = 4;
  std::vector<ConnPtr> idle_; // 空闲连接
  size_t active_ = 0;         // 当前已借出（含拨号中）
  bool initialized_ = false;
  std::mutex mutex_;
  std::condition_variable cv_;
};