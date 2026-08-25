#include "mysql_pool.h"
#include "Logger.h"
#include <cppconn/connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <mysql_driver.h>

void MysqlPool::ConnDeleter::operator()(sql::Connection *conn) const {
  delete conn; // Connector/C++ 用 delete 释放，析构内部会 close
}

MysqlPool::MysqlPool() = default;
MysqlPool::~MysqlPool() = default;

bool MysqlPool::init(const std::string &host, unsigned int port,
                     const std::string &user, const std::string &passwd,
                     const std::string &db, size_t pool_size) {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    host_ = host;
    port_ = port;
    user_ = user;
    passwd_ = passwd;
    db_ = db;
    pool_size_ = pool_size;
    initialized_ = true;
  }
  // 急切拨 1 条验证连通；其余按需惰性建
  ConnPtr c = dial();
  if (!c) {
    LOG_ERROR("MysqlPool::init: initial connect to %s:%u failed", host.c_str(),
              port);
    return false;
  }
  std::lock_guard<std::mutex> lk(mutex_);
  idle_.push_back(std::move(c));
  return true;
}

MysqlPool::Guard MysqlPool::acquire() {
  for (;;) {
    std::unique_lock<std::mutex> lk(mutex_);
    if (!initialized_) {
      return Guard(*this, nullptr);
    }
    if (!idle_.empty()) {
      ConnPtr c = std::move(idle_.back());
      idle_.pop_back();
      ++active_;
      lk.unlock(); //校验是网络操作，不持锁
      if (validate(c.get())) {
        return Guard(*this, std::move(c));
      }
      LOG_ERROR("MysqlPool::acquire: stale connection to %s:%u, reconnecting",
                  host_.c_str(), port_);
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

void MysqlPool::release(ConnPtr conn, bool broken) {
  {
    std::lock_guard<std::mutex> lk(mutex_);
    if (conn) {
      --active_;
      if (!broken) {
        idle_.push_back(std::move(conn)); // 健康归还
      }
      // broken：conn 出作用域 → ConnDeleter → delete
    }
  }
  cv_.notify_one();
}

MysqlPool::ConnPtr MysqlPool::dial() {
  try {
    sql::mysql::MySQL_Driver *driver = sql::mysql::get_mysql_driver_instance();
    std::string host = "tcp://" + host_ + ":" + std::to_string(port_);
    ConnPtr conn(driver->connect(host, user_, passwd_));
    conn->setSchema(db_);
    return conn;
  } catch (sql::SQLException &e) {
    LOG_ERROR("MysqlPool::dial failed: %s (errno=%d)", e.what(),
              e.getErrorCode());
    return nullptr;
  }
}
bool MysqlPool::validate(sql::Connection *conn) {
  if (!conn)
    return false;
  try {
    return !conn->isClosed() && conn->isValid();
  } catch (sql::SQLException &e) {
    return false;
  }
}
// ---- Guard 实现 ----
MysqlPool::Guard::Guard(MysqlPool &pool, ConnPtr conn)
    : pool_(pool), conn_(std::move(conn)) {}

MysqlPool::Guard::~Guard() {
  if (armed_) {
    pool_.release(std::move(conn_), broken_);
  }
}

MysqlPool::Guard::Guard(Guard &&o) noexcept
    : pool_(o.pool_), conn_(std::move(o.conn_)), broken_(o.broken_),
      armed_(o.armed_) {
  o.armed_ = false;
}