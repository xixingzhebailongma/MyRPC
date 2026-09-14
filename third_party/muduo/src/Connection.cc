#include "../include/Connection.h"
#include "Logger.h"
#include <asm-generic/errno.h>
#include <chrono>
#include <cstring>
#include <memory>
#include <openssl/err.h> // ← 新增：ERR_get_error / ERR_error_string
#include <openssl/ssl.h> // ← 新增
Connection::Connection(EventLoop *loop, std::unique_ptr<Socket> clientsock,
                       SSL_CTX *tls_ctx)
    : loop_(loop), clientsock_(std::move(clientsock)),
      clientchannel_(new Channel(loop_, clientsock_->fd())),
      disconnect_(false) {
  clientsock_->settcpnodelay(true); // 关闭 Nagle，小 RPC 帧不再被拖 40ms
  clientsock_->setkeepalive(true); // 开启 TCP keepalive 探测
  clientchannel_->setreadcallback(std::bind(&Connection::onmessage, this));
  clientchannel_->setclosecallback(std::bind(&Connection::closecallback, this));
  clientchannel_->seterrorcallback(std::bind(&Connection::errorcallback, this));
  clientchannel_->setwritecallback(std::bind(&Connection::writecallback, this));
  clientchannel_->useet(); // 客户端连上来的fd采用边缘触发。

  // TLS 必须在 enablereading() 之前挂好：否则
  // ClientHello 可能先被当明文读走。
  if (tls_ctx != nullptr) {
    ssl_.reset(SSL_new(tls_ctx));
    if (!ssl_) {
      LOG_ERROR("Connection: SSL_new failed for fd %d", clientsock_->fd());
      disconnect_ = true;
    } else {
      tls_enabled_ = true;
      SSL_set_fd(ssl_.get(), clientsock_->fd());
      SSL_set_accept_state(ssl_.get());
    }
  }
  // 注意：不再在此处 enablereading()。
  // 读事件注册移到 TcpServer::newconnection 里 tieChannel() 之后执行，
  // 否则 channel 在 tie_（生命周期守卫）尚未写入前就可能触发 handleevent()，
  // 与 tie 写入构成数据竞争（weak_ptr 非线程安全，会破坏控制块）。
}

bool Connection::enableReading() {
  if (!clientchannel_->enablereading()) {
    LOG_ERROR("Connection: failed to register read "
              "event for fd %d. Marking as "
              "disconnected.",
              clientsock_->fd());
    disconnect_ = true;
    return false;
  }
  return true;
}

Connection::~Connection() = default;

int Connection::fd() const { return clientsock_->fd(); }

std::string Connection::ip() const { return clientsock_->ip(); }

uint16_t Connection::port() const { return clientsock_->port(); }

void Connection::closecallback() {
  clientchannel_->remove();
  disconnect_ = true;
  loop_->removeconnection(fd()); // 新增：drop EventLoop::conns_ 的引用
  closecallback_(shared_from_this());
}

void Connection::errorcallback() {
  disconnect_ = true;
  clientchannel_->remove();
  loop_->removeconnection(fd());
  errorcallback_(shared_from_this());
}

void Connection::setclosecallback(std::function<void(spConnection)> fn) {
  closecallback_ = fn;
}

void Connection::seterrorcallback(std::function<void(spConnection)> fn) {
  errorcallback_ = fn;
}

void Connection::setonmessagecallback(
    std::function<void(spConnection, Buffer &)> fn) {
  onmessagecallback_ = fn;
}

void Connection::setsendcompletecallback(std::function<void(spConnection)> fn) {
  sendcompletecallback_ = fn;
}

// 驱动非阻塞 TLS 握手。WANT_READ 等下次 EPOLLIN，WANT_WRITE 等 EPOLLOUT。
void Connection::driveHandshake() {
  if (handshake_done_)
    return;
  int r = SSL_accept(ssl_.get());
  if (r == 1) {
    handshake_done_ = true;
    return;
  }
  int err = SSL_get_error(ssl_.get(), r);
  if (err == SSL_ERROR_WANT_READ) {
    return; // 等下一次 EPOLLIN
  } else if (err == SSL_ERROR_WANT_WRITE) {
    clientchannel_->enablewriting(); // 等 EPOLLOUT 再驱动
    return;
  }
  LOG_ERROR("Connection: TLS handshake failed fd=%d: %s", fd(),
            ERR_error_string(ERR_get_error(), nullptr));
  errorcallback();
}

void Connection::onmessage() {
  if (tls_enabled_) {
    if (!handshake_done_) {
      driveHandshake();
      return;
    }
    // TLS 已握手：SSL_read 解密进输入缓冲，循环读到 WANT_READ（ET 必须读空）
    char extrabuf[65536];
    bool got_data = false;
    while (true) {
      int nread = SSL_read(ssl_.get(), extrabuf, sizeof(extrabuf));
      if (nread > 0) {
        inputbuffer_.append(extrabuf, static_cast<size_t>(nread));
        got_data = true;
        lastActiveTime_ = std::chrono::steady_clock::now();
      } else {
        int err = SSL_get_error(ssl_.get(), nread);
        if (err == SSL_ERROR_WANT_READ) {
          break; // 本轮读空
        } else if (err == SSL_ERROR_WANT_WRITE) {
          clientchannel_->enablewriting();
          break;
        } else if (err == SSL_ERROR_ZERO_RETURN) {
          closecallback();
          return;
        } else {
          LOG_ERROR("onmessage: SSL_read failed fd=%d: %s", fd(),
                    ERR_error_string(ERR_get_error(), nullptr));
          errorcallback();
          return;
        }
      }
    }
    if (got_data)
      onmessagecallback_(shared_from_this(), inputbuffer_);
    return;
  }

  // 明文路径（原逻辑不变）
  int savedErrno = 0;
  bool got_data = false;
  while (true) {
    ssize_t nread = inputbuffer_.readFd(fd(), &savedErrno);
    if (nread > 0) {
      got_data = true;
      lastActiveTime_ = std::chrono::steady_clock::now();
    } else if (nread == -1 && savedErrno == EINTR) {
      continue;
    } else if (nread == -1 &&
               (savedErrno == EAGAIN || savedErrno == EWOULDBLOCK)) {
      break; // 读完了
    } else if (nread == 0) {
      closecallback();
      return;
    } else {
      LOG_ERROR("onmessage: read() failed for fd = %d: %s", fd(),
                strerror(savedErrno));
      errorcallback();
      return;
    }
  }
  // 哑管道：不分帧，把原始字节交给上层，由上层用 protocol 的 codec 自行拆帧。
  if (got_data) {
    onmessagecallback_(shared_from_this(), inputbuffer_);
  }
}

void Connection::send(const char *data, size_t size) {
  send(std::string(data, size)); // 构造临时串，走下面的移动重载
}

void Connection::send(std::string &&data) {
  send(std::make_shared<std::string>(
      std::move(data))); // move 进共享指针，无深拷贝
}

void Connection::send(std::shared_ptr<std::string> message) {
  if (disconnect_ == true) {
    LOG_WARN("客户端连接已断开(fd=%d)，send()直接返回。", fd());
    return;
  }
  if (loop_->isinloopthread()) {
    sendinloop(std::move(message));
  } else {
    std::weak_ptr<Connection> weak_self = shared_from_this();
    loop_->queueinloop([weak_self,
                        message]() { // shared_ptr 按值捕获：只 +1
                                     // 引用计数，不拷数据
      auto self = weak_self.lock();
      if (self) {
        self->sendinloop(message);
      }
    });
  }
}
void Connection::sendThenClose(const char *data, size_t size) {
  auto message = std::make_shared<std::string>(data, size);
  if (disconnect_ == true) {
    LOG_WARN("客户端连接已断开(fd=%d)，sendThenClose()直接返回。", fd());
    return;
  }
  // 关键：close_after_send_ 必须在 sendinloop 把帧放进输出缓冲*之后*才置位。
  // 若在调用瞬间置位，同一条连接上并发的另一帧（如 RPC 响应）的 writecallback
  // 会先于本帧入缓冲看到该标志而提前关闭连接，导致本帧被丢弃。
  std::weak_ptr<Connection> weak_self = shared_from_this();
  loop_->queueinloop([weak_self, message]() {
    auto self = weak_self.lock();
    if (self) {
      self->sendinloop(message);
      self->close_after_send_.store(true);
    }
  });
}
void Connection::forceClose() {
  if (disconnect_ == true)
    return; // 已关闭
  std::weak_ptr<Connection> weak_self = shared_from_this();
  loop_->queueinloop([weak_self]() {
    auto self = weak_self.lock();
    if (self) {
      self->closecallback();
    }
  });
}
void Connection::sendinloop(std::shared_ptr<std::string> data) {
  outputbuffer_.append(data->data(), data->size());
  if (!clientchannel_->enablewriting()) {
    LOG_ERROR("sendinloop: enablewriting() failed for "
              "fd %d, closing connection.",
              fd());
    closecallback();
    return;
  } // 注册写事件。
}

void Connection::writecallback() {
  if (tls_enabled_) {
    if (!handshake_done_) {
      driveHandshake(); // EPOLLOUT 触发：继续握手
      return;
    }
    while (outputbuffer_.readableBytes() > 0) {
      int n = SSL_write(ssl_.get(), outputbuffer_.peek(),
                        static_cast<int>(outputbuffer_.readableBytes()));
      if (n > 0) {
        outputbuffer_.retrieve(static_cast<size_t>(n)); // n = 消费的明文字节数
      } else {
        int err = SSL_get_error(ssl_.get(), n);
        if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) {
          break; // 等下次 EPOLLOUT
        }
        LOG_ERROR("SSL_write failed fd=%d: %s", fd(),
                  ERR_error_string(ERR_get_error(), nullptr));
        closecallback();
        return;
      }
    }
    if (outputbuffer_.readableBytes() == 0) {
      clientchannel_->disablewriting();
      if (close_after_send_.load()) {
        closecallback(); // 帧已全部写出，此时关闭不会丢数据
        return;
      }
      if (sendcompletecallback_)
        sendcompletecallback_(shared_from_this());
    }
    return;
  }

  // 明文路径（原逻辑不变）
  while (outputbuffer_.readableBytes() > 0) {
    ssize_t writen =
        ::send(fd(), outputbuffer_.peek(), outputbuffer_.readableBytes(), 0);
    if (writen > 0) {
      outputbuffer_.retrieve(writen);
    } else if (writen == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      break; // 内核缓冲区满了，等下次 EPOLLOUT
    } else {
      LOG_ERROR("send() failed for fd = %d:%s", fd(), strerror(errno));
      closecallback();
      break;
    }
  }

  // 发送完成判断移到循环外，逻辑与 TLS 分支（第 244–250 行）保持一致
  if (outputbuffer_.readableBytes() == 0) {
    clientchannel_->disablewriting();
    if (close_after_send_.load()) {
      closecallback(); // 帧已全部写出，此时关闭不会丢数据
      return;
    }
    if (sendcompletecallback_) {
      sendcompletecallback_(shared_from_this());
    }
  }
}

void Connection::setIdleTimeout(double seconds) {
  idletimeout_ = seconds;
  std::weak_ptr<Connection> weak_self = shared_from_this();
  loop_->queueinloop([weak_self, seconds] {
    auto self = weak_self.lock();
    if (self && !self->disconnected()) {
      self->armIdleTimerInLoop(seconds);
    }
  });
}

void Connection::armIdleTimerInLoop(double seconds) {
  std::weak_ptr<Connection> weak_self = shared_from_this();
  loop_->runAfter(seconds, [weak_self, seconds]() {
    auto self = weak_self.lock();
    if (!self || self->disconnected())
      return;
    if (self->idleExpired(seconds)) {
      LOG_INFO("Connection(fd=%d) idle %.1fs, closing.", self->fd(), seconds);
      self->closecallback();
    } else {
      self->armIdleTimerInLoop(seconds); // 仍活跃，重装
    }
  });
}

bool Connection::idleExpired(double timeout) const {
  return std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                       lastActiveTime_)
             .count() >= timeout;
}