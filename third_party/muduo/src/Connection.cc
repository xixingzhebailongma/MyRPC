#include "../include/Connection.h"
#include "Logger.h"
#include <asm-generic/errno.h>
#include <chrono>
#include <cstring>
#include <memory>
Connection::Connection(EventLoop *loop, std::unique_ptr<Socket> clientsock)
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
  if (!clientchannel_->enablereading()) {
    LOG_ERROR("Connection: failed to register read event for fd %d. Marking as "
              "disconnected.",
              clientsock_->fd());
    disconnect_ = true;
    // 不能在构造函数里调closecallback()，因为shared_from_this()此时无效。
  }
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

void Connection::onmessage() {
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
    loop_->queueinloop(
        [weak_self,
         message]() { // shared_ptr 按值捕获：只 +1 引用计数，不拷数据
          auto self = weak_self.lock();
          if (self) {
            self->sendinloop(message);
          }
        });
  }
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
    LOG_ERROR(
        "sendinloop: enablewriting() failed for fd %d, closing connection.",
        fd());
    closecallback();
    return;
  } // 注册写事件。
}

void Connection::writecallback() {
  while (outputbuffer_.readableBytes() > 0) {
    ssize_t writen =
        ::send(fd(), outputbuffer_.peek(), outputbuffer_.readableBytes(), 0);
    if (writen > 0) {
      outputbuffer_.retrieve(writen);
    } else if (writen == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      break; // 内核缓冲区满了，等下次 EPOLLOUT
    } else {
      // 真正的错误，关闭连接
      LOG_ERROR("send() failed for fd = %d:%s", fd(), strerror(errno));
      closecallback();
      break;
    }
  }
  if (outputbuffer_.readableBytes() == 0) {
    clientchannel_->disablewriting();
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
      self->closecallback(); // 完整拆除：摘 channel、置 disconnect_、回调
                             // TcpServer::closeconnection
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
