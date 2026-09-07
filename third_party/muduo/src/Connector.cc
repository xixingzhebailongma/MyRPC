#include "../include/Connector.h"
#include "../include/Channel.h"
#include "../include/EventLoop.h"
#include "../include/Socket.h"
#include "Logger.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

// 读取 connect 的真实结果：0 表示成功，非 0 是错误码
static int getSocketError(int sockfd) {
  int optval = 0;
  socklen_t optlen = sizeof(optval);
  if (::getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &optval, &optlen) < 0)
    return errno;
  return optval;
}

Connector::Connector(EventLoop *loop, const InetAddress &serverAddr)
    : loop_(loop), serverAddr_(serverAddr) {}

Connector::~Connector() { channel_.reset(); }

void Connector::start() {
  connect_ = true;
  loop_->queueinloop([this] { startInLoop(); });
}

void Connector::restart() {
  connect_ = true;
  startInLoop();
}

void Connector::stop() {
  connect_ = false;
  loop_->queueinloop([this] {
    if (state_ == States::kConnecting) {
      int fd = removeAndResetChannel();
      ::close(fd);
    }
  });
}

void Connector::startInLoop() {
  if (state_ == States::kConnecting)
    return; // 已有一次 connect 在途，避免重复建连
  connect();
}

void Connector::connect() {
  int sockfd = createnonblocking();
  int ret = ::connect(sockfd, serverAddr_.addr(), sizeof(sockaddr_in));
  int savedErrno = (ret == 0) ? 0 : errno;
  switch (savedErrno) {
  case 0:
  case EINPROGRESS:
  case EINTR:
  case EISCONN:
    connecting(sockfd);
    break;
  default:
    LOG_WARN("Connector: connect() to %s:%u failed: %s", serverAddr_.ip(),
             serverAddr_.port(), strerror(savedErrno));
    retry(sockfd);
    break;
  }
}

void Connector::connecting(int sockfd) {
  state_ = States::kConnecting;
  channel_.reset(new Channel(loop_, sockfd));
  channel_->setwritecallback([this] { handleWrite(); });
  channel_->seterrorcallback([this] { handleError(); });
  channel_->enablewriting(); // 等 EPOLLOUT 表示连接结果
}

void Connector::handleWrite() {
  if (state_ == States::kConnecting) {
    int sockfd = removeAndResetChannel();
    int err = getSocketError(sockfd);
    if (err != 0) {
      LOG_WARN("Connector: connect to %s:%u failed: %s", serverAddr_.ip(),
               serverAddr_.port(), strerror(err));
      retry(sockfd);
    } else {
      state_ = States::kConnected;
      retryDelayMs_ = kInitRetryDelayMs;
      LOG_INFO("Connector: connected to %s:%u", serverAddr_.ip(),
               serverAddr_.port());
      if (connect_ && newConnectionCallback_)
        newConnectionCallback_(sockfd); // fd 所有权移交
      else
        ::close(sockfd);
    }
  }
}

void Connector::handleError() {
  if (state_ == States::kConnecting) {
    int sockfd = removeAndResetChannel();
    retry(sockfd);
  }
}

void Connector::retry(int sockfd) {
  ::close(sockfd);
  state_ = States::kDisconnected;
  if (connect_) {
    LOG_WARN("Connector: retry connect to %s:%u in %d ms", serverAddr_.ip(),
             serverAddr_.port(), retryDelayMs_);
    loop_->runAfter(retryDelayMs_ / 1000.0, [this] { startInLoop(); });
    retryDelayMs_ = std::min(retryDelayMs_ * 2, kMaxRetryDelayMs);
  } else {
    retryDelayMs_ = kInitRetryDelayMs;
  }
}

int Connector::removeAndResetChannel() {
  channel_->remove(); // 清零事件 + 从 epoll 摘除
  int sockfd = channel_->fd();
  // 不能在当前 channel 回调里直接销毁 channel_（它正被 epoll 遍历使用），
  // 投递到下一轮 loop 再 reset。
  loop_->queueinloop([this] { channel_.reset(); });
  return sockfd;
}