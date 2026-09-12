#include "../include/TcpServer.h"
#include "Logger.h"
#include <openssl/err.h> // ← 新增：ERR_get_error / ERR_error_string
TcpServer::TcpServer(const std::string &ip, const uint16_t port, int threadnum)
    : mainloop_(new EventLoop(true)), acceptor_(mainloop_.get(), ip, port),
      threadnum_(threadnum), threadpool_(threadnum_, "IO") {
  mainloop_->setepolltimeoutcallback(
      std::bind(&TcpServer::epolltimeout, this, std::placeholders::_1));
  acceptor_.setnewconnectioncb(
      std::bind(&TcpServer::newconnection, this, std::placeholders::_1));
  //创建从事件循环
  for (int ii = 0; ii < threadnum_; ++ii) {
    subloops_.emplace_back(new EventLoop(false));
    subloops_[ii]->setepolltimeoutcallback(
        std::bind(&TcpServer::epolltimeout, this, std::placeholders::_1));
    threadpool_.addtask(std::bind(&EventLoop::run, subloops_[ii].get()));
  }
}

TcpServer::~TcpServer() {
  if (!stopped_) {
    LOG_WARN("TcpServer 析构时尚未调用 stop()，执行紧急停止。");
    stop();
  }
}

void TcpServer::enableTls(const std::string &cert, const std::string &key) {
  tls_ctx_.reset(SSL_CTX_new(TLS_server_method()));
  if (!tls_ctx_) {
    LOG_ERROR("TcpServer: SSL_CTX_new failed");
    return;
  }
  if (SSL_CTX_use_certificate_chain_file(tls_ctx_.get(), cert.c_str()) != 1) {
    LOG_ERROR("TcpServer: load cert failed: %s",
              ERR_error_string(ERR_get_error(), nullptr));
    tls_ctx_.reset();
    return;
  }
  if (SSL_CTX_use_PrivateKey_file(tls_ctx_.get(), key.c_str(),
                                  SSL_FILETYPE_PEM) != 1) {
    LOG_ERROR("TcpServer: load key failed: %s",
              ERR_error_string(ERR_get_error(), nullptr));
    tls_ctx_.reset();
    return;
  }
  if (SSL_CTX_check_private_key(tls_ctx_.get()) != 1) {
    LOG_ERROR("TcpServer: private key mismatch: %s",
              ERR_error_string(ERR_get_error(), nullptr));
    tls_ctx_.reset();
    return;
  }
  LOG_INFO("TcpServer: TLS enabled (cert=%s)", cert.c_str());
}

void TcpServer::start() { mainloop_->run(); }

void TcpServer::stopAccept() {
  acceptor_.stop();
  LOG_INFO("Acceptor 已停止，不再接受新连接。");
}

void TcpServer::stop() {
  if (stopped_)
    return;
  stopped_ = true;

  // 1. 停止接受新连接
  acceptor_.stop();
  LOG_INFO("Acceptor 已停止，不再接受新连接。");

  // 2. 收集所有现有连接（避免在遍历中修改 conns_）
  std::vector<spConnection> conns;
  {
    std::lock_guard<std::mutex> gd(mmutex_);
    for (auto &[fd, conn] : conns_) {
      conns.push_back(conn);
    }
    conns_.clear();
  }

  // 3. 逐个优雅关闭连接
  //    closecallback() 链: disconnect_=true → 从 epoll 移除 → 回调业务层
  //    HandleDisconnect → 清理 Redis
  for (auto &conn : conns) {
    conn->closecallback();
  }
  LOG_INFO("所有客户端连接已关闭。");

  // 4. 停止主事件循环
  mainloop_->stop();
  LOG_INFO("主事件循环已停止。");

  // 5. 停止从事件循环
  for (int ii = 0; ii < threadnum_; ii++) {
    subloops_[ii]->stop();
  }
  LOG_INFO("从事件循环已停止。");

  // 6. 等待 IO 线程退出
  threadpool_.stop();
  LOG_INFO("IO 线程池已停止。");
}

void TcpServer::newconnection(std::unique_ptr<Socket> clientsock) {
  spConnection conn(
      new Connection(subloops_[clientsock->fd() % threadnum_].get(),
                     std::move(clientsock), tls_ctx_.get()));
  // Channel 绑定到 Connection 的 shared_ptr：handleevent() 回调期间若连接关闭、
  // 最后一个 shared_ptr 被释放，Channel 仍能存活到回调返回（避免 use-after-free）。
  conn->tieChannel();
  conn->setclosecallback(
      std::bind(&TcpServer::closeconnection, this, std::placeholders::_1));
  conn->seterrorcallback(
      std::bind(&TcpServer::errorconnection, this, std::placeholders::_1));
  conn->setonmessagecallback(std::bind(&TcpServer::onmessage, this,
                                       std::placeholders::_1,
                                       std::placeholders::_2));
  conn->setsendcompletecallback(
      std::bind(&TcpServer::sendcomplete, this, std::placeholders::_1));

  {
    std::lock_guard<std::mutex> gd(mmutex_);
    conns_[conn->fd()] = conn;
  }
  if (idletimeout_ > 0)
    conn->setIdleTimeout(idletimeout_); // 仅当配置了空闲超时后才下发
  subloops_[conn->fd() % threadnum_]->newconnection(conn);

  if (newconnectioncb_)
    newconnectioncb_(conn);
  // 回调上层业务类的HandleNewConnection()。

  // 最后再注册读事件：tie 已就绪、上层 newconnectioncb_ 已把 fd 登记进自己的
  // 表（如 gateway 的 fd_to_conn_），channel 才会开始触发 handleevent()，
  // 避免「channel 先收到帧、上层还没登记」的跨线程丢帧竞争。
  conn->enableReading();
}

// 关闭客户端的连接，在Connection类中回调此函数。
void TcpServer::closeconnection(spConnection conn) {
  if (closeconnectioncb_)
    closeconnectioncb_(conn); // 回调上层业务类的HandleClose()。

  // printf("client(fd=%d) disconnected.\n",conn->fd());
  {
    std::lock_guard<std::mutex> gd(mmutex_);
    conns_.erase(conn->fd()); // 从map中删除conn。
  }
}

// 客户端的连接错误，在Connection类中回调此函数。
void TcpServer::errorconnection(spConnection conn) {
  if (errorconnectioncb_)
    errorconnectioncb_(conn); // 回调上层业务类的HandleError()。

  // printf("client(fd=%d) error.\n",conn->fd());
  {
    std::lock_guard<std::mutex> gd(mmutex_);
    conns_.erase(conn->fd()); // 从map中删除conn。
  }
}

// 数据发送完成后，在Connection类中回调此函数。
void TcpServer::sendcomplete(spConnection conn) {
  // printf("send complete.\n");

  if (sendcompletecb_)
    sendcompletecb_(conn); // 回调上层业务类的HandleSendComplete()。
}

// epoll_wait()超时，在EventLoop类中回调此函数。
void TcpServer::epolltimeout(EventLoop *loop) {
  // printf("epoll_wait() timeout.\n");

  if (timeoutcb_)
    timeoutcb_(loop); // 回调上层业务类的HandleTimeOut()。
}

void TcpServer::setnewconnectioncb(std::function<void(spConnection)> fn) {
  newconnectioncb_ = fn;
}

void TcpServer::setcloseconnectioncb(std::function<void(spConnection)> fn) {
  closeconnectioncb_ = fn;
}

void TcpServer::seterrorconnectioncb(std::function<void(spConnection)> fn) {
  errorconnectioncb_ = fn;
}

void TcpServer::setonmessagecb(
    std::function<void(spConnection, Buffer &message)> fn) {
  onmessagecb_ = fn;
}

void TcpServer::setsendcompletecb(std::function<void(spConnection)> fn) {
  sendcompletecb_ = fn;
}

void TcpServer::settimeoutcb(std::function<void(EventLoop *)> fn) {
  timeoutcb_ = fn;
}

void TcpServer::setPeriodicTimer(double interval,
                                 std::function<void(EventLoop *)> fn) {
  // runEvery 只能在 loop 线程内调用（TimerQueue 操作 std::set 无锁），
  // 所以用 queueinloop 把"注册定时器"这个动作投递到主 loop 线程里执行。
  EventLoop *loop = mainloop_.get();
  loop->queueinloop([loop, interval, fn] {
    loop->runEvery(interval, [loop, fn]() { fn(loop); });
  });
}
// 删除conns_中的Connection对象，在EventLoop::handletimer()中将回调此函数。
void TcpServer::removeconn(int fd) {
  {
    std::lock_guard<std::mutex> gd(mmutex_);
    conns_.erase(fd); // 从map中删除conn。
  }

  if (removeconnectioncb_)
    removeconnectioncb_(fd);
}

void TcpServer::setremoveconnectioncb(std::function<void(int)> fn) {
  removeconnectioncb_ = fn;
}

spConnection TcpServer::getConnByFd(int fd) {
  std::lock_guard<std::mutex> gd(mmutex_);
  auto it = conns_.find(fd);
  if (it != conns_.end()) {
    return it->second;
  }
  return nullptr;
}

void TcpServer::onmessage(spConnection conn, Buffer &message) {
  if (onmessagecb_)
    onmessagecb_(conn, message);
}