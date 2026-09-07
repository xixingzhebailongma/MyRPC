#include "../include/TcpClient.h"
#include "../include/Connector.h"
#include "../include/EventLoop.h"
#include "../include/Socket.h"

TcpClient::TcpClient(EventLoop *loop, const InetAddress &serverAddr,
                     const std::string &name)
    : loop_(loop), serverAddr_(serverAddr), name_(name),
      connector_(new Connector(loop, serverAddr)) {
  connector_->setNewConnectionCallback(
      [this](int sockfd) { newConnection(sockfd); });
}

TcpClient::~TcpClient() { stop(); }

void TcpClient::connect() {
  connect_ = true;
  connector_->start();
}

void TcpClient::disconnect() {
  connect_ = false;
  loop_->queueinloop([this] {
    if (connection_)
      connection_->forceClose();
  });
}

void TcpClient::stop() {
  connect_ = false;
  connector_->stop();
  loop_->queueinloop([this] {
    if (connection_)
      connection_->forceClose();
  });
}

// Connector 的回调：在 loop 线程内被调用
void TcpClient::newConnection(int sockfd) {
  std::unique_ptr<Socket> sock(new Socket(sockfd));
  sock->setipport(serverAddr_.ip(), serverAddr_.port());
  spConnection conn(new Connection(loop_, std::move(sock)));
  conn->setclosecallback([this](spConnection c) { removeConnection(c); });
  conn->seterrorcallback([this](spConnection c) { removeConnection(c); });
  conn->setonmessagecallback(messageCallback_);
  connection_ = conn;
  if (connectionCallback_)
    connectionCallback_(conn);
}

// Connection 断开时回调（loop 线程内）。真正释放投递到下一轮 loop，
// 避免在自身回调里析构自己。
void TcpClient::removeConnection(const spConnection &conn) {
  loop_->queueinloop([this, conn] {
    if (closeCallback_)
      closeCallback_(conn);
    connection_.reset();
    if (retry_ && connect_) {
      loop_->runAfter(kRetryDelaySec, [this] { connector_->restart(); });
    }
  });
}