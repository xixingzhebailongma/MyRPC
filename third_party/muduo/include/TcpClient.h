#pragma once
#include "Connection.h"
#include "InetAddress.h"
#include <atomic>
#include <functional>
#include <memory>
#include <string>

class EventLoop;
class Connector;
using spConnection = std::shared_ptr<Connection>;

// 客户端 TCP 连接：内部用 Connector 非阻塞建连，拿到 fd 后包装成
// Connection，并管理断线自动重连。
class TcpClient {
public:
  using ConnectionCallback = std::function<void(spConnection)>;
  using MessageCallback = std::function<void(spConnection, Buffer &)>;
  using CloseCallback = std::function<void(spConnection)>;
  TcpClient(EventLoop *loop, const InetAddress &serverAddr,
            const std::string &name);
  ~TcpClient();

  void connect();    // 建立连接（线程安全）
  void disconnect(); // 断开并停止重试（线程安全）
  void stop();

  void enableRetry() { retry_ = true; }

  void setConnectionCallback(const ConnectionCallback &cb) {
    connectionCallback_ = cb;
  }
  void setMessageCallback(const MessageCallback &cb) { messageCallback_ = cb; }
  void setCloseCallback(const CloseCallback &cb) { closeCallback_ = cb; }

  const std::string &name() const { return name_; }

private:
  void newConnection(int sockfd);
  void removeConnection(const spConnection &conn);

  static constexpr double kRetryDelaySec = 3.0;

  EventLoop *loop_;
  InetAddress serverAddr_;
  const std::string name_;
  std::unique_ptr<Connector> connector_;
  ConnectionCallback connectionCallback_;
  MessageCallback messageCallback_;
  CloseCallback closeCallback_;
  std::atomic<bool> retry_{false};
  std::atomic<bool> connect_{true};
  std::shared_ptr<Connection> connection_; // 只在 loop 线程访问
};