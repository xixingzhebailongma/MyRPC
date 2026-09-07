#pragma once
#include "InetAddress.h"
#include <atomic>
#include <functional>
#include <memory>

class EventLoop;
class Channel;

// 非阻塞 connect 状态机：驱动一个 socket 完成 TCP 建连，
// 成功后把已连接 fd 通过回调交给上层（TcpClient）。
// 连接失败按指数退避自动重试；stop() 后不再重试。
class Connector {
public:
  using NewConnectionCallback = std::function<void(int sockfd)>;

  Connector(EventLoop *loop, const InetAddress &serverAddr);
  ~Connector();

  void start(); // 开始连接（线程安全：内部投递到 loop 线程）
  void restart(); // 重连（须在 loop 线程内调用）
  void stop();    // 停止，不再重试（线程安全）

  void setNewConnectionCallback(const NewConnectionCallback &cb) {
    newConnectionCallback_ = cb;
  }

private:
  enum class States { kDisconnected, kConnecting, kConnected };

  void startInLoop();
  void connect();
  void connecting(int sockfd);
  void handleWrite();
  void handleError();
  void retry(int sockfd);
  int removeAndResetChannel();

  static constexpr int kInitRetryDelayMs = 500;
  static constexpr int kMaxRetryDelayMs = 30 * 1000;

  EventLoop *loop_;
  InetAddress serverAddr_;
  NewConnectionCallback newConnectionCallback_;
  States state_{States::kDisconnected};
  std::unique_ptr<Channel> channel_;
  std::atomic<bool> connect_{false};
  int retryDelayMs_{kInitRetryDelayMs}; // 只在 loop 线程读写
};