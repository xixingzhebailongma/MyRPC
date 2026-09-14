#pragma once
#include "Buffer.h"
#include "Channel.h"
#include "EventLoop.h"
#include "InetAddress.h"
#include "Socket.h"
#include "Timer.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <openssl/ssl.h>
#include <sys/syscall.h>
class EventLoop;
class Channel;
class Connection;
using spConnection = std::shared_ptr<Connection>;

class Connection : public std::enable_shared_from_this<Connection> {
private:
  EventLoop *loop_;
  std::unique_ptr<Socket> clientsock_;
  std::unique_ptr<Channel> clientchannel_;
  Buffer inputbuffer_;
  Buffer outputbuffer_;
  std::atomic_bool disconnect_;
  std::atomic_bool close_after_send_{false}; // 发送缓冲刷空后关闭连接（force_close 用）
  Timestamp lastActiveTime_; //最近一次收到数据的时间
  double idletimeout_ = 0.0; //秒；0=禁用（默认不启用空闲检测）

  // TLS：ssl_ 持有本次连接的会话对象。SSL_new 内部会 up_ref ctx，
  // 所以 SSL_CTX 由 TcpServer 独立持有即可，本类只管 ssl_ 生命周期。
  std::unique_ptr<SSL, decltype(&SSL_free)> ssl_{nullptr, SSL_free};
  bool tls_enabled_ = false;
  bool handshake_done_ = false;

  std::function<void(spConnection)> closecallback_;
  std::function<void(spConnection)>
      errorcallback_; // fd_发生了错误的回调函数，将回调TcpServer::errorconnection()。
  std::function<void(spConnection, Buffer &)>
      onmessagecallback_; // 处理报文的回调函数，将回调TcpServer::onmessage()。
  std::function<void(spConnection)>
      sendcompletecallback_; // 发送数据完成后的回调函数，将回调TcpServer::sendcomplete()。

  void
  armIdleTimerInLoop(double seconds); // 在所属 loop 线程装一次性定时器并重装
  bool idleExpired(double timeout) const; //空闲是否已超时
  void driveHandshake(); // 驱动非阻塞 TLS 握手（SSL_accept）
public:
  Connection(EventLoop *loop, std::unique_ptr<Socket> clientsock,
             SSL_CTX *tls_ctx = nullptr);
  ~Connection();

  // 把 Channel 绑定到自身 shared_ptr（tie 守卫）。
  // 必须在 Connection 已置于 shared_ptr 之后调用（不能在本类构造函数里调）。
  void tieChannel() { clientchannel_->tie(shared_from_this()); }

  // 注册读事件（开始接收数据）。必须在 tieChannel() 之后调用，否则
  // handleevent() 里的 tie_.lock() 会与 tie 写入数据竞争。
  bool enableReading();

  int fd() const;
  std::string ip() const;
  uint16_t port() const;
  void onmessage(); // 处理对端发送过来的消息。
  void closecallback(); // TCP连接关闭（断开）的回调函数，供Channel回调。
  void errorcallback(); // TCP连接错误的回调函数，供Channel回调。
  void writecallback(); // 处理写事件的回调函数，供Channel回调。

  void setclosecallback(
      std::function<void(spConnection)> fn); // 设置关闭fd_的回调函数。
  void seterrorcallback(
      std::function<void(spConnection)> fn); // 设置fd_发生了错误的回调函数。
  void setonmessagecallback(std::function<void(spConnection, Buffer &)>
                                fn); // 设置处理报文的回调函数。
  void setsendcompletecallback(
      std::function<void(spConnection)> fn); // 发送数据完成后的回调函数。

  void send(const char *data, size_t size);
  void send(std::string &&data);
  // 移动语义：调用方交出字符串所有权，省一次深拷贝
  void send(std::shared_ptr<std::string> data);
  // 发送该帧后，待输出缓冲刷空再关闭连接（替代「send 后立即 forceClose」，
  // 否则 closecallback 先于 writecallback 执行，帧会被丢弃）。
  void sendThenClose(const char *data, size_t size);
  // 直接持有共享串，跨线程投递只加引用计数
  // 发送数据，如果当前线程是IO线程，直接调用此函数，如果是工作线程，将把此函数传给IO线程去执行。
  // void sendinloop(const char *data,size_t size);
  void sendinloop(std::shared_ptr<std::string> data);

  void setIdleTimeout(double seconds);
  void forceClose(); // 新增：线程安全地关闭连接（内部 queueinloop）
  bool disconnected() const { return disconnect_.load(); }
};