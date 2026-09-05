#pragma once

#include "Connection.h"
#include "TcpServer.h"
#include "ThreadPool.h"
#include "lb_rpc_client.h"
#include "rpc_channel_pool.h"
#include "rpc_header.pb.h"
#include "rpc_server.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// IM 接入层（Gateway）：持有客户端长连接，透传转发到固定 IM 节点，
// 接收 IM 节点回推（GatewayService.Push）并写回对应连接。
//
// 改造后 Gateway 是信任域入口：Connect 首帧在这里调 Auth ResolveTicket
// 完成身份绑定，之后每条转发请求都注入 HMAC 签名的 Identity，IM 验签后信任。
class GatewayServer {
public:
  GatewayServer(const std::string &client_ip, uint16_t client_port,
                const std::string &rpc_ip, uint16_t rpc_port,
                const std::string &gateway_id,
                const std::string &etcd_endpoints,
                const std::string &im_service, const std::string &auth_service,
                const std::string &shared_secret);

  // 启用客户端接入 TLS（须在 start() 前调用）
  void enableClientTls(const std::string &cert, const std::string &key);

  void start();
  void stop();

private:
  static constexpr int kForwardMaxRetries =
      2; // 转发最多重试 2 次（共 3 次尝试）
  static constexpr int kRetryBackoffMs = 50;    // 每次重试前的小退避
  static constexpr int kForwardTimeoutMs = 500; // 单次转发超时（毫秒）

  struct ConnState {
    std::weak_ptr<Connection> conn; // weak：避免与 TcpServer 的强引用冲突
    std::string im_ip; // 固定到的 IM 节点（空 = 尚未固定）
    uint16_t im_port = 0;
    bool authenticated = false; // 是否已通过 ticket 完成身份绑定
    Identity identity;          // 绑定后的身份（Gateway 注入用）
  };

  void onNewConnection(spConnection conn);
  void onConnectionClosed(spConnection conn);
  void onClientMessage(spConnection conn, Buffer &buf);

  void forwardToIm(uint64_t conn_id, std::string payload);
  bool isRetryableMethod(const std::string &method) const;
  bool isPreAuthMethod(const std::string &method) const;
  void notifyDisconnect(uint64_t conn_id, const std::string &im_ip,
                        uint16_t im_port);
  std::shared_ptr<RpcChannel> getImChannel(const std::string &ip,
                                           uint16_t port);

  // 消费 ticket 并绑定身份：调 Auth ResolveTicket，成功后写入 ConnState。
  bool resolveTicket(uint64_t conn_id, const std::string &ticket);
  // 注入身份 + 时间戳 + nonce + HMAC 签名。
  void signAndInject(RpcMessage &req, const Identity &id);

  std::string handlePush(spConnection conn, const std::string &request_body);

  TcpServer client_server_; // 客户端侧：持有长连接
  ThreadPool work_pool_;    // 跑阻塞的 RPC 转发，避免阻塞 IO 线程
  RpcServer rpc_server_;    // IM 侧：接收回推

  LbRpcClient im_client_;      // 发现并选择 IM 节点
  LbRpcClient auth_client_;    // 调用 Auth（ResolveTicket）
  RpcChannelPool im_channels_; // 到 IM 节点的连接池（key=ip:port）

  std::string gateway_id_;
  std::string rpc_ip_;
  uint16_t rpc_port_;
  std::string shared_secret_;

  std::atomic<uint64_t> next_conn_id_{0};
  std::atomic<uint64_t> next_nonce_{0};

  std::mutex mutex_;
  std::unordered_map<int, uint64_t> fd_to_conn_;  // fd -> conn_id
  std::unordered_map<uint64_t, ConnState> conns_; // conn_id -> 状态
};