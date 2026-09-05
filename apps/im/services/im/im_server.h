#pragma once
#include "Connection.h"
#include "EventLoop.h"
#include "im.pb.h" // 所有 IM 消息类型
#include "lb_rpc_client.h"
#include "message_store.h"
#include "redis_client.h"
#include "redis_subscriber.h"
#include "route_cache.h"
#include "rpc_channel_pool.h"
#include "rpc_server.h"
#include "stream_producer.h"
#include "user_dao.h"
#include "user_manager.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <google/protobuf/message.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace im;
class ImServer {
public:
  // ip/port:       对外服务地址（客户端和其他 IM Server 都连这里）
  // server_id:     本服务器 ID，如 "IM1"
  // route_service:  Route Server 在 etcd 中的服务名，如 "RouteService"
  // etcd_endpoints: etcd 地址
  ImServer(const std::string &ip, int port, const std::string &server_id,
           uint64_t worker_id, const std::string &route_service,
           const std::string &etcd_endpoints, const std::string &redis_ip,
           int redis_port, const DbConfig &db_cfg,
           const std::string &auth_service = "AuthService",
           bool auth_enabled = true, const std::string &shared_secret = "");
  void start();
  void stop();

private:
  //面向客户端的RPC接口
  std::string handleLogin(spConnection conn, const std::string &request_body,
                          const RpcHeader &hdr);
  std::string handleSendMessage(spConnection conn,
                                const std::string &request_body,
                                const RpcHeader &hdr);
  std::string handleGetFriendList(spConnection conn,
                                  const std::string &request_body,
                                  const RpcHeader &hdr);
  std::string handleRegister(spConnection conn,
                             const std::string &request_body);
  std::string handleAddFriend(spConnection conn,
                              const std::string &request_body,
                              const RpcHeader &hdr);

  // ACK确认 &离线消息拉取
  std::string handleAckMessage(spConnection conn,
                               const std::string &request_body);
  std::string handlePullOfflineMessages(spConnection conn,
                                        const std::string &request_body,
                                        const RpcHeader &hdr);

  bool deliverLocal(const ChatMessage &msg, bool store_on_miss = true);
  //重载：推送ACK通知
  bool deliverLocal(const MessageAck &ack);
  // 把已打包的帧推给某个 Gateway 上的连接；force_close=true 时发完让 Gateway
  // 关连接
  bool pushToGateway(const RouteServer &server, const std::string &frame,
                     bool force_close = false);
  // Route Server交互
  RouteQueryResponse queryUserRoute(const std::string &user_id);
  std::vector<RouteServer> resolveRoutes(const std::string &user_id);
  bool registerUserOnline(const std::string &user_id, const ClientConnRef &ref);
  bool unregisterUserFromRoute(const std::string &user_id,
                               const ClientConnRef &ref);
  // 工具：把一个 protobuf 消息打包成 [4字节LE长度][序列化数据] 的帧
  std::string packFrame(const google::protobuf::Message &msg);

  // 周期任务：统计并打印在线用户数（样板：定时器回调只发令，重活丢工作池）
  void reportOnlineStats();

  // 验证 Gateway 注入的身份：时间戳新鲜度 → nonce 防重放 → HMAC 验签。
  bool verifyIdentity(const RpcHeader &hdr, std::string *err);

  void onRouteChange(const std::string &payload);
  //新增handler
  std::string handleChangePassword(spConnection conn,
                                   const std::string &request_body,
                                   const RpcHeader &hdr);
  std::string handleConnect(spConnection conn, const std::string &request_body,
                            const RpcHeader &hdr);
  std::string handleIssueTicket(spConnection conn,
                                const std::string &request_body);
  std::string handleRefresh(spConnection conn, const std::string &request_body);
  std::string handleLogout(spConnection conn, const std::string &request_body,
                           const RpcHeader &hdr);
  // Gateway -> IM：通知某连接已断开（gateway_id/conn_id 在 body 里）
  std::string handleClientDisconnect(spConnection conn,
                                     const std::string &request_body);
  // 事件消费：收到用户数据变更事件后踢下线
  void onUserChanged(const std::string &payload);
  void kickOffline(const im::UserChangedEvent &ev);

  // 成员（在 route_subscriber_ 附近加）
  RedisSubscriber user_subscriber_; // 订阅用户数据变更事件

  RpcServer rpc_server_;
  UserManager user_manager_;
  RedisClient nonce_redis_; // 验签 nonce 防重放（SET NX EX 5）F
  MessageStore message_store_;
  StreamProducer producer_; // 消息队列生产者（投递下沉到 deliver_server）
  UserDao user_dao_;
  LbRpcClient route_client_; //调用Route Server
  LbRpcClient auth_client_;  //调用 Auth Server（无状态，轮询即可）
  RouteCache route_cache_;   // 本地路由缓存
  RedisSubscriber route_subscriber_; // 订阅路由变更事件
  //到其它IM Server的连接池（自持锁，调用方无需手动加锁）
  RpcChannelPool server_channels_;

  std::string server_id_;
  std::string ip_;
  int port_;
  std::string redis_ip_;
  int redis_port_;
  std::string auth_service_;
  bool auth_enabled_;
  std::string shared_secret_; // Gateway↔IM 共享密钥
};