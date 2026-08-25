#pragma once

#include "Connection.h"
#include "EventLoop.h"
#include "lb_rpc_client.h"
#include "user_dao.h"
#include "im.pb.h" // 所有 IM 消息类型
#include "message_store.h"
#include "redis_subscriber.h"
#include "route_cache.h"
#include "rpc_channel_pool.h"
#include "rpc_server.h"
#include "user_manager.h"
#include <cstring>
#include <google/protobuf/message.h>

#include <string>

using namespace im;
class ImServer {
public:
  // ip/port:       对外服务地址（客户端和其他 IM Server 都连这里）
  // server_id:     本服务器 ID，如 "IM1"
  // route_service:  Route Server 在 etcd 中的服务名，如 "RouteService"
  // etcd_endpoints: etcd 地址
  ImServer(const std::string &ip, int port, const std::string &server_id,
           const std::string &route_service, const std::string &etcd_endpoints,
           const std::string &redis_ip, int redis_port, const DbConfig &db_cfg);
  void start();
  void stop();

private:
  static constexpr int MAX_RETRY = 3;
  static constexpr int64_t RETRY_INTERVAL_MS = 30000;
  //面向客户端的RPC接口
  std::string handleLogin(spConnection conn, const std::string &request_body);
  std::string handleSendMessage(spConnection conn,
                                const std::string &request_body);
  std::string handleGetFriendList(spConnection conn,
                                  const std::string &request_body);
  std::string handleRegister(spConnection conn,
                             const std::string &request_body);
  std::string handleAddFriend(spConnection conn,
                              const std::string &request_body);
  //面向其它IM server的接口
  std::string handleForwardMessage(spConnection conn,
                                   const std::string &request_body);

  // ACK确认 &离线消息拉取
  std::string handleAckMessage(spConnection conn,
                               const std::string &request_body);
  std::string handlePullOfflineMessages(spConnection conn,
                                        const std::string &request_body);
  //消息路由核心
  bool routeMessage(const ChatMessage &msg);
  bool deliverLocal(const ChatMessage &msg);
  //重载：推送ACK通知
  bool deliverLocal(const MessageAck &ack);
  bool forwardToRemote(const ChatMessage &msg, const RouteQueryResponse &route);

  bool callRemote(const RouteQueryResponse &route, const std::string &method,
                  const google::protobuf::Message &req,
                  google::protobuf::Message *resp);
  bool forwardAckToRemote(const MessageAck &ack,
                          const RouteQueryResponse &route);
  // Route Server交互
  RouteQueryResponse queryUserRoute(const std::string &user_id);
  bool registerUserOnline(const std::string &user_id);
  bool unregisterUserFromRoute(const std::string &user_id);
  // 工具：把一个 protobuf 消息打包成 [4字节LE长度][序列化数据] 的帧
  std::string packFrame(const google::protobuf::Message &msg);
  void onRetryCheck(EventLoop *loop);
  void onRouteChange(const std::string &payload);
  //新增handler
  std::string handleChangePassword(spConnection conn,
                                   const std::string &request_body);
  // 事件消费：收到用户数据变更事件后踢下线
  void onUserChanged(const std::string &payload);
  void kickOffline(const im::UserChangedEvent &ev);

  // 成员（在 route_subscriber_ 附近加）
  RedisSubscriber user_subscriber_; // 订阅用户数据变更事件

  RpcServer rpc_server_;
  UserManager user_manager_;
  MessageStore message_store_;
  UserDao user_dao_;
  LbRpcClient route_client_;         //调用Route Server
  RouteCache route_cache_;           // 本地路由缓存
  RedisSubscriber route_subscriber_; // 订阅路由变更事件
  //到其它IM Server的连接池（自持锁，调用方无需手动加锁）
  RpcChannelPool server_channels_;

  std::string server_id_;
  std::string ip_;
  int port_;
  std::string redis_ip_;
  int redis_port_;
};