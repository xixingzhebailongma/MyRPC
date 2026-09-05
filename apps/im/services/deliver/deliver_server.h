#pragma once
#include "im.pb.h"
#include "lb_rpc_client.h"
#include "message_store.h"
#include "route_cache.h"
#include "rpc_channel_pool.h"
#include "stream_consumer.h"
#include <atomic>
#include <cstdint>
#include <google/protobuf/message.h>
#include <string>
#include <thread>
#include <vector>

// 消息投递 worker：消费 im:delivery 流，查路由 → 推 Gateway → 重试/转离线。
class DeliverServer {
public:
  DeliverServer(const std::string &server_id, uint64_t worker_id,
                const std::string &route_service,
                const std::string &etcd_endpoints, const std::string &redis_ip,
                int redis_port, const std::string &consumer_name);
  ~DeliverServer();

  bool init();  // 连接 Redis（消费端 + MessageStore）+ 建消费组
  void start(); // 启动消费循环线程
  void stop();  // 停止循环

private:
  void runLoop();
  void onMessage(const std::string &entry_id, const std::string &payload);
  void onReclaim(const std::string &entry_id, const std::string &payload);

  std::vector<im::RouteServer> resolveRoutes(const std::string &user_id);
  im::RouteQueryResponse queryUserRoute(const std::string &user_id);
  bool pushToGateway(const im::RouteServer &server, const std::string &frame);
  std::string chatFrame(const im::ChatMessage &msg);
  std::string packFrame(const google::protobuf::Message &msg);

  StreamConsumer consumer_;
  MessageStore message_store_; // 状态 + 离线 + 重试计数
  LbRpcClient route_client_;   // 发现 RouteService
  RouteCache route_cache_;
  RpcChannelPool gateway_channels_; // 连各 Gateway

  std::string consumer_name_;
  std::string redis_ip_;
  int redis_port_;
  std::atomic<bool> stop_{false};
  std::thread loop_thread_;
};