#include "deliver_server.h"
#include "Logger.h"
#include "mq_constants.h"
#include "rpc_protocol.h"
#include <chrono>
#include <memory>
#include <thread>

namespace {
constexpr int kBatchSize = 100;
constexpr int64_t kMinIdleMs = 30000; // 重试间隔：闲置 30s 仍没 ACK 就回收
constexpr int kMaxRetry = 3;          // 与旧 MAX_RETRY 一致
constexpr int kRetryTtlSec = 3600;     // retry side key 的 TTL
constexpr int kReclaimIntervalSec = 5; // 回收扫描周期
} // namespace

DeliverServer::DeliverServer(const std::string &server_id, uint64_t worker_id,
                             const std::string &route_service,
                             const std::string &etcd_endpoints,
                             const std::string &redis_ip, int redis_port,
                             const std::string &consumer_name)
    : message_store_(server_id, worker_id, /*enable_snowflake=*/false),
      route_client_(etcd_endpoints, route_service,
                    std::make_shared<ConsistentHashBalancer>(150)),
      consumer_name_(consumer_name), redis_ip_(redis_ip),
      redis_port_(redis_port) {}

DeliverServer::~DeliverServer() { stop(); }

bool DeliverServer::init() {
  if (!consumer_.connect(redis_ip_, redis_port_)) {
    LOG_ERROR("DeliverServer: failed to connect Redis at %s:%d",
              redis_ip_.c_str(), redis_port_);
    return false;
  }
  if (!message_store_.connect(redis_ip_, redis_port_)) {
    LOG_ERROR("DeliverServer: failed to connect MessageStore Redis");
    return false;
  }
  if (!consumer_.ensureGroup(immq::kDeliveryStream, immq::kDeliveryGroup)) {
    LOG_ERROR("DeliverServer: failed to create consumer group %s",
              immq::kDeliveryGroup);
    return false;
  }
  LOG_INFO("DeliverServer: consumer %s ready (stream=%s group=%s)",
           consumer_name_.c_str(), immq::kDeliveryStream, immq::kDeliveryGroup);
  return true;
}

void DeliverServer::start() {
  loop_thread_ = std::thread([this] { runLoop(); });
}

void DeliverServer::stop() {
  stop_.store(true);
  if (loop_thread_.joinable()) {
    loop_thread_.join();
  }
}

void DeliverServer::runLoop() {
  using namespace std::chrono;
  auto last_reclaim = steady_clock::now();
  while (!stop_.load()) {
    int n = consumer_.consume(
        immq::kDeliveryStream, immq::kDeliveryGroup, consumer_name_, kBatchSize,
        [this](const std::string &id, const std::string &body) {
          onMessage(id, body);
        });
    if (n == 0) {
      std::this_thread::sleep_for(milliseconds(100));
    }
    auto now = steady_clock::now();
    if (now - last_reclaim >= seconds(kReclaimIntervalSec)) {
      last_reclaim = now;
      consumer_.reclaim(immq::kDeliveryStream, immq::kDeliveryGroup,
                        consumer_name_, kMinIdleMs, kBatchSize,
                        [this](const std::string &id, const std::string &body) {
                          onReclaim(id, body);
                        });
    }
  }
}

void DeliverServer::onMessage(const std::string &entry_id,
                              const std::string &payload) {
  im::ChatMessage msg;
  if (!msg.ParseFromString(payload)) {
    LOG_ERROR("DeliverServer: entry %s parse fail, acked", entry_id.c_str());
    consumer_.ack(immq::kDeliveryStream, immq::kDeliveryGroup, entry_id);
    return;
  }
  // 幂等：已被 ACK 标记为已投递/已读 → 直接收尾
  im::MessageStatus st = message_store_.getStatus(msg.msg_id());
  if (st == im::MessageStatus::DELIVERED || st == im::MessageStatus::READ) {
    consumer_.ack(immq::kDeliveryStream, immq::kDeliveryGroup, entry_id);
    return;
  }
  // 投递：查路由 → 逐连接推 Gateway
  std::vector<im::RouteServer> servers = resolveRoutes(msg.to_user_id());
  if (servers.empty()) {
    // 真·离线：立即落离线库，不进重试链（保持旧语义）
    message_store_.storeOfflineMessage(msg.to_user_id(), msg);
    consumer_.ack(immq::kDeliveryStream, immq::kDeliveryGroup, entry_id);
    return;
  }
  std::string frame = chatFrame(msg);
  for (const auto &s : servers) {
    pushToGateway(s, frame);
  }
  // 推成功不 ACK：留在 PEL 等收件人 ACK，由 onReclaim 收尾
}

void DeliverServer::onReclaim(const std::string &entry_id,
                              const std::string &payload) {
  im::ChatMessage msg;
  if (!msg.ParseFromString(payload)) {
    LOG_ERROR("DeliverServer: reclaim entry %s parse fail, acked",
              entry_id.c_str());
    consumer_.ack(immq::kDeliveryStream, immq::kDeliveryGroup, entry_id);
    return;
  }
  im::MessageStatus st = message_store_.getStatus(msg.msg_id());
  if (st == im::MessageStatus::DELIVERED || st == im::MessageStatus::READ) {
    consumer_.ack(immq::kDeliveryStream, immq::kDeliveryGroup, entry_id);
    return;
  }
  int64_t retries = message_store_.incrementRetryCount(entry_id, kRetryTtlSec);
  if (retries > kMaxRetry) {
    // 超过最大重试 → 转离线收尾
    message_store_.storeOfflineMessage(msg.to_user_id(), msg);
    consumer_.ack(immq::kDeliveryStream, immq::kDeliveryGroup, entry_id);
    return;
  }
  // 再推一次（路由可能已恢复）
  std::vector<im::RouteServer> servers = resolveRoutes(msg.to_user_id());
  if (servers.empty()) {
    // 重试期间发现用户已离线 → 转离线，不再无谓重试
    message_store_.storeOfflineMessage(msg.to_user_id(), msg);
    consumer_.ack(immq::kDeliveryStream, immq::kDeliveryGroup, entry_id);
    return;
  }
  std::string frame = chatFrame(msg);
  for (const auto &s : servers) {
    pushToGateway(s, frame);
  }
  // 不 ACK：继续留 PEL，等 ACK 或下一轮回收
}

std::vector<im::RouteServer>
DeliverServer::resolveRoutes(const std::string &user_id) {
  // 投递是核心路径：回源 Route 权威数据，不用本地缓存。
  // deliver 不订阅路由事件，本地缓存会在「多端第二设备上线/下线」时陈旧，
  // 导致漏投到新上线的设备（缓存最长 60s 才过期）。强一致回源更稳。
  im::RouteQueryResponse resp = queryUserRoute(user_id);
  std::vector<im::RouteServer> servers;
  for (const auto &s : resp.servers()) {
    servers.push_back(s);
  }
  return servers;
}

im::RouteQueryResponse
DeliverServer::queryUserRoute(const std::string &user_id) {
  im::RouteQueryRequest req;
  req.set_user_id(user_id);
  std::string req_body = req.SerializeAsString();
  std::string resp_body;
  int error_code = 0;
  bool ok = route_client_.Call("RouteQuery", req_body, resp_body, error_code,
                               user_id);
  if (!ok || error_code != 0) {
    return {};
  }
  im::RouteQueryResponse resp;
  resp.ParseFromString(resp_body);
  return resp;
}

bool DeliverServer::pushToGateway(const im::RouteServer &server,
                                  const std::string &frame) {
  auto ch = gateway_channels_.getOrCreate(
      server.server_id(), server.server_ip(),
      static_cast<uint16_t>(server.server_port()));
  im::GatewayPushRequest req;
  req.set_conn_id(server.conn_id());
  req.set_frame(frame);
  req.set_force_close(false);
  std::string req_body = req.SerializeAsString();
  std::string resp_body;
  int32_t err = 0;
  return ch->Call("GatewayService", "Push", req_body, resp_body, err);
}

std::string DeliverServer::chatFrame(const im::ChatMessage &msg) {
  im::ServerPushEnvelope envelope;
  envelope.set_type(im::ServerPushEnvelope::CHAT_MESSAGE);
  msg.SerializeToString(envelope.mutable_payload());
  return packFrame(envelope);
}

std::string DeliverServer::packFrame(const google::protobuf::Message &msg) {
  std::string body;
  msg.SerializeToString(&body);
  uint32_t len = body.size();
  std::string frame;
  char lb[kHeaderLen];
  writeLenBE(lb, len); // 大端长度前缀
  frame.append(lb, kHeaderLen);
  frame.append(body);
  return frame;
}