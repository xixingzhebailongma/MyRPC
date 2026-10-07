#include "async_lb_rpc_client.h"
#include "Logger.h"
#include "async_rpc_channel.h"
#include "rpc_error_code.h"
#include "uuid.h"
#include <algorithm>

AsyncLbRpcClient::AsyncLbRpcClient(const std::string &etcdEndpoints,
                                   const std::string &serviceName,
                                   std::shared_ptr<ILoadBalancer> balancer,
                                   const RpcClientConfig &cfg)
    : discovery_(std::make_shared<ServiceDiscovery>(etcdEndpoints)),
      balancer_(std::move(balancer)), serviceName_(serviceName), cfg_(cfg),
      client_(0, cfg) {
  // 节点表由 watch 首轮 Resync 同步；首调若早于 Resync 落地，走 pick 路径的
  // 懒 discover 兜底，所以这里不再同步全量 refreshNodes。
  LOG_INFO("AsyncLbRpcClient: initialized for service=%s (nodes synced via watch)",
           serviceName_.c_str());

  // etcd watch：节点上下线以增量 Add/Remove 应用，只在首连/compacted/断连过久
  // 时投递一次 ReplaceAll（全量）。
  discovery_->watch(serviceName_, [this](const ServiceNodeEvent &e) {
    onNodeEvent(e);
  });
}

AsyncLbRpcClient::~AsyncLbRpcClient() { discovery_->stopWatch(); }

void AsyncLbRpcClient::onNodeEvent(const ServiceNodeEvent &e) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<ServiceNode> updated = nodes_;
  switch (e.type) {
    case ServiceNodeEvent::Type::ReplaceAll:
      updated = e.all;
      break;
    case ServiceNodeEvent::Type::Add: {
      bool exists = false;
      for (const auto &n : updated) {
        if (n.address() == e.node.address()) {
          exists = true;
          break;
        }
      }
      if (!exists)
        updated.push_back(e.node);
      break;
    }
    case ServiceNodeEvent::Type::Remove:
      updated.erase(std::remove_if(updated.begin(), updated.end(),
                                   [&](const ServiceNode &n) {
                                     return n.address() == e.address;
                                   }),
                    updated.end());
      break;
  }
  applyNodes(std::move(updated));
}

void AsyncLbRpcClient::applyNodes(std::vector<ServiceNode> newNodes) {
  nodes_ = std::move(newNodes);
  std::vector<std::string> ids;
  ids.reserve(nodes_.size());
  for (const auto &n : nodes_)
    ids.push_back(n.address());
  balancer_->rebuild(ids);
  std::unordered_set<std::string> alive(ids.begin(), ids.end());
  client_.removeExcept(alive);
}

std::future<std::string> AsyncLbRpcClient::Call(const std::string &methodName,
                                                const std::string &requestBody,
                                                int timeout_ms) {
  auto promise = std::make_shared<std::promise<std::string>>();
  auto fut = promise->get_future();
  CallImpl(methodName, requestBody, nullptr, timeout_ms, promise, nullptr);
  return fut;
}

std::future<std::string> AsyncLbRpcClient::Call(const std::string &methodName,
                                                const std::string &requestBody,
                                                const std::string &key,
                                                int timeout_ms) {
  auto promise = std::make_shared<std::promise<std::string>>();
  auto fut = promise->get_future();
  CallImpl(methodName, requestBody, &key, timeout_ms, promise, nullptr);
  return fut;
}

void AsyncLbRpcClient::Call(const std::string &methodName,
                            const std::string &requestBody, ResponseCallback cb,
                            int timeout_ms) {
  CallImpl(methodName, requestBody, nullptr, timeout_ms, nullptr,
           std::move(cb));
}

void AsyncLbRpcClient::Call(const std::string &methodName,
                            const std::string &requestBody,
                            const std::string &key, ResponseCallback cb,
                            int timeout_ms) {
  CallImpl(methodName, requestBody, &key, timeout_ms, nullptr, std::move(cb));
}

void AsyncLbRpcClient::CallImpl(
    const std::string &methodName, const std::string &requestBody,
    const std::string *key, int timeout_ms,
    std::shared_ptr<std::promise<std::string>> promise, ResponseCallback cb) {
  auto st = std::make_shared<CallState>();
  st->methodName = methodName;
  st->requestBody = requestBody;
  st->request_id = generateUuid(); // 幂等键：跨 failover 复用
  st->maxRetries = cfg_.max_retries;
  st->timeout_ms = timeout_ms;
  st->key = key ? std::optional<std::string>(*key) : std::nullopt;
  st->promise = promise;
  st->cb = std::move(cb);
  attemptCall(st);
}

std::optional<ServiceNode>
AsyncLbRpcClient::pickNodeForAsync(const std::unordered_set<std::string> &tried,
                                   const std::string *key) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (nodes_.empty()) {
    if (auto n = discovery_->discover(serviceName_))
      applyNodes(std::move(*n));
  }
  if (nodes_.empty()) {
    LOG_ERROR("AsyncLbRpcClient: no available nodes for %s",
              serviceName_.c_str());
    return std::nullopt;
  }

  // 收集候选：跳过已尝试(tried)与熔断中(broken)的节点
  std::vector<size_t> candidates;
  std::unordered_set<size_t> broken;
  for (size_t i = 0; i < nodes_.size(); ++i) {
    if (tried.count(nodes_[i].address()))
      continue;
    if (client_.isCircuitOpen(nodes_[i].address())) {
      broken.insert(i);
      continue;
    }
    candidates.push_back(i);
  }
  if (candidates.empty()) {
    LOG_ERROR("AsyncLbRpcClient: all %zu nodes tried or circuit-broken for %s",
              nodes_.size(), serviceName_.c_str());
    return std::nullopt;
  }

  size_t idx;
  if (key) {
    std::unordered_set<size_t> excluded = broken;
    for (size_t i = 0; i < nodes_.size(); ++i)
      if (tried.count(nodes_[i].address()))
        excluded.insert(i);
    idx = balancer_->select(nodes_.size(), *key, excluded);
  } else {
    idx = balancer_->select(nodes_.size());
    if (tried.count(nodes_[idx].address()) || broken.count(idx)) {
      size_t rr = rr_counter_.fetch_add(1, std::memory_order_relaxed);
      idx = candidates[rr % candidates.size()];
    }
  }
  return nodes_[idx];
}

void AsyncLbRpcClient::attemptCall(std::shared_ptr<CallState> st) {
  const std::string *key = st->key ? &*st->key : nullptr;
  auto node = pickNodeForAsync(st->tried, key);
  if (!node) {
    deliver(st, "", static_cast<int32_t>(RpcError::NO_AVAILABLE_NODE));
    return;
  }
  st->tried.insert(node->address());
  client_.Call(
      node->ip, node->port, serviceName_, st->methodName, st->requestBody,
      [this, st](std::string body, int32_t ec) {
        onResponse(st, ec, std::move(body));
      },
      st->timeout_ms, st->request_id);
}

void AsyncLbRpcClient::onResponse(std::shared_ptr<CallState> st, int32_t ec,
                                  std::string body) {
  if (ec == 0) {
    deliver(st, std::move(body), 0);
    return;
  }
  if (isRetryable(ec) && static_cast<int>(st->tried.size()) <= st->maxRetries) {
    failover_count_.fetch_add(1, std::memory_order_relaxed);
    LOG_WARN("AsyncLbRpcClient: attempt %zu got error %d, failover...",
             st->tried.size(), ec);
    attemptCall(st);
    return;
  }
  deliver(st, "", ec);
}

void AsyncLbRpcClient::deliver(std::shared_ptr<CallState> st, std::string body,
                               int32_t ec) {
  if (st->promise) {
    if (ec == 0)
      st->promise->set_value(std::move(body));
    else
      st->promise->set_exception(std::make_exception_ptr(AsyncRpcError(ec)));
  } else if (st->cb) {
    st->cb(std::move(body), ec);
  }
}