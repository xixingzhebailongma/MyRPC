#include "stream_consumer.h"
#include "Logger.h"
#include <optional>

namespace {
// Stream 里承载消息体的字段名，必须与 stream_producer.cc 保持一致
constexpr const char *kBodyField = "body";

std::optional<std::string> findField(const StreamEntry &e,
                                     const std::string &field) {
  for (const auto &kv : e.fields) {
    if (kv.first == field) {
      return kv.second;
    }
  }
  return std::nullopt;
}
} // namespace

bool StreamConsumer::connect(const std::string &ip, int port) {
  return redis_.connect(ip, port);
}

bool StreamConsumer::ensureGroup(const std::string &stream,
                                 const std::string &group) {
  return redis_.xgroupCreate(stream, group);
}

int StreamConsumer::consume(const std::string &stream, const std::string &group,
                            const std::string &consumer, int count,
                            const Handler &handler) {
  // 读回一批后立刻释放连接再回调 handler，避免回调里再 acquire 时占着连接
  std::vector<StreamEntry> entries =
      redis_.xreadgroup(group, consumer, stream, count, 0);
  int n = 0;
  for (auto &e : entries) {
    auto body = findField(e, kBodyField);
    if (!body) {
      LOG_ERROR("StreamConsumer::consume: entry %s missing body field, acked",
                e.id.c_str());
      redis_.xack(stream, group, e.id);
      continue;
    }
    handler(e.id, *body);
    ++n;
  }
  return n;
}

int StreamConsumer::reclaim(const std::string &stream, const std::string &group,
                            const std::string &consumer, int64_t min_idle_ms,
                            int count, const Handler &handler) {
  XAutoClaimResult r =
      redis_.xautoclaim(stream, group, consumer, min_idle_ms, count, "0-0");
  if (!r.deleted_ids.empty()) {
    LOG_INFO("StreamConsumer::reclaim: %zu entries already evicted from stream",
             r.deleted_ids.size());
  }
  int n = 0;
  for (auto &e : r.claimed) {
    auto body = findField(e, kBodyField);
    if (!body) {
      LOG_ERROR("StreamConsumer::reclaim: entry %s missing body field, acked",
                e.id.c_str());
      redis_.xack(stream, group, e.id);
      continue;
    }
    handler(e.id, *body);
    ++n;
  }
  return n;
}

bool StreamConsumer::ack(const std::string &stream, const std::string &group,
                         const std::string &id) {
  return redis_.xack(stream, group, id);
}