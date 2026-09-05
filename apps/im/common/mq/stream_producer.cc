#include "stream_producer.h"

namespace {
// Stream 里承载消息体的字段名，必须与 stream_consumer.cc 保持一致
constexpr const char *kBodyField = "body";
} // namespace

bool StreamProducer::connect(const std::string &ip, int port) {
  return redis_.connect(ip, port);
}

bool StreamProducer::produce(const std::string &stream,
                             const std::string &payload, std::string *out_id) {
  std::string id = redis_.xadd(stream, kBodyField, payload);
  if (id.empty()) {
    return false;
  }
  if (out_id) {
    *out_id = id;
  }
  return true;
}