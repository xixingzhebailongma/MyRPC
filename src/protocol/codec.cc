#include "rpc_header.pb.h"
#include "rpc_protocol.h"
#include <cstdint>
#include <cstring>

std::string encodeMessage(const RpcMessage &msg) {
  size_t body_size = msg.ByteSizeLong(); // 先算长度，只算不写
  uint32_t len = static_cast<uint32_t>(body_size);
  std::string result;
  result.resize(kHeaderLen + body_size); // 一次分配，含 4 字节长度头
  writeLenBE(result.data(), len);        // 写入大端长度前缀
  if (body_size > 0) {
    // 直接序列化进 result 的 body 区，不再产生中间串
    msg.SerializeToArray(&result[kHeaderLen], static_cast<int>(body_size));
  }
  return result;
}
FrameDecode tryDecodeFrame(const char *data, size_t len, size_t *frame_len,
                           std::string *body) {
  if (len < kHeaderLen)
    return FrameDecode::kNeedMore;
  uint32_t body_len = readLenBE(data); // 读 4 字节 BE 长度
  if (body_len == 0 || body_len > kMaxMessageLen)
    return FrameDecode::kError; // 非法长度，防坏帧触发巨量分配
  if (len < kHeaderLen + body_len)
    return FrameDecode::kNeedMore;           // 数据不完整，继续等
  body->assign(data + kHeaderLen, body_len); // 只拷贝 payload
  *frame_len = kHeaderLen + body_len;
  return FrameDecode::kOk;
}
bool decodeMessage(const std::string &wire_data, RpcMessage &msg) {
  return msg.ParseFromString(wire_data);
}

RpcMessage buildRequest(const std::string &service_name,
                        const std::string &method_name, uint64_t sequence_id,
                        const std::string &body) {
  RpcMessage msg;
  auto *header = msg.mutable_header();
  header->set_service_name(service_name);
  header->set_method_name(method_name);
  header->set_sequence_id(sequence_id);
  header->set_error_code(0);
  msg.set_body(body);
  return msg;
}

RpcMessage buildResponse(uint64_t sequence_id, int32_t error_code,
                         const std::string &body) {
  RpcMessage msg;
  auto *header = msg.mutable_header();
  header->set_sequence_id(sequence_id);
  header->set_error_code(error_code);
  msg.set_body(body);
  return msg;
}

RpcMessage buildHeartbeat(uint64_t sequence_id) {
  RpcMessage msg;
  auto *h = msg.mutable_header();
  h->set_sequence_id(sequence_id);
  h->set_type(MessageType::MSG_HEARTBEAT);
  return msg;
}

RpcMessage buildHeartbeatAck(uint64_t sequence_id) {
  RpcMessage msg;
  auto *h = msg.mutable_header();
  h->set_sequence_id(sequence_id);
  h->set_type(MessageType::MSG_HEARTBEAT_ACK);
  return msg;
}