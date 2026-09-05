#pragma once
#include "rpc_header.pb.h"
#include <cstdint>
#include <string>

// ===== 帧格式约定 =====
// 网络帧 = [4 字节 LE uint32 长度][protobuf 序列化后的 RpcMessage]
// 长度上限 64MB，超过视为非法帧。
constexpr uint32_t kHeaderLen = 4;
constexpr uint32_t kMaxMessageLen = 64u << 20; // 64MB，超过视为非法帧

// ===== 编解码（成对、对称）=====

// 编码：RpcMessage -> 完整帧（序列化 + 加 4 字节长度头）
std::string encodeMessage(const RpcMessage &msg);

// 流式解帧：从连续字节流剥出一帧的 body（剥头 + 长度校验），
// 与 encodeMessage 的「加头」成对。供哑管道（muduo）逐帧消费。
enum class FrameDecode { kNeedMore, kOk, kError };
// kOk：*frame_len = 4 + body_len，*body = 剥头后的 payload；
// kNeedMore：数据不完整，继续等；kError：长度非法（0 或 >64MB），应断开连接。
FrameDecode tryDecodeFrame(const char *data, size_t len, size_t *frame_len,
                           std::string *body);

// 解码：对「已剥头」的 body 做 protobuf 反序列化。
// 注意：入参是 body（不含 4 字节长度头），不是完整帧。
bool decodeMessage(const std::string &body, RpcMessage &msg);

// 构造一个请求 RpcMessage
RpcMessage buildRequest(const std::string &service_name,
                        const std::string &method_name, uint64_t sequence_id,
                        const std::string &body);
// 构造一个响应 RpcMessage
RpcMessage buildResponse(uint64_t sequence_id, int32_t error_code,
                         const std::string &body);

// 构造一帧心跳 ping（客户端 readLoop 空闲探测）
RpcMessage buildHeartbeat(uint64_t sequence_id);
// 构造一帧心跳 pong（服务端应答）
RpcMessage buildHeartbeatAck(uint64_t sequence_id);