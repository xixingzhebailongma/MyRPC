#pragma once
#include "rpc_header.pb.h"
#include <string>
#include <cstdint>
// 把 RpcMessage 编码成网络格式: [4字节LE长度][序列化后的proto数据]
std::string encodeMessage(const RpcMessage& msg);
 // 从网络数据（已去掉4字节长度前缀）解码成 RpcMessage
bool decodeMessage(const std::string & wire_data,RpcMessage& msg);
 // 构造一个请求 RpcMessage
RpcMessage buildRequest(const std::string & service_name,const std::string& method_name,
uint64_t sequence_id,const std::string& body);
  // 构造一个响应 RpcMessage
RpcMessage buildResponse(uint64_t sequence_id,int32_t error_code,const std::string&body);