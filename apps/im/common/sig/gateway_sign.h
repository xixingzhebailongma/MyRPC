#pragma once

#include "rpc_header.pb.h"
#include <string>
// Gateway 与 IM 之间共享的身份签名：Gateway 用共享密钥对身份字段做
// HMAC-SHA256，IM 用同一密钥验签。两侧必须用同一份 buildCanonical 保证
// 签名字符串一致。
namespace gateway_sign {

// 计算 HMAC-SHA256，返回小写 hex。
std::string hmacSha256Hex(const std::string &secret, const std::string &data);

// 由 header 的字段拼出规范化的待签名串（字段顺序必须两侧一致）。
std::string buildCanonical(const RpcHeader &hdr);

// 给 header 计算签名（返回 hex，调用方自行 set_signature）。
std::string sign(const std::string &secret, const RpcHeader &hdr);

// 验签：重算 HMAC 后常量时间比较。
bool verify(const std::string &secret, const RpcHeader &hdr);

} // namespace gateway_sign