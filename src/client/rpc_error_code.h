#pragma once
#include <cstdint>

// RPC 框架错误码（网络/传输层），与业务错误码隔离。
// 约定：框架错误码落在 [10000, 19999] 区间；业务错误码保留 0/-1 及 0~9999。
// 成功路径的 error_code 仍取自响应头（resp.header().error_code()），是业务码；
// 失败路径由 RpcChannel / LbRpcClient 写入本枚举对应值。
enum class RpcError : int32_t {
  OK = 0,
  TIMEOUT = 10001,            // 等待响应超时
  CONNECTION_REFUSED = 10002, // 建连失败
  CONNECTION_BROKEN = 10003,  // 发送失败 / 连接中途断开
  CIRCUIT_OPEN = 10004,       // 熔断快速失败
  SERVER_ERROR = 10005,       // 响应缺失 / 解析失败
  NO_AVAILABLE_NODE = 10006,  // 无可用节点 / 全部熔断
  SERVER_OVERLOADED =
      10007, // 服务端工作队列满、拒绝请求（临时性错误，客户端应 failover 重试）
  INVALID_REQUEST_ID = 10008, // 服务端拒绝非法 request_id（幂等键格式校验失败）
  UNKNOWN = 10999,
};

// 错误码 -> 可读字符串（日志用）
inline const char *rpcErrorName(RpcError e) {
  switch (e) {
  case RpcError::OK:
    return "OK";
  case RpcError::TIMEOUT:
    return "TIMEOUT";
  case RpcError::CONNECTION_REFUSED:
    return "CONNECTION_REFUSED";
  case RpcError::CONNECTION_BROKEN:
    return "CONNECTION_BROKEN";
  case RpcError::CIRCUIT_OPEN:
    return "CIRCUIT_OPEN";
  case RpcError::SERVER_ERROR:
    return "SERVER_ERROR";
  case RpcError::NO_AVAILABLE_NODE:
    return "NO_AVAILABLE_NODE";
  case RpcError::SERVER_OVERLOADED:
    return "SERVER_OVERLOADED";
  case RpcError::INVALID_REQUEST_ID:
    return "INVALID_REQUEST_ID";
  case RpcError::UNKNOWN:
    return "UNKNOWN";
  }
  return "UNKNOWN";
}

// 可重试业务错误码区间：业务错误码保留在 [0, 9999]，
// 其中 [1000, 1999] 约定为「临时性/可重试」业务失败（框架据此 failover 重试）。
constexpr int32_t kRetryableBusinessErrorMin = 1000;
constexpr int32_t kRetryableBusinessErrorMax = 1999;

// 是否为临时性错误（可 failover 重试）。
// 运输层：TIMEOUT / CONNECTION_REFUSED / CONNECTION_BROKEN / CIRCUIT_OPEN；
// 业务层：落在可重试业务区间 [kRetryableBusinessErrorMin,
// kRetryableBusinessErrorMax]。
inline bool isRetryable(int32_t code) {
  switch (code) {
  case static_cast<int32_t>(RpcError::TIMEOUT):
  case static_cast<int32_t>(RpcError::CONNECTION_REFUSED):
  case static_cast<int32_t>(RpcError::CONNECTION_BROKEN):
  case static_cast<int32_t>(RpcError::CIRCUIT_OPEN):
  case static_cast<int32_t>(RpcError::SERVER_OVERLOADED):
    return true;
  default:
    return code >= kRetryableBusinessErrorMin &&
           code <= kRetryableBusinessErrorMax;
  }
}