#pragma once
#include "EventLoop.h"
#include "rpc_header.pb.h"
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
// 处理函数：接受请求的原始字节，返回响应的原始字节
// 返回值是序列化后的 protobuf response body

using RpcMethodHandler =
    std::function<std::string(const std::string &request_body)>;

using RpcMethodHandlerWithConn =
    std::function<std::string(spConnection, const std::string &)>;

// 新增：带完整 header 的 handler（IM 节点用 gateway_id/conn_id 推导身份）
using RpcMethodHandlerWithContext = std::function<std::string(
    spConnection, const std::string &, const RpcHeader &)>;

// 结果型 handler 的返回值：error_code=0 表示成功（幂等去重时缓存响应体）；
// 非 0 表示业务失败（不缓存，客户端可重试重执行）。
struct RpcMethodResult {
  int32_t error_code = 0;
  std::string body;
};

using RpcMethodHandlerResult =
    std::function<RpcMethodResult(const std::string &request_body)>;

using RpcMethodHandlerWithConnResult =
    std::function<RpcMethodResult(spConnection, const std::string &)>;

using RpcMethodHandlerWithContextResult = std::function<RpcMethodResult(
    spConnection, const std::string &, const RpcHeader &)>;
class ServiceManager {
public:
  void registerMethod(const std::string &service_name,
                      const std::string &method_name,
                      RpcMethodHandler handler) {
    std::string key = service_name + "/" + method_name;
    methods_[key] = std::move(handler);
  }
  void registerMethod(const std::string &service_name,
                      const std::string &method_name,
                      RpcMethodHandlerWithConn handler) {
    std::string key = service_name + "/" + method_name;
    methodsWithConn_[key] = std::move(handler);
  }
  void registerMethod(const std::string &service_name,
                      const std::string &method_name,
                      RpcMethodHandlerWithContext handler) {
    std::string key = service_name + "/" + method_name;
    methodsWithContext_[key] = std::move(handler);
  }
  RpcMethodHandler findMethod(const std::string &service_name,
                              const std::string &method_name) const {
    std::string key = service_name + "/" + method_name;
    auto it = methods_.find(key);
    if (it != methods_.end()) {
      return it->second;
    }
    return nullptr;
  }
  RpcMethodHandlerWithConn
  findMethodWithConn(const std::string &service_name,
                     const std::string &method_name) const {
    std::string key = service_name + "/" + method_name;
    auto it = methodsWithConn_.find(key);
    if (it != methodsWithConn_.end()) {
      return it->second;
    }
    return nullptr;
  }
  RpcMethodHandlerWithContext
  findMethodWithContext(const std::string &service_name,
                        const std::string &method_name) const {
    std::string key = service_name + "/" + method_name;
    auto it = methodsWithContext_.find(key);
    if (it != methodsWithContext_.end()) {
      return it->second;
    }
    return nullptr;
  }

  // —— 结果型 handler（带 error_code，供「失败可重试」的非幂等操作使用） ——
  void registerMethodWithResult(const std::string &service_name,
                                const std::string &method_name,
                                RpcMethodHandlerResult handler) {
    std::string key = service_name + "/" + method_name;
    methodsResult_[key] = std::move(handler);
  }
  void registerMethodWithResult(const std::string &service_name,
                                const std::string &method_name,
                                RpcMethodHandlerWithConnResult handler) {
    std::string key = service_name + "/" + method_name;
    methodsWithConnResult_[key] = std::move(handler);
  }
  void registerMethodWithResult(const std::string &service_name,
                                const std::string &method_name,
                                RpcMethodHandlerWithContextResult handler) {
    std::string key = service_name + "/" + method_name;
    methodsWithContextResult_[key] = std::move(handler);
  }

  RpcMethodHandlerResult
  findMethodResult(const std::string &service_name,
                   const std::string &method_name) const {
    std::string key = service_name + "/" + method_name;
    auto it = methodsResult_.find(key);
    if (it != methodsResult_.end()) {
      return it->second;
    }
    return nullptr;
  }
  RpcMethodHandlerWithConnResult
  findMethodWithConnResult(const std::string &service_name,
                           const std::string &method_name) const {
    std::string key = service_name + "/" + method_name;
    auto it = methodsWithConnResult_.find(key);
    if (it != methodsWithConnResult_.end()) {
      return it->second;
    }
    return nullptr;
  }
  RpcMethodHandlerWithContextResult
  findMethodWithContextResult(const std::string &service_name,
                              const std::string &method_name) const {
    std::string key = service_name + "/" + method_name;
    auto it = methodsWithContextResult_.find(key);
    if (it != methodsWithContextResult_.end()) {
      return it->second;
    }
    return nullptr;
  }

private:
  std::unordered_map<std::string, RpcMethodHandler> methods_;
  std::unordered_map<std::string, RpcMethodHandlerWithConn> methodsWithConn_;
  std::unordered_map<std::string, RpcMethodHandlerWithContext>
      methodsWithContext_;
  std::unordered_map<std::string, RpcMethodHandlerResult> methodsResult_;
  std::unordered_map<std::string, RpcMethodHandlerWithConnResult>
      methodsWithConnResult_;
  std::unordered_map<std::string, RpcMethodHandlerWithContextResult>
      methodsWithContextResult_;
};