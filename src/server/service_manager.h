#pragma once
#include "EventLoop.h"
#include <functional>
#include <string>
#include <unordered_map>

// 处理函数：接受请求的原始字节，返回响应的原始字节
// 返回值是序列化后的 protobuf response body

using RpcMethodHandler =
    std::function<std::string(const std::string &request_body)>;

using RpcMethodHandlerWithConn =
    std::function<std::string(spConnection, const std::string &)>;
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

private:
  std::unordered_map<std::string, RpcMethodHandler> methods_;
  std::unordered_map<std::string, RpcMethodHandlerWithConn> methodsWithConn_;
};