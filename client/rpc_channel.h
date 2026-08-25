#pragma once
#include "rpc_protocol.h"
#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
class RpcChannel {
public:
  // timeout_ms: 单次 RPC 调用的超时时间(毫秒)，默认 3000ms
  RpcChannel(const std::string &server_ip, uint16_t server_port,
             int timeout_ms = 3000);
  ~RpcChannel();
  // 同步 RPC 调用，阻塞直到收到响应
  // 返回 true 表示成功，response_body 和 error_code 是返回值
  bool Call(const std::string &service_name, const std::string &method_name,
            const std::string &request_body, std::string &response_body,
            int32_t &error_code);

  //连接到服务器(首次Call时自动调用)
  bool connect();
  void close();
  bool isConnected() const { return sockfd_ >= 0; }

  //动态修改超时时间（毫秒）
  void setTimeout(int timeout_ms) { timeout_ms_ = timeout_ms; }

private:
  //明它发送的是「已经带长度前缀的帧」，只负责写满字节、不再加前缀。
  bool sendFrame(const std::string &data);
  bool recvFrame(std::string &data);
 // 无锁关闭 socket。前提：调用前必须已持有 call_mutex_。
    // （Call() 失败路径在持锁时调用它，避免与 public close() 自死锁。）
    void closeLocked();
    
  std::string server_ip_;
  uint16_t server_port_;
  int sockfd_;                        //-1表示未连接
  int timeout_ms_;                    //超时时间（毫秒）
  std::atomic<uint64_t> next_seq_id_; //请求序列号，每次调用自增
  std::mutex call_mutex_;             //单 channel 并发 Call 串行化
};