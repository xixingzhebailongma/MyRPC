#pragma once
#include <cstdint>

// RPC 客户端配置：把散落在 RpcChannel / RpcChannelPool / LbRpcClient 里的
// 重试、超时、心跳、熔断等参数集中到一处，通过命令行注入。
// 默认值与改造前的硬编码完全一致，因此不带参数时运行时行为不变。
struct RpcClientConfig {
  int max_retries = 3;                  // LbRpcClient 默认重试次数
  int timeout_ms = 3000;                // 单次调用响应超时
  int connect_timeout_ms = 3000;        // 建连超时
  int heartbeat_interval_ms = 5000;     // 空闲多久发一次心跳
  int heartbeat_miss_threshold = 3;     // 连续 miss 多少次判死
  int circuit_failure_threshold = 3;    // 连续失败多少次进入熔断
  int circuit_backoff_base_ms = 1000;   // 熔断退避基数
  int circuit_backoff_max_ms = 30000;   // 熔断退避上限
  uint64_t channel_idle_ttl_ms = 60000; // channel 空闲回收阈值
  uint64_t channel_reap_interval_ms = 10000; // channel 回收扫描周期
  int channel_reconnect_delay_ms = 3000; // 断线后重连延迟（与原 TcpClient 3s 一致）
};