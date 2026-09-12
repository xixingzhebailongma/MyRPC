# 压测 / 基准

## 吞吐 & 延迟（AsyncRpcClient）

```bash
cmake --build build --target bench_async_rpc
./build/bench/bench_async_rpc --servers=4 --duration=5 --threads=4 --concurrency=16 --payload=64
```

| 参数 | 含义 |
|---|---|
| `--servers` | 回显服务节点数（= 客户端并发连接数） |
| `--duration` | 压测时长（秒） |
| `--threads` | AsyncRpcClient 事件循环线程数（0 = 自动） |
| `--concurrency` | 并发打流的 worker 线程数 |
| `--payload` | echo 请求体字节数 |

输出：总请求、QPS、avg / p50 / p90 / p99 延迟。

## 连接抖动下的行为（功能性，非基准）

心跳淘汰 / 连接池回收 / 熔断恢复已由功能测试覆盖，量化这些时机的
延迟与恢复时间可直接跑对应 CTest：

```bash
ctest -R 'test_rpc_channel_heartbeat|test_rpc_channel_pool_reap|test_rpc_channel_circuit_breaker'
```
