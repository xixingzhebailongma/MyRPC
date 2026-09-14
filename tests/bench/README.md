# 压测 / 基准

## 吞吐 & 延迟（AsyncRpcClient）

```bash
cmake --build build --target bench_async_rpc
./build/tests/bench/bench_async_rpc --servers=4 --duration=5 --threads=4 --concurrency=16 --payload=64
```

| 参数 | 含义 |
|---|---|
| `--servers` | 回显服务节点数（= 客户端并发连接数） |
| `--duration` | 压测时长（秒） |
| `--threads` | AsyncRpcClient 事件循环线程数（0 = 自动） |
| `--concurrency` | 并发打流的 worker 线程数 |
| `--payload` | echo 请求体字节数 |

输出：总请求、QPS、avg / p50 / p90 / p99 延迟。

## 实测数据

环境：WSL2（loopback 127.0.0.1），`duration=3s payload=64B`。绝对数值随机器/环境变化，仅作量级与相对趋势参考。

| 配置 | QPS | avg | p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|
| 单并发 `--servers=4 --concurrency=1 --threads=4` | 5720 | 174µs | 163µs | 250µs | 354µs | 1084µs |
| 8 并发 `--servers=4 --concurrency=8 --threads=4` | 26107 | 306µs | 287µs | 420µs | 619µs | 2450µs |
| 32 并发 `--servers=8 --concurrency=32 --threads=8` | 37141 | 860µs | 792µs | 1398µs | 2170µs | 4924µs |

> 单并发档测的是纯延迟下限（此时 QPS × avg ≈ 1s）；加并发后 QPS 上升、延迟也上升，符合 Little's Law（并发 = QPS × 平均延迟）。

## 连接抖动下的行为（功能性，非基准）

心跳淘汰 / 连接池回收 / 熔断恢复已由功能测试覆盖，量化这些时机的
延迟与恢复时间可直接跑对应 CTest：

```bash
ctest -R 'test_rpc_channel_heartbeat|test_rpc_channel_pool_reap|test_rpc_channel_circuit_breaker'
```
