# 框架 / 单元测试

RPC 框架各组件与客户端连接原语的单元 / 集成测试，多数无外部依赖：

- RPC 通道：`frame` / `concurrency` / `heartbeat` / `metrics` / `pool_reap` / `circuit_breaker`
- 服务端：`rpc_server_e2e` / `rpc_server_concurrency` / `rpc_server_idempotency`
- 客户端：`async_rpc_client` / `async_lb_rpc_client` / `rpc_lb_failover_etcd`
- 幂等：`idempotency_lru` / `idempotency_redis`
- 基础：`queue` / `buffer` / `threadpool` / `load_balancer` / `eventloop_functor_reentry`
- 连接原语：`im_conn_concurrency` / `gateway_tls`（spawn 真实 gateway 测 TLS 握手）

> 少数测试需要 etcd 在跑：`test_rpc_lb_failover_etcd`、`test_async_lb_rpc_client`、
> `test_gateway_tls`（spawn gateway_server，需要 etcd 发现）。

## 一键运行

```bash
./tests/unit/run.sh
```

等价于：

```bash
cmake --build build -j
ctest --test-dir build -E 'test_im_e2e|test_im_business|test_im_multi_node'
```
