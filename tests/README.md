# 测试套件

本目录统一组织 MyRPC 的全部测试，按类别分到子目录，每个子目录带 README + 一键 `run.sh`。

| 子目录 | 内容 | 外部依赖 | 一键运行 |
|---|---|---|---|
| `unit/` | 框架/客户端单元测试（RPC 通道、负载均衡、幂等、TLS、连接原语…） | 少数需 etcd | `./tests/unit/run.sh` |
| `business/` | IM 业务端到端（happy-path + 完整业务场景） | etcd + Redis 7 + MySQL | `./tests/business/run.sh` |
| `multi-node/` | IM 多节点一致性（2 个 IM 节点 + 去 pin） | etcd + Redis 7 + MySQL | `./tests/multi-node/run.sh` |
| `bench/` | AsyncRpcClient 吞吐/延迟压测 | 无 | `./tests/bench/run.sh` |

一键跑全部：`./tests/run_all.sh`。

## 前置依赖

- 构建：CMake 3.16+、protobuf、hiredis、mysqlclient、mysqlcppconn、OpenSSL。
- 业务 / 多节点测试额外需要：etcd(`:2379`)、Redis **7.x**(`:6379`)、MySQL(`:3306`，库 `myrpc_im`)。
  这些测试内部会自己 fork 起 route/auth/im/gateway/deliver 五个服务进程，
  **不要**先跑 `scripts/start_im_services.sh` —— 它会往 etcd 注册同名的 `ImService`，与测试冲突。

## 构建产物

- 单元 / 业务 / 多节点测试二进制 → `build/tests/`
- 压测二进制 → `build/tests/bench/`

## 全量 CTest

```bash
cmake -B build && cmake --build build -j
ctest --test-dir build --output-on-failure
```

三个 IM E2E 测试（`test_im_e2e` / `test_im_business` / `test_im_multi_node`）共用固定服务名
`ImService`，已在 CMake 里用 `RUN_SERIAL` 标记串行，避免并行互抢 etcd 同名节点。
