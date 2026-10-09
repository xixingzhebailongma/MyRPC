# 容器化（最小可用）

一条命令拉起 IM 系统的基础设施 + route；`--profile full` 拉起全部五个服务。

## 启动

```bash
docker compose up -d --build          # 默认 profile：etcd/redis/mysql/route
docker compose ps                     # 等 route healthy
./scripts/smoke_test.sh               # smoke：验证基础设施 + route 注册进 etcd
docker compose down -v                # 干净清理（含数据卷）

# 完整五个服务（含 auth，见下方 Known Issues）
docker compose --profile full up -d --build
```

**smoke 覆盖范围**：`./scripts/smoke_test.sh` 验证默认 profile 的基础设施（etcd/redis/mysql）
与 route 都能 healthy，且 route 真实注册进 etcd（`/myrpc/services/RouteService/`）。**不覆盖**
auth/im/gateway/deliver 的消息链路（它们需 full profile，而 auth 有 known issue）。

## Known Issues

### etcd-cpp-apiv3 版本分叉（阻塞 full profile 的 auth）

- **根因**：源码版本分叉——dev 能跑的 .so 是 vendored `v0.15.4-10-g7c6e714`，而
  `install_deps.sh`/CI 钉的是 GitHub `v0.15.3`（+ watch-cancel 补丁）。ABI 对比无差异
  （同为 gcc 11.4 + C++11 ABI），所以不是 ABI，是版本。
- **现象**：fresh 构建 + full profile 起 auth 时，auth 启动即 SIGSEGV（exit 139）。
- **影响范围**：仅 full profile 的 auth（及依赖它的 im/gateway/deliver）；默认 profile
  （infra+route）不受影响——route 走 etcd 基础 put 路径，不触发崩溃。
- **现状**：CI 的 `etcd-fresh-build` job（`continue-on-error`）构建 full 栈并复现，作为
  known issue 跟踪，不阻塞合并。
- **未做（决策记录）**：不把 dev 的 6MB .so 提交进仓库做止血——进 git 历史后清理成本高，
  且属于伪绿（只解决"能跑"不解决"版本一致"）。留待长期对齐。
- **长期方案**：对齐 etcd-cpp-apiv3 源码版本（让 CI/容器照抄 vendored 7c6e714 的构建），或
  统一 vendored 策略。

## 端口

| 服务 | RPC/客户端端口 | metrics 端口 | 对外暴露 |
|---|---|---|---|
| route | 8889 | 9090 | 否 |
| auth | 9101 | 9091 | 否 |
| im | 9001 | 9092 | 否 |
| gateway | client 9000 · rpc 9100 | 9093 | **9000** |
| deliver | 无监听（worker） | 9094 | 否 |

服务间经 etcd 互相发现，不需要互传地址；配置经 CLI flag（`command:`）注入，复用各 `*_main.cc` 的解析。关键 flag：

- `--etcd.endpoints=http://etcd:2379` `--redis.ip=redis` `--mysql.host=mysql`
- gateway / im 共用 `--shared.secret=devsecret123`（HMAC 身份签名，两侧必须一致）

## 镜像

统一 `docker/Dockerfile` + `--build-arg SERVICE_NAME=<route_server|auth_server|im_server|gateway_server|deliver_server|im_test_client>`。

- **build 阶段**：`install_deps.sh` 装依赖（版本钉死）+ 源码编译六个二进制；该层在六个镜像间共享缓存（一次编译，六份镜像只多各自 `COPY`）。
- **runtime 阶段**：只装运行时库（`libgrpc++1 libcpprest2.10 libprotobuf23 libhiredis0.14 libmysqlclient21 libmysqlcppconn7v5` 等）+ 拷二进制 + 源码构建的 `libetcd-cpp-api.so` + 非 root 用户。

优化前后（预估，以 CI 实测为准）：

| | 镜像内容 | 体积量级 |
|---|---|---|
| 优化前（单阶段，装 -dev） | build-essential + 全部 -dev 头文件 + 源码 | ~1GB+ |
| 优化后（多阶段，只运行时库） | 运行时 .so + 二进制 | ~250MB |

> 体积下限由动态依赖决定（grpc + abseil + cpprest + protobuf 本身就重），要再小需静态链接或换传输层，属超出"最小可用"的优化。

## CI

`.github/workflows/ci.yml` 里三个 job 的定位：

| job | 定位 |
|---|---|
| `container` | 默认 profile 的 docker build + smoke（infra+route），**阻断** |
| `etcd-fresh-build` | full profile 构建 + 起 auth 复现 SIGSEGV，`continue-on-error`，**跟踪不阻断** |
| `test` | ASan/UBSan 矩阵 + 单元/E2E 测试（与容器化无关） |

未做 GHCR push（可选，后续加 `docker buildx` + `ghcr.io` 登录即可）。

## 常见排错

- **etcd healthcheck 失败 / 服务连不上 etcd**：确认 compose 里 etcd 用 exec 形式 healthcheck（`["CMD","etcdctl",...]`），distroless 镜像不能跑 shell 形式。
- **redis 报 `GETDEL`/`XAUTOCLAIM` 不支持**：镜像必须是 `redis:7.x`（6.x 缺这两个命令）。
- **服务连不上 redis/mysql**：它们默认连 `127.0.0.1`，容器内必须用 `--redis.ip=redis` / `--mysql.host=mysql` 覆盖成服务名。
- **auth/im 起不来 / mysql 认证失败**：mysql 是 `MYSQL_ALLOW_EMPTY_PASSWORD=yes`，root 空密码；schema 靠挂载 `schema.sql` 到 initdb 自动建表。
- **send 返回失败**：确认 gateway 和 im 的 `--shared.secret` 一致。
