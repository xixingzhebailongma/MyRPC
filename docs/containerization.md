# 容器化（最小可用）

一条命令拉起整个 IM 系统：5 个 C++ 服务 + etcd / Redis 7 / MySQL，跑通真实链路。

## 启动

```bash
docker compose up -d --build          # 构建镜像 + 后台启动
docker compose ps                     # 等 5 个服务全部 healthy
./scripts/smoke_test.sh               # 端到端 smoke（register/login/send）
docker compose down -v                # 干净清理（含数据卷）
```

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

`.github/workflows/ci.yml` 的 `container` job 跑 `scripts/smoke_test.sh`（构建镜像 + 端到端验证）。未做 GHCR push（可选，后续加 `docker buildx` + `ghcr.io` 登录即可）。

## 常见排错

- **etcd healthcheck 失败 / 服务连不上 etcd**：确认 compose 里 etcd 用 exec 形式 healthcheck（`["CMD","etcdctl",...]`），distroless 镜像不能跑 shell 形式。
- **redis 报 `GETDEL`/`XAUTOCLAIM` 不支持**：镜像必须是 `redis:7.x`（6.x 缺这两个命令）。
- **服务连不上 redis/mysql**：它们默认连 `127.0.0.1`，容器内必须用 `--redis.ip=redis` / `--mysql.host=mysql` 覆盖成服务名。
- **auth/im 起不来 / mysql 认证失败**：mysql 是 `MYSQL_ALLOW_EMPTY_PASSWORD=yes`，root 空密码；schema 靠挂载 `schema.sql` 到 initdb 自动建表。
- **send 返回失败**：确认 gateway 和 im 的 `--shared.secret` 一致。
