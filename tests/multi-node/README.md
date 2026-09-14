# IM 多节点一致性测试

验证无状态化改造的核心命题「任意 IM 节点都能服务任意请求」：起 **2 个 IM 节点**
（同 `ImService` 服务名、不同 worker_id/端口）+ 1 个 gateway + route/auth/deliver。

覆盖：

- **路由落共享存储**：直连 Redis 校验 `im:route:{uid}` 有 field，且每条 field 的
  反查索引 `im:conn:{gw}:{conn}` 都指回 uid（而非节点本地内存）。
- **跨节点收发**：发消息节点与投递解耦，投递查共享路由。
- **跨节点多端扇出 / 离线补推**。
- **跨节点单设备踢**：发起节点与目标连接注册节点可不同。
- **改密踢人**（跨节点吊销所有会话）。
- **去 pin 生效**：解出各消息雪花 msg_id 的 worker_id，得到 {1,2}，证明消息被
  轮询到了两个不同节点处理。

## 前置

同业务测试（etcd + Redis 7 + MySQL）。

## 一键运行

```bash
./tests/multi-node/run.sh
```

或手动：

```bash
cmake --build build --target test_im_multi_node -j
./build/tests/test_im_multi_node
```
