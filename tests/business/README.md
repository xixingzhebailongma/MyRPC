# IM 业务端到端测试

两个测试都 fork 起真实的 route/auth/im/gateway/deliver 五个服务进程，用 `ImClientConn` 驱动完整业务流：

- **`test_im_business.cc`** —— 完整业务场景（7 组 / 35 项断言）：
  认证边界（重复注册/错误密码）、消息可靠性（幂等去重 / 多端扇出 / 离线补推）、
  多端会话（ListSessions / KickSession / KickAllSessions / 同端互踢）、
  账号生命周期（改密 / 续期 / 登出）、好友边界 + 在线状态、未认证请求拒绝。
- **`test_im_e2e.cc`** —— happy-path 冒烟（注册→登录→加好友→发消息→ACK→离线→重连）。

## 前置

etcd(`:2379`) + Redis **7.x**(`:6379`) + MySQL(`:3306`，库 `myrpc_im`) 已就绪。
**不要**先跑 `scripts/start_im_services.sh`（会抢 etcd 里同名的 `ImService`）。

## 一键运行

```bash
./tests/business/run.sh                    # 跑 test_im_business
E2E=1 ./tests/business/run.sh              # 顺带跑 test_im_e2e
KEEP_BIZ_LOGS=1 ./tests/business/run.sh    # 保留 /tmp/myrpc_biz_* 日志排查
```

或手动：

```bash
cmake --build build --target test_im_business -j
./build/tests/test_im_business
```
