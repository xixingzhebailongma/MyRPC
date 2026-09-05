# 引入 RocketMQ：IM 消息投递与事件广播上 broker

## Context

项目定位从「学习/演示 RPC 框架」转向「生产级演进」。当前 IM 数据面存在两个明确的瓶颈，且都集中在投递链路：

1. **同步扇出阻塞 WORK 线程**：`handleSendMessage`（`apps/im/services/im/im_server.cc:385`）在 RpcServer 的 8 线程 work pool 上同步做完「去重 → 路由查询 → 逐收件人 `pushToGateway` 阻塞 RPC（默认 3s 超时）→ 状态写入」。只要有慢/离线收件人，整个节点的消息处理就被拖住，`resp` 直到扇出全部结束才返回给客户端。
2. **手写 pending 重试队列脆弱**：`onRetryCheck`（`im_server.cc:759`）+ 单线程 `retry_thread_`（`im_server.cc:154`）基于 Redis ZSET（`msg:pending:{server_id}`）+ 每记录一次 GET（N+1 RTT），本质是一个「延迟 + 持久重投」队列，但单线程串行、与线上发送争抢同一个 4 连接 Redis 池。

结论：引入 RocketMQ（用户已确认「生产级 + 独立 broker + 全量上 broker」），把**投递扇出、重试、事件广播**从同步 RPC 与手写 Redis 队列里解耦出来。

## 目标架构

RocketMQ 5.x + gRPC Proxy（用 `apache/rocketmq-clients` 的 C++ 绑定，命名空间 `rmq`，类 `rmq::Producer` / `rmq::PushConsumer`）。发送路径变为：

```
客户端 → Gateway → IM(仅: 鉴权 + 去重 + 生成msg_id + 落库)
                    │ produce（异步，按 to_user_id 路由到有序队列）
                    ▼
             RocketMQ topic: im-delivery
                    │ consume（PushConsumer，独立线程池）
                    ▼
              投递消费者（先期与 ImServer 同进程）: resolveRoutes → pushToGateway → 未达则 RECONSUME_LATER
```

核心收益：`handleSendMessage` 只做「幂等去重 + 生成 msg_id + 快速落库 + produce」，毫秒级返回；扇出与重试全部移到 broker 之后，由投递消费者用自己的线程池异步完成，WORK 线程不再被慢收件人阻塞；手写 pending ZSET + retry 线程整体删除，重试改用 RocketMQ 的 `RECONSUME_LATER`（内置延迟级别）。

## 一个关键判断：哪些数据留在 Redis

「全量上 broker」我建议按**语义**区分，而不是字面全迁：消息队列适合「单向流动的流」，不适合「按主键随机读写的状态」。

| 数据 | 语义 | 归属 | 理由 |
|---|---|---|---|
| 投递扇出 | 流（发一次，投多次） | **RocketMQ `im-delivery`** | 削峰 + 异步 + 内置重试 |
| 投递重试 | 延迟 + 重投 | **RocketMQ 重试队列（`%RETRY%{group}`）** | 替换 pending ZSET |
| 用户变更事件广播（踢下线） | 广播流 | **RocketMQ `im-event`** | 替换 `im:user:events` Pub/Sub，获得持久/可回放 |
| 离线消息 | 按 user_id 随机读取/整取 | **保留 Redis `msg:offline:{uid}`** | broker 无法「按用户拉取全部未读」，这是存储不是队列 |
| 投递状态 `msg:status:{msg_id}` | 按 msg_id 随机读写（ACK 用） | **保留 Redis** | 消费端重试前要随机查状态，队列语义不符 |
| 幂等去重 `msg:req:{from}:{req_id}` | 按 key SETNX | **保留 Redis** | 同上 |
| 路由表 `im:route:*` | 随机读写 + 缓存 | **保留 Redis/RouteServer** | 已确认 |

> 这点与「全量上 broker」字面略有出入，但离线/状态/去重是**随机访问状态**而非**流**，放进 topic 会导致「按 user_id 拉取」「按 msg_id 查状态」变成全量扫描，明显是反模式。若你坚持离线也上 broker，需要改成「每个用户一个 tag/队列 + 消费组按 tag 过滤」，复杂度高且收益低，我不推荐。**计划按上表执行，离线/状态/去重保留 Redis。**

## Topic 与消费组

| Topic | 类型 | 生产者 | 消费者 | 顺序键 | 说明 |
|---|---|---|---|---|---|
| `im-delivery` | 有序（分区顺序消息） | ImServer（handleSendMessage） | 投递消费者（消费组 `im-delivery-consumers`） | `to_user_id`（单聊可扩展到 conversation key） | 替换 `routeMessage` + pending 重试 |
| `im-event` | 普通（广播） | ImServer / AuthServer（用户数据变更处） | ImServer、AuthServer 各自订阅 | — | 替换 `im:user:events` Pub/Sub；`im:route:events` 可暂留 Redis Pub/Sub（路由缓存允许丢失，靠 TTL 兜底） |

消息体：`im-delivery` 复用现有 `im.proto` 的 `ChatMessage`（payload = 序列化 `ChatMessage`，`msg_id` 已在发送端生成）；`im-event` 复用 `UserChangedEvent`。这样无需新增 proto。

## 实现步骤（分阶段）

### Phase A — 依赖与脚手架
1. 引入 RocketMQ C++ 客户端到 `third_party/`：`apache/rocketmq-clients` 的 C++ 绑定（5.x，gRPC 协议）。构建依赖新增 gRPC/gRPC++ 与 protobuf（项目已有 protobuf）。
2. 新增 `apps/im/common/mq/rocketmq_producer.{h,cc}` 与 `rocketmq_consumer.{h,cc}`（镜像现有 `common/redis/` 的封装风格），暴露：
   - `MqProducer::start(name_server_addr, topic)` / `send(topic, key, body)`（异步 send）
   - `MqConsumer::start(name_server_addr, group, topic, listener)`，listener 返回 `成功/CONSUME_LATER`
3. `apps/im/CMakeLists.txt` 新增 `mq` 静态库（`find_package(gRPC CONFIG REQUIRED)` + `gRPC::grpc++`），并链接进 `im_server`。
4. 配置项（`im_server_main.cc`）：`rocketmq.nameserver`、`rocketmq.topic`、`rocketmq.group`，从命令行/env 读入。

### Phase B — 投递异步化（核心）
1. `handleSendMessage`（`im_server.cc:385`）改造：保留「鉴权 → 去重 → 生成 msg_id → `markStatus(SENT)`」，把 `routeMessage(msg)` 这行替换为 `mq_producer_.send("im-delivery", msg.to_user_id(), payload)`，成功后即返回「sent」；produce 失败则回退到 `storeOfflineMessage`（与现有失败分支一致，保证不丢）。
2. 新增投递消费者（`ImServer` 内，`deliverLocal` 语义搬移）：收到 `ChatMessage` 后，先查 `msg:status:{msg_id}`（复用现有幂等状态）——若已 `DELIVERED/READ` 直接 ack 跳过；否则 `resolveRoutes → pushToGateway`（复用 `im_server.cc:622/653` 现有代码）；成功 ack；失败返回 `RECONSUME_LATER`（内置延迟级别 30s 起，与现有 `RETRY_INTERVAL_MS` 对齐）。
3. 删除 `MessageStore` 的 pending 相关方法（`addPending/fetchDuePending/peekEarliestPending/updatePending/removePending`）与 `im_server.cc:154-190` 的 `retry_thread_`/`onRetryCheck`/`notifyPending`。`message_store.h` 中对应声明一并删掉。
4. 幂等保障：RocketMQ 是 at-least-once，消费端靠 `msg:status` 去重（与现 `onRetryCheck` 先查状态再重试的逻辑一致）；生产端靠现有 `tryClaimRequest` 幂等。

### Phase C — 事件广播迁移
1. `UserChangedEvent` 发布处（`im_server.cc:55-77` 的 DAO publisher、`auth_server.cc` 相关）改发 `MqProducer::send("im-event", ...)`。
2. ImServer/AuthServer 各起一个 `im-event` 消费者，收到后走现有 `onUserChanged` → `kickOffline`（`im_server.cc:706/714`），删除 `user_subscriber_`（`im_server.cc:145`）。
3. `im:route:events` Pub/Sub 暂留（路由缓存允许丢失，TTL 兜底），后续可选迁移。

### Phase D —（可选）抽离独立 `deliver_server`
当前投递消费者与 ImServer 同进程（复用 `route_cache_` + `server_channels_`），解耦只到「broker 缓冲」这一层。若需投递与业务节点独立扩缩容，再把投递消费者拆成独立进程 `apps/im/services/deliver/`，自带 route 缓存与到 Gateway 的 channel pool。作为后续演进项，不在本阶段做。

## 需要新建 / 修改的文件

**新建**
- `apps/im/common/mq/rocketmq_producer.{h,cc}`
- `apps/im/common/mq/rocketmq_consumer.{h,cc}`
- `third_party/rocketmq-clients/`（或作为外部依赖由 CMake `FetchContent`/`find_package` 引入）

**修改**
- `apps/im/CMakeLists.txt`：新增 `mq` 库 + gRPC 依赖，链接进 `im_server`
- `apps/im/services/im/im_server.cc`：改造 `handleSendMessage`、删除重试线程/`onRetryCheck`、新增投递消费者与 `im-event` 消费者
- `apps/im/services/im/im_server.h`：成员 `MqProducer`/`MqConsumer`，删除 `retry_thread_`/`retry_cv_`/`retry_epoch_`
- `apps/im/services/im/im_server_main.cc`：读 RocketMQ 配置并启动 producer/consumer
- `apps/im/services/im/message_store.{h,cc}`：删除 pending 方法（离线/状态/去重保留）
- `apps/im/services/auth/auth_server.cc`：`im-event` 发布/订阅替换 `user_subscriber_`

## 关键技术点

- **顺序消息**：`im-delivery` 用分区顺序，producer 端按 `to_user_id` 哈希选 queue（`rmq` 的 `MessageQueueSelector`），consumer 用 FIFO listener，保证单聊到同一接收方有序。
- **重试与幂等**：重试语义从「手写 ZSET」平移到 RocketMQ 消费组的 `RECONSUME_LATER`；消费端重投前查 `msg:status` 去重，与现有逻辑一致，不引入新一致性问题。
- **延迟消息**：RocketMQ 内置延迟级别直接覆盖「30s 后重试」需求；如要精确到指定毫秒，可用 timed/delay message 特性（5.x C++ 已支持）。
- **事务消息（可选后续）**：现在「去重 claim + 落库 + produce」三步非原子，极端情况可能重复投递（靠消费端 `msg:status` 兜底）。若要求 exactly-once 更强，后续可用 RocketMQ 事务消息把「落库」与「produce」绑成半消息提交。本阶段不强制。

## 验证

1. **部署前置**：`docker-compose` 起 RocketMQ 5.x（NameServer + Broker + gRPC Proxy）。C++ 5.x 客户端必须连 Proxy（remoting 协议不行）。
2. **编译**：`cmake --build` 全量通过（新增 gRPC 依赖后重点确认链接）。
3. **端到端**：
   - 起 auth/route/im/gateway + RocketMQ，用 `im_test_client` 两账号互发消息，确认在线投递正常、顺序不乱。
   - 收件人离线时发消息 → 确认走 `storeOfflineMessage`；上线后 `handleConnect` 拉取正常。
   - 制造 Gateway 短暂不可达 → 确认投递消费者 `RECONSUME_LATER` 重试、状态最终 `DELIVERED`，且不重复投递（看 `msg:status` + 客户端去重）。
   - 改密码触发 `im-event` → 确认跨节点踢下线仍生效（替代原 Pub/Sub 后行为不变）。
4. **回归**：现有 14 个 `tests/` 单元测试不受影响（RocketMQ 只在 apps/im 层引入，`src/` 框架不动）。

## 风险与取舍

- **依赖变重**：5.x C++ 客户端带来 gRPC 依赖，构建复杂度上升；若想轻量，可退到 legacy 4.x `apache/rocketmq-client-cpp`（remoting 协议，无需 Proxy，但依赖 libevent/boost/jsoncpp 且已不主推）。计划默认 5.x + Proxy。
- **同进程消费者**：Phase B 消费者与 ImServer 同进程，解耦只到 broker 缓冲层，进程级隔离留给 Phase D。若你更看重立刻能独立扩缩容，可把 Phase D 提前。
- **离线/状态/去重保留 Redis**：与「全量上 broker」字面有出入，已在上表说明理由；如不接受请指出，我再给离线也上 broker 的替代设计。
