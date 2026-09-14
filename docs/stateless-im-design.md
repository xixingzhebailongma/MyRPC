# IM 节点无状态化设计（去掉 Gateway pin）

## 背景与目标

当前 Gateway 会为每条客户端长连接 **pin 到一个固定 IM 节点**（`GatewayServer::ConnState.im_ip/im_port`），
原因只有一个：IM 节点在进程内存里（`UserManager`）保存了"哪些用户/session 在本节点在线"。
这份本地状态让同一个用户的所有会话操作必须落到同一个 IM 节点上，否则状态散落、断开清理/在线判断/踢人会错乱。

而路由可达性（user → gateway 连接）的**权威数据其实已经在 Route Server 的 Redis 里**，IM 回推也是走
`GatewayService.Push` 的普通 TCP RPC，不需要"持有"连接。

**目标**：把 `UserManager` 这份本地在线会话表也搬进共享存储，使 IM 节点彻底无状态，
从而删除 Gateway 的 pin，任意 IM 节点都能处理任意请求。

---

## 现状（关键事实）

| 存储 | 存了什么 | 位置 |
|---|---|---|
| Route Server (Redis) | `user_id → { gateway_id:conn_id → ip:port }` | `route_server.cc:55-60` |
| Auth SessionStore (Redis) | `auth:session:{sid}` hash（user/device/…） | `session_store.h:13-25` |
| **UserManager (IM 进程内存)** | `sessions_`(sid→meta)、`conn_key_to_session_`(conn→sid)、`user_to_sessions_`(uid→{sid}) | `user_manager.h:70-75` |
| Gateway ConnState | `im_ip/im_port`（连接被固定到哪个 IM 节点） | `gateway_server.h:44-50` |

`UserManager` 在 IM 内**只被 `im_server.cc` 使用**（`deliver_server` 已无状态），live 调用点：

| 方法 | 调用点 | 用途 |
|---|---|---|
| `userOnline` | `im_server.cc:248,820` | 登录/重连绑定会话 |
| `isOnline` | `im_server.cc:472,686` | 好友在线 + 踢人归属判断 |
| `getConnectionBySession` | `im_server.cc:724` | 单设备踢（session_id→连接） |
| `userOfflineByConn` | `im_server.cc:929,1096` | 登出/断线清理（conn→user 反查） |
| `onlineUserCount/sessionCount` | `im_server.cc:756-757` | 监控统计 |
| `getConnections` / `getUserIdByConn` | 无调用 | **已死代码** |

---

## 两张权威表 + 一条索引（目标数据模型）

### 1. Route 表（连接权威，增强）
```
key   = im:route:{user_id}
field = {gateway_id}:{conn_id}
value = {gateway_ip}:{gateway_port}:{session_id}
```
- 一个 user 可有多条 field（多端）。
- `session_id` 是后加的，**只用于踢人时匹配连接，投递时忽略**。

### 2. Auth session 表（会话权威，沿用现有）
`auth:session:{sid}` 保证能查到：是否有效/被吊销、属于哪个 `user_id`、设备信息、过期时间。

### 3. 反查索引（断线清理用，RouteServer 内部封装）
```
key   = im:conn:{gateway_id}:{conn_id}
value = user_id
```
- 作用：断线时 Gateway 只给 `gateway_id + conn_id`（无 user_id），靠它反查该 conn 属于哪个 user，从而 HDEL 正确 hash。
- **完全封装在 RouteServer 内部**：写进 `RouteRegister`，删在 `RouteUnregister` / `RouteUnregisterByConn`，IM 不直接读它。
- 生命周期与 route field 一致（若 route 加 TTL，它也加同样 TTL）。

### 在线语义（三分）
```
连接在线 = Route 里该 user 有 field
会话在线 = Auth session 有效
业务在线 = 连接在线 且 会话在线
```
- 好友展示用"连接在线"够用。
- 踢人、发消息、权限判断用"业务在线"（见"遗留项"：发消息的 session 有效性门控是新增检查）。

---

## 四个关键流程

### 1. 登录 / 建连
两条路径，最终都由 IM 写 Route（携带 session_id）：

- **Connect（重连）**：Gateway `resolveTicket` 绑身份 → 注入带 `session_id` 的 Identity → IM `handleConnect` 调 `registerUserOnline(user_id, conn, ip, port, sid)`。
- **Login（首次）**：Gateway pre-auth 透传 → IM `handleLogin` 调 Auth 拿 `session_id` → `registerUserOnline(...)`。

`registerUserOnline` 请求带上 `session_id`，`RouteRegister` 落 value = `ip:port:sid` + 写反查索引。

### 2. 断线清理（反查索引，不依赖 Gateway 带 user_id）
```
Gateway 检测连接关闭
-> 发 ClientDisconnectRequest{gateway_id, conn_id} 给任意 IM 节点
-> IM handleClientDisconnect -> RouteUnregisterByConn{gateway_id, conn_id}
-> RouteServer：GET im:conn:{gw}:{conn} -> user_id -> HDEL route field + DEL 反查索引
```
- Gateway **不改** `ClientDisconnectRequest`（依旧只有 gateway_id + conn_id）。
- 未认证的连接没有反查索引 → GET 空 → 直接 no-op。

### 3. 消息投递（user 粒度，强一致回源）
```
to_user_id -> 查 Route（回源 Redis，不吃本地缓存）-> 所有 field -> 逐条取 ip:port
-> RPC Gateway.Push(conn_id, frame) -> Gateway 按 conn_id 写字节
```
- **投递是 user 粒度**：对用户所有 field（多端）逐条 push，**不需要 sid**。
- **回源**：投递是核心路径，保守起见查 Route 权威数据，不用 60s 缓存。
  （现状用 Stream 预热 cache + 离线消息兜底也能跑，这里选回源更稳。）

### 4. 踢人（单设备 / 强制下线，发起节点直连）
```
发起节点：
1. 吊销 Auth session（先做，幂等）
2. 查 Route 该 user 所有 field（回源）
3. 按 sid 匹配要踢的连接
4. 对匹配的每条：RPC Gateway.Push(force_close) + RouteUnregister
5. push 失败不影响吊销
```
- 不再广播、不再让各节点"认领"。
- `force_close` 后 Gateway 会因连接关闭**再发一次 ClientDisconnect** → 二次 `RouteUnregisterByConn` → 幂等 HDEL，无害。

---

## 需要改的文件与要点

### 协议 `apps/im/proto/im.proto`
- `RouteServer` 增加 `string session_id = 5;`
- `RouteRegisterRequest` 增加 `string session_id = 6;`
- 新增 `RouteUnregisterByConnRequest { string gateway_id = 1; uint64 conn_id = 2; }` / `Response`（disconnect 用）。
- `ClientDisconnectRequest` **不改**。

### Route `apps/im/services/route/route_server.{h,cc}`
- `handleRouteRegister`：value = `ip:port:sid`；**原子写** route field + 反查索引
  `im:conn:{gw}:{conn} → user_id`（用 Lua 或 MULTI，参照 `session_store.cc` 的 `eval`）。
- `handleRouteQuery`：解析 value 回填 `session_id`。
- `handleRouteUnregister`：HDEL route field + **DEL 反查索引**。
- 新增 `handleRouteUnregisterByConn`：GET 反查索引 → user_id → HDEL route field + DEL 反查索引。

### Gateway `apps/im/services/gateway/gateway_server.{h,cc}`
- `ConnState` 删除 `im_ip/im_port`。
- `forwardToIm`：删除 pin 读取/写回（`gateway_server.cc:157-170, 234-235, 270-278`）；每消息从
  `im_client_` 选节点，重试用 `pickNodeExcept(tried)` 做 failover（保留现有白名单重试）。
- `onConnectionClosed`（`gateway_server.cc:78-104`）：捕获 `identity.user_id()`（非空表示认证过），
  非空才发 `ClientDisconnect`（替换"`im_ip` 非空才发"）。
- `notifyDisconnect`（`gateway_server.cc:339-349`）：目标改为轮询 IM 节点，body 仍只带 `gateway_id + conn_id`。

### IM `apps/im/services/im/im_server.{h,cc}`
- 删除成员 `UserManager user_manager_`（`im_server.h:127`）与 `user_subscriber_`（`.h:124`）。
- `handleLogin`/`handleConnect`：去掉 `userOnline`；`registerUserOnline` 传 session_id。
- `registerUserOnline`（`im_server.cc:622-636`）：请求带 session_id。
- `handleLogout`（`im_server.cc:902-940`）：用验签得到的 `uid` + header 的 `gateway_id/conn_id`
  直接 `unregisterUserFromRoute`，删 `userOfflineByConn`。
- `handleClientDisconnect`（`im_server.cc:1085-1106`）：改为调 `RouteUnregisterByConn`，删 `userOfflineByConn`。
- `handleGetFriendList`（`im_server.cc:472`）：`is_online = !resolveRoutes(fid).empty()`。
- `handleKickSession`/`handleKickAllSessions`：直连踢（见流程 4），删 `publishUserEvent`。
- 删除 `kickOffline`/`onUserChanged`/`publishUserEvent` 与 `user_subscriber_.start(...)`（`im_server.cc:183-187`），
  以及 `user_dao_.setPublisher`（`im_server.cc:81-103`）的广播发布器。
- `handleChangePassword` 等触发用户变更的 handler：写库成功后改调 `kickUserOffline`。
- 新增 helper（替代 `kickOffline`）：
  - `kickUserOffline(uid, notice_text)`：resolveRoutes(回源) → 逐连接 push(force_close) + unregister。
  - `kickSessionOffline(uid, session_id, notice_text)`：resolveRoutes(回源) → 按 sid 匹配 → push + unregister。
- `reportOnlineStats`（`im_server.cc:753-760`）：删除或改为从 Route/Redis 统计（见"遗留项"）。

### 删除 `apps/im/services/im/user_manager.{h,cc}`
- 连同死代码 `getConnections`/`getUserIdByConn` 一并移除。

---

## 分阶段落地（每步可独立验证、可回滚）

1. **P0 断线清理去本地化（反查索引）**：RouteServer 加反查索引 + `RouteUnregisterByConn`；
   IM `handleClientDisconnect` 改调它；Gateway 去掉对 pin 的依赖（`onConnectionClosed` 按 identity 判断、`notifyDisconnect` 轮询）。
2. **P1 session_id 进 Route 表**：proto + route_server + `registerUserOnline` + login/connect。
3. **P2 isOnline 查路由**：`handleGetFriendList` 改造。
4. **P3 踢人直连化**：`handleKickSession/All` + DAO 变更事件，删广播链。
5. **P4 删 pin + 删 UserManager**：Gateway 去 pin、`forwardToIm` 轮询；删除 `UserManager`。
6. **P5 监控/清理**：`reportOnlineStats` 与死代码清理。

> 说明：P0 里 Gateway 已能"不依赖 pin 判断断线"，但 pin 字段本身在 P4 才彻底删除——P0~P3 期间 pin 仍可作为
> 无害的优化保留，降低一次性大改风险。

---

## 验证方式

- **多节点一致性**：起 2 个 IM 节点 + 1 个 Gateway + Route + Auth + Redis/MySQL。
- **登录/重连**：user1 连 gateway，`im:route:user1` 出现 `gw:conn → ip:port:sid`，且 `im:conn:gw:conn → user1` 存在。
- **去 pin 后路由**：连续发多条消息，观察日志确认轮询到不同 IM 节点，功能仍正常（发送/送达/ACK）。
- **断线清理**：kill 客户端，`im:route:user1` 对应 field 与 `im:conn:*` 反查索引都被删。
- **单设备踢**：双设备登录，KickSession 只断目标 session 的连接，另一设备在线；route/反查索引一致。
- **改密/封禁踢人**：改密后用户连接收到系统通知并被 force_close，route 清空。
- **离线消息**：断线期间发消息 → 重连后拉取/自动下发正常。
- 复用现有测试：`tests/`、`apps/im/common/db/user_dao_test.cc`；跑 `build/`、`build-asan`、`build-tsan`。

---

## 遗留项 / 风险

- **Route 泄漏**：Gateway 崩溃时其连接全部消失，但无 Gateway 发起 `ClientDisconnect`，route + 反查索引会残留。
  建议后续给 route field 与反查索引加 TTL + 心跳续期。
- **一致语义变化**：`isOnline` 由强一致变最终一致（≤60s 缓存），仅用于好友展示，可接受；投递/踢人已改为回源。
- **"业务在线"门控（新增）**：现在 `handleSendMessage` 只验 HMAC 签名（`verifyIdentity`，`im_server.cc:762-792`），
  签名有效 ≠ 会话仍有效（吊销后到 force_close 之间有 TOCTOU 窗口）。若要"发消息用业务在线"，需每条消息
  额外查 Auth session 有效性（多一次 Auth 往返或加 session-validity 缓存）——是改进，但属新增成本，单独排期。
- **Route 客户端一致性哈希**：`ImServer::route_client_` 用 `ConsistentHashBalancer` 按 user_id 路由，
  但 RouteServer 本身无状态（Redis 后端），可改为轮询，属顺手简化，非必须。
- **监控统计**：`onlineUserCount/sessionCount` 删除后，如需在线数可用 Redis `SCAN im:route:*` 或
  在 RouteServer 维护计数器（register +1 / unregister -1），作为可选增强。
