// IM 全链路 E2E：起 route/auth/im/gateway/deliver 五个真实服务进程，
// 用 ImClientConn 驱动完整业务流并断言：
//   注册 → 登录 → 加好友 → 发消息 → 在线推送 → ACK 回执 →
//   离线消息（对端离线时发送，上线后自动补推）→ 断线重连。
//
// 前置：etcd / redis / mysql 已就绪（apps/im/deploy/docker-compose.yml）。
// 隔离：route/auth 用唯一服务名 + 唯一端口；IM 服务名固定为 "ImService"，
//       因此要求 etcd 里没有其它正在运行的 IM 节点。
#include "im.pb.h"
#include "im_client_conn.h"
#include "request_id.h"
#include "test_helpers.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace im;

namespace {

std::string envStr(const char *name, const std::string &def) {
  const char *v = std::getenv(name);
  return (v && *v) ? std::string(v) : def;
}

int fail(const char *msg) {
  std::fprintf(stderr, "FAILED: %s\n", msg);
  return 1;
}

// 线程安全的推送收集器：push 回调在 ImClientConn 收帧线程里触发。
class PushQueue {
public:
  struct Item {
    ServerPushEnvelope::PayloadType type = ServerPushEnvelope::CHAT_MESSAGE;
    ChatMessage chat;
    MessageAck ack;
  };

  void add(Item it) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      q_.push_back(std::move(it));
    }
    cv_.notify_all();
  }

  // 等待一条 CHAT_MESSAGE 推送；返回其 content / from_user_id。
  bool waitChat(const std::string &expect_from, const std::string &expect_content,
                int timeout_ms, std::string *content_out = nullptr) {
    std::unique_lock<std::mutex> lk(mu_);
    if (!cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
          for (const auto &it : q_)
            if (it.type == ServerPushEnvelope::CHAT_MESSAGE &&
                it.chat.from_user_id() == expect_from &&
                it.chat.content() == expect_content)
              return true;
          return false;
        }))
      return false;
    if (content_out)
      *content_out = expect_content;
    return true;
  }

  // 等待一条 DELIVERY_ACK 推送（来自对端的回执）。
  bool waitAck(int timeout_ms) {
    std::unique_lock<std::mutex> lk(mu_);
    return cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::DELIVERY_ACK)
          return true;
      return false;
    });
  }

private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Item> q_;
};

// 一个 IM 客户端会话：封装 ImClientConn + 常用业务调用。
class Client {
public:
  explicit Client(PushQueue *q = nullptr) : q_(q) {}
  ~Client() { conn_.close(); }

  void setPushQueue(PushQueue *q) { q_ = q; }

  bool open(const std::string &ip, int port) { return conn_.connect(ip, port); }
  void close() { conn_.close(); }
  bool connected() const { return conn_.connected(); }

  // 安装推送处理器：把 ServerPushEnvelope 解析后塞进 q_。
  void installPush() {
    conn_.setPushHandler([this](const std::string &payload) {
      ServerPushEnvelope env;
      if (!env.ParseFromString(payload) || !q_)
        return;
      PushQueue::Item it;
      it.type = env.type();
      if (env.type() == ServerPushEnvelope::CHAT_MESSAGE)
        it.chat.ParseFromString(env.payload());
      else if (env.type() == ServerPushEnvelope::DELIVERY_ACK)
        it.ack.ParseFromString(env.payload());
      q_->add(std::move(it));
    });
  }

  // 通用 RPC：返回响应 body（失败/超时返回空串）。
  std::string rpc(const std::string &method, const std::string &body,
                  int timeout_ms = 5000) {
    return conn_.call("ImService", method, body, timeout_ms);
  }

  bool registerUser(const std::string &u, const std::string &p,
                    std::string *msg = nullptr) {
    RegisterRequest r;
    r.set_username(u);
    r.set_password(p);
    RegisterResponse resp;
    bool ok = resp.ParseFromString(rpc("Register", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    return ok && resp.success();
  }

  bool login(const std::string &u, const std::string &p, std::string *at,
             std::string *rt, std::string *msg = nullptr) {
    LoginRequest r;
    r.set_username(u);
    r.set_password(p);
    r.set_device_id("e2e-device");
    LoginResponse resp;
    bool ok = resp.ParseFromString(rpc("Login", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    if (!ok || !resp.success())
      return false;
    if (at)
      *at = resp.access_token();
    if (rt)
      *rt = resp.refresh_token();
    return true;
  }

  // 用 access_token 换一次性 ticket 并绑定连接（断线重连标准流程）。
  bool bind(const std::string &at, std::string *err = nullptr) {
    IssueTicketRequest tr;
    tr.set_access_token(at);
    IssueTicketResponse tresp;
    if (!tresp.ParseFromString(rpc("IssueTicket", tr.SerializeAsString())) ||
        !tresp.success()) {
      if (err)
        *err = tresp.message();
      return false;
    }
    ConnectRequest cr;
    cr.set_ticket(tresp.ticket());
    ConnectResponse cresp;
    if (!cresp.ParseFromString(rpc("Connect", cr.SerializeAsString())) ||
        !cresp.success()) {
      if (err)
        *err = cresp.message();
      return false;
    }
    return true;
  }

  bool addFriend(const std::string &fid, std::string *msg = nullptr) {
    AddFriendRequest r;
    r.set_friend_id(fid);
    AddFriendResponse resp;
    bool ok = resp.ParseFromString(rpc("AddFriend", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    return ok && resp.success();
  }

  bool getFriendList(std::vector<std::string> *friends) {
    GetFriendListRequest r;
    GetFriendListResponse resp;
    if (!resp.ParseFromString(rpc("GetFriendList", r.SerializeAsString())) ||
        !resp.success())
      return false;
    friends->clear();
    for (const auto &f : resp.friends())
      friends->push_back(f.user_id());
    return true;
  }

  bool send(const std::string &to, const std::string &content,
            std::string *msg_id = nullptr, std::string *msg = nullptr) {
    SendMessageRequest r;
    ChatMessage *m = r.mutable_msg();
    m->set_to_user_id(to);
    m->set_content(content);
    m->set_chat_type(0);
    r.set_client_request_id(generateRequestId());
    SendMessageResponse resp;
    bool ok = resp.ParseFromString(rpc("SendMessage", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    if (!ok || !resp.success())
      return false;
    if (msg_id)
      *msg_id = resp.msg_id();
    return true;
  }

  bool ack(const std::string &msg_id, const std::string &from,
           const std::string &to) {
    AckMessageRequest r;
    auto *a = r.mutable_ack();
    a->set_msg_id(msg_id);
    a->set_from_user_id(from); // 发 ACK 的人（本端）
    a->set_to_user_id(to);     // 收 ACK 的人（原始发送方）
    a->set_status(im::MessageStatus::DELIVERED);
    AckMessageResponse resp;
    return resp.ParseFromString(rpc("AckMessage", r.SerializeAsString())) &&
           resp.success();
  }

private:
  ImClientConn conn_;
  PushQueue *q_ = nullptr;
};

// 启动一个服务进程，返回 pid；失败返回 -1。
pid_t startService(const std::string &bin, const std::vector<std::string> &args,
                   const std::string &log) {
  std::vector<std::string> argv{bin};
  argv.insert(argv.end(), args.begin(), args.end());
  return testutil::spawn(argv, log);
}

// 等待 "ImService" 在 etcd 里稳定为恰好 1 个节点且地址 == myAddr。
// 处理「残留的其它 IM 节点 lease 未过期」导致的串扰：等到旧 key 过期消失。
bool waitForImService(const std::string &etcd, const std::string &myAddr,
                      int timeout_ms = 60000) {
  ServiceDiscovery sd(etcd);
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    auto nodes = sd.discover("ImService");
    if (nodes && nodes->size() == 1 && (*nodes)[0].address() == myAddr)
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }
  return false;
}

} // namespace

int main() {
  const std::string etcd = envStr("RPC_TEST_ETCD", "http://127.0.0.1:2379");
  const std::string redisIp = envStr("RPC_TEST_REDIS_IP", "127.0.0.1");
  const int redisPort = std::atoi(envStr("RPC_TEST_REDIS_PORT", "6379").c_str());
  const std::string mysqlHost =
      envStr("RPC_TEST_MYSQL_HOST", "127.0.0.1");
  const int mysqlPort = std::atoi(envStr("RPC_TEST_MYSQL_PORT", "3306").c_str());

  // 前置依赖探测：缺任一直接 SKIP。
  if (!testutil::etcdReachable(etcd) ||
      !testutil::tcpPortOpen(redisIp, static_cast<uint16_t>(redisPort)) ||
      !testutil::tcpPortOpen(mysqlHost, static_cast<uint16_t>(mysqlPort))) {
    std::printf("test_im_e2e: SKIP (etcd/redis/mysql not ready)\n");
    return 0;
  }

  // 唯一服务名 + 唯一端口，隔离多次运行。
  const std::string ts = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const std::string routeSvc = "RouteService." + ts;
  const std::string authSvc = "AuthService." + ts;

  const uint16_t routePort = testutil::pickFreePort();
  const uint16_t authPort = testutil::pickFreePort();
  const uint16_t imPort = testutil::pickFreePort();
  const uint16_t gwClientPort = testutil::pickFreePort();
  const uint16_t gwRpcPort = testutil::pickFreePort();
  if (!routePort || !authPort || !imPort || !gwClientPort || !gwRpcPort)
    return fail("pickFreePort()");

  const std::string logDir = "/tmp/myrpc_e2e_" + ts;
  std::string mkdirCmd = "mkdir -p " + logDir;
  std::system(mkdirCmd.c_str());

  std::vector<pid_t> pids;
  auto stopAll = [&]() {
    for (auto p : pids)
      testutil::stopProcess(p);
    std::system(("rm -rf " + logDir).c_str());
  };
  // RAII：任何路径退出 main 前都停掉子进程、清理日志目录。
  struct Cleanup {
    std::function<void()> fn;
    ~Cleanup() { fn(); }
  } cleanup{stopAll};

  // 1) route → 2) auth → 3) im → 4) gateway → 5) deliver
  pids.push_back(startService(
      ROUTE_SERVER_BIN,
      {"--server.ip=127.0.0.1",
       "--server.port=" + std::to_string(routePort),
       "--redis.ip=" + redisIp,
       "--redis.port=" + std::to_string(redisPort),
       "--etcd.endpoints=" + etcd, "--service.name=" + routeSvc},
      logDir + "/route.log"));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  pids.push_back(startService(
      AUTH_SERVER_BIN,
      {"--server.ip=127.0.0.1",
       "--server.port=" + std::to_string(authPort),
       "--redis.ip=" + redisIp,
       "--redis.port=" + std::to_string(redisPort),
       "--etcd.endpoints=" + etcd, "--service.name=" + authSvc,
       "--mysql.host=" + mysqlHost,
       "--mysql.port=" + std::to_string(mysqlPort), "--mysql.user=root",
       "--mysql.password=", "--mysql.db=myrpc_im"},
      logDir + "/auth.log"));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  pids.push_back(startService(
      IM_SERVER_BIN,
      {"--server.ip=127.0.0.1", "--server.port=" + std::to_string(imPort),
       "--server.id=IM1", "--worker.id=1", "--etcd.endpoints=" + etcd,
       "--route.service=" + routeSvc, "--auth.service=" + authSvc,
       "--redis.ip=" + redisIp, "--redis.port=" + std::to_string(redisPort),
       "--mysql.host=" + mysqlHost,
       "--mysql.port=" + std::to_string(mysqlPort), "--mysql.user=root",
       "--mysql.password=", "--mysql.db=myrpc_im", "--shared.secret=devsecret"},
      logDir + "/im.log"));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  pids.push_back(startService(
      GATEWAY_SERVER_BIN,
      {"--client.ip=127.0.0.1",
       "--client.port=" + std::to_string(gwClientPort),
       "--rpc.ip=127.0.0.1", "--rpc.port=" + std::to_string(gwRpcPort),
       "--gateway.id=GW1", "--etcd.endpoints=" + etcd, "--im.service=ImService",
       "--auth.service=" + authSvc, "--shared.secret=devsecret"},
      logDir + "/gateway.log"));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  pids.push_back(startService(
      DELIVER_SERVER_BIN,
      {"--server.id=deliver", "--worker.id=0", "--etcd.endpoints=" + etcd,
       "--route.service=" + routeSvc, "--redis.ip=" + redisIp,
       "--redis.port=" + std::to_string(redisPort), "--consumer.name=d1"},
      logDir + "/deliver.log"));

  // 就绪：等待各服务端口开放 + etcd 里 route/auth/im 都注册完成。
  bool ready = testutil::waitForPort("127.0.0.1", routePort) &&
               testutil::waitForPort("127.0.0.1", authPort) &&
               testutil::waitForPort("127.0.0.1", imPort) &&
               testutil::waitForPort("127.0.0.1", gwClientPort);
  // "ImService" 是固定服务名：必须确认 etcd 里只剩本测试的 IM 节点，
  // 否则 Gateway 的轮询可能打到残留的旧节点。
  ready = ready &&
          waitForImService(etcd, std::string("127.0.0.1:") +
                                    std::to_string(imPort));
  // 留出各客户端 discovery/watch 的暖机时间。
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  if (!ready)
    return fail("some service did not become ready");

  // 每次运行用唯一用户名，避免 MySQL 残留冲突。
  const std::string alice = "alice_" + ts;
  const std::string bob = "bob_" + ts;
  const std::string carol = "carol_" + ts;

  PushQueue pushA, pushB, pushC;
  Client a(&pushA), b(&pushB), c(&pushC);

  // ---- 注册 ----
  if (!a.open("127.0.0.1", gwClientPort))
    return fail("A open failed");
  if (!a.registerUser(alice, "pass123"))
    return fail("register A failed");
  a.close();

  if (!b.open("127.0.0.1", gwClientPort))
    return fail("B open failed");
  if (!b.registerUser(bob, "pass123"))
    return fail("register B failed");
  b.close();

  if (!c.open("127.0.0.1", gwClientPort))
    return fail("C open failed");
  if (!c.registerUser(carol, "pass123"))
    return fail("register C failed");
  c.close();

  // ---- 登录 ----
  std::string atA, rtA, atB, rtB;
  if (!a.open("127.0.0.1", gwClientPort) || !a.login(alice, "pass123", &atA, &rtA))
    return fail("login A failed");
  a.close();
  if (!b.open("127.0.0.1", gwClientPort) || !b.login(bob, "pass123", &atB, &rtB))
    return fail("login B failed");
  b.close();
  if (atA.empty() || atB.empty())
    return fail("empty access token");

  // ---- 加好友：A 加 B ----
  if (!a.open("127.0.0.1", gwClientPort))
    return fail("A reopen for addFriend failed");
  a.installPush();
  if (!a.bind(atA))
    return fail("A bind failed");
  if (!a.addFriend(bob))
    return fail("A addFriend(B) failed");
  a.close();

  // ---- A、B 上线（绑定 + 推送处理器） ----
  if (!a.open("127.0.0.1", gwClientPort) || !a.bind(atA))
    return fail("A online failed");
  a.installPush();
  if (!b.open("127.0.0.1", gwClientPort) || !b.bind(atB))
    return fail("B online failed");
  b.installPush();

  // ---- 发消息 + 在线推送 ----
  std::string msgId;
  if (!a.send(bob, "hello-bob", &msgId))
    return fail("A send to B failed");
  if (msgId.empty())
    return fail("empty msg_id");
  if (!pushB.waitChat(alice, "hello-bob", 8000))
    return fail("B did not receive push");

  // ---- ACK 回执：B 回执 → A 收到 DELIVERY_ACK ----
  if (!b.ack(msgId, bob, alice))
    return fail("B ack failed");
  if (!pushA.waitAck(8000))
    return fail("A did not receive DELIVERY_ACK");

  // ---- 好友列表：A 应看到 B ----
  {
    std::vector<std::string> friends;
    if (!a.getFriendList(&friends))
      return fail("A getFriendList failed");
    bool found = false;
    for (const auto &f : friends)
      if (f == bob)
        found = true;
    if (!found)
      return fail("A friend list missing B");
  }

  // ---- 离线消息：A 发给未上线的 C，C 上线后自动补推 ----
  if (!a.send(carol, "offline-hello", &msgId))
    return fail("A send to C (offline) failed");
  // C 此时离线；登录 + 绑定（Connect 会触发离线补推）。
  if (!c.open("127.0.0.1", gwClientPort))
    return fail("C open failed");
  std::string atC, rtC;
  if (!c.login(carol, "pass123", &atC, &rtC))
    return fail("login C failed");
  c.close();
  if (!c.open("127.0.0.1", gwClientPort))
    return fail("C reopen failed");
  c.installPush();
  if (!c.bind(atC))
    return fail("C bind failed");
  if (!pushC.waitChat(alice, "offline-hello", 8000))
    return fail("C did not receive offline message");

  // ---- 断线重连：B 断开 → 重连 → 仍可收发 ----
  b.close();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  if (!b.open("127.0.0.1", gwClientPort))
    return fail("B reconnect open failed");
  b.installPush();
  if (!b.bind(atB))
    return fail("B reconnect bind failed");
  // B 重连后给 A 发一条，验证重连后会话仍可用。
  if (!b.send(alice, "after-reconnect"))
    return fail("B send after reconnect failed");
  if (!pushA.waitChat(bob, "after-reconnect", 8000))
    return fail("A did not receive post-reconnect message");

  a.close();
  b.close();
  c.close();

  std::printf("PASSED\n");
  return 0;
}
