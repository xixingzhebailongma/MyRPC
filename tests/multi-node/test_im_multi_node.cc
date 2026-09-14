// IM 多节点一致性 E2E：起 2 个 IM 节点（同 "ImService" 服务名、不同 worker_id/端口）
// + 1 个 Gateway + Route + Auth + Deliver，验证无状态化改造的核心命题——
// 「任意 IM 节点都能服务任意请求」：
//
//   1. 路由与反查索引落在共享 Redis（而非节点本地内存）：直连 Redis 校验
//      im:route:{uid} 的 field 与 im:conn:{gw}:{conn} 反查索引一致。
//   2. 完整业务流在跨节点（Gateway 轮询分发）下仍正确：登录 / 收发 / ACK /
//      多端扇出 / 离线补推 / 跨节点单设备踢 / 改密踢人。
//   3. 去 pin 生效：两个 IM 节点都实际处理过请求（从各自日志统计）。
//
// 前置：etcd / redis(7.x) / mysql 已就绪；etcd 里不能有其它运行中的 ImService。
// 说明：Client/PushQueue 等辅助与原 test_im_business.cc 同构，为独立可执行而复制。
#include "im.pb.h"
#include "im_client_conn.h"
#include "redis_client.h"
#include "request_id.h"
#include "test_helpers.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace im;

namespace {

std::string envStr(const char *name, const std::string &def) {
  const char *v = std::getenv(name);
  return (v && *v) ? std::string(v) : def;
}

int g_fail = 0;
#define SECTION(title) std::printf("\n== %s ==\n", title)
#define CHECK(cond, msg)                                                     \
  do {                                                                       \
    if (cond) {                                                              \
      std::printf("    ok: %s\n", msg);                                      \
    } else {                                                                 \
      std::printf("    FAIL: %s\n", msg);                                    \
      ++g_fail;                                                              \
    }                                                                        \
  } while (0)

void sleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

// 线程安全推送收集器。
class PushQueue {
public:
  struct Item {
    ServerPushEnvelope::PayloadType type = ServerPushEnvelope::CHAT_MESSAGE;
    ChatMessage chat;
    MessageAck ack;
    SystemNotice notice;
  };

  void add(Item it) {
    { std::lock_guard<std::mutex> lk(mu_); q_.push_back(std::move(it)); }
    cv_.notify_all();
  }

  bool waitChat(const std::string &from, const std::string &content, int ms) {
    std::unique_lock<std::mutex> lk(mu_);
    return cv_.wait_for(lk, std::chrono::milliseconds(ms), [&] {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::CHAT_MESSAGE &&
            it.chat.from_user_id() == from && it.chat.content() == content)
          return true;
      return false;
    });
  }

  bool waitAck(int ms) {
    std::unique_lock<std::mutex> lk(mu_);
    return cv_.wait_for(lk, std::chrono::milliseconds(ms), [&] {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::DELIVERY_ACK)
          return true;
      return false;
    });
  }

  bool waitNotice(int ms, std::string *text = nullptr) {
    std::unique_lock<std::mutex> lk(mu_);
    bool ok = cv_.wait_for(lk, std::chrono::milliseconds(ms), [&] {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::SYSTEM_NOTICE)
          return true;
      return false;
    });
    if (ok && text) {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::SYSTEM_NOTICE) { *text = it.notice.message(); break; }
    }
    return ok;
  }

  int countNotice() {
    std::lock_guard<std::mutex> lk(mu_);
    int n = 0;
    for (const auto &it : q_)
      if (it.type == ServerPushEnvelope::SYSTEM_NOTICE) ++n;
    return n;
  }

private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Item> q_;
};

// IM 客户端会话封装。
class Client {
public:
  struct Session { std::string session_id; std::string device_id; int device_type = 0; };

  explicit Client(PushQueue *q = nullptr) : q_(q) {}
  ~Client() { conn_.close(); }
  void setPushQueue(PushQueue *q) { q_ = q; }
  bool open(const std::string &ip, int port) { return conn_.connect(ip, port); }
  void close() { conn_.close(); }

  void installPush() {
    conn_.setPushHandler([this](const std::string &payload) {
      ServerPushEnvelope env;
      if (!env.ParseFromString(payload) || !q_) return;
      PushQueue::Item it;
      it.type = env.type();
      if (env.type() == ServerPushEnvelope::CHAT_MESSAGE)
        it.chat.ParseFromString(env.payload());
      else if (env.type() == ServerPushEnvelope::DELIVERY_ACK)
        it.ack.ParseFromString(env.payload());
      else if (env.type() == ServerPushEnvelope::SYSTEM_NOTICE)
        it.notice.ParseFromString(env.payload());
      q_->add(std::move(it));
    });
  }

  std::string rpc(const std::string &method, const std::string &body, int ms = 5000) {
    return conn_.call("ImService", method, body, ms);
  }

  bool registerUser(const std::string &u, const std::string &p, std::string *msg = nullptr) {
    RegisterRequest r; r.set_username(u); r.set_password(p);
    RegisterResponse resp;
    bool ok = resp.ParseFromString(rpc("Register", r.SerializeAsString()));
    if (msg) *msg = resp.message();
    return ok && resp.success();
  }

  bool login(const std::string &u, const std::string &p, std::string *at, std::string *rt,
             std::string *msg = nullptr, const std::string &device_id = "e2e-device",
             int device_type = 0) {
    LoginRequest r; r.set_username(u); r.set_password(p);
    r.set_device_id(device_id); r.set_device_type(device_type);
    LoginResponse resp;
    bool ok = resp.ParseFromString(rpc("Login", r.SerializeAsString()));
    if (msg) *msg = resp.message();
    if (!ok || !resp.success()) return false;
    if (at) *at = resp.access_token();
    if (rt) *rt = resp.refresh_token();
    return true;
  }

  bool issueTicket(const std::string &at, std::string *ticket, std::string *msg = nullptr) {
    IssueTicketRequest tr; tr.set_access_token(at);
    IssueTicketResponse tresp;
    if (!tresp.ParseFromString(rpc("IssueTicket", tr.SerializeAsString())) || !tresp.success()) {
      if (msg) *msg = tresp.message();
      return false;
    }
    if (ticket) *ticket = tresp.ticket();
    return true;
  }

  bool bind(const std::string &at, std::string *err = nullptr) {
    std::string ticket;
    if (!issueTicket(at, &ticket, err)) return false;
    ConnectRequest cr; cr.set_ticket(ticket);
    ConnectResponse cresp;
    if (!cresp.ParseFromString(rpc("Connect", cr.SerializeAsString())) || !cresp.success()) {
      if (err) *err = cresp.message();
      return false;
    }
    return true;
  }

  bool addFriend(const std::string &fid, std::string *msg = nullptr) {
    AddFriendRequest r; r.set_friend_id(fid);
    AddFriendResponse resp;
    bool ok = resp.ParseFromString(rpc("AddFriend", r.SerializeAsString()));
    if (msg) *msg = resp.message();
    return ok && resp.success();
  }

  bool send(const std::string &to, const std::string &content, std::string *msg_id = nullptr) {
    SendMessageRequest r;
    ChatMessage *m = r.mutable_msg();
    m->set_to_user_id(to); m->set_content(content); m->set_chat_type(0);
    r.set_client_request_id(generateRequestId());
    SendMessageResponse resp;
    bool ok = resp.ParseFromString(rpc("SendMessage", r.SerializeAsString()));
    if (!ok || !resp.success()) return false;
    if (msg_id) *msg_id = resp.msg_id();
    return true;
  }

  bool ack(const std::string &msg_id, const std::string &from, const std::string &to) {
    AckMessageRequest r;
    auto *a = r.mutable_ack();
    a->set_msg_id(msg_id); a->set_from_user_id(from); a->set_to_user_id(to);
    a->set_status(im::MessageStatus::DELIVERED);
    AckMessageResponse resp;
    return resp.ParseFromString(rpc("AckMessage", r.SerializeAsString())) && resp.success();
  }

  bool listSessions(std::vector<Session> *out, std::string *msg = nullptr) {
    ListSessionsRequest r;
    ListSessionsResponse resp;
    if (!resp.ParseFromString(rpc("ListSessions", r.SerializeAsString())) || !resp.success()) {
      if (msg) *msg = resp.message();
      return false;
    }
    out->clear();
    for (const auto &s : resp.sessions())
      out->push_back({s.session_id(), s.device_id(), static_cast<int>(s.device_type())});
    return true;
  }

  bool kickSession(const std::string &sid, std::string *msg = nullptr) {
    KickSessionRequest r; r.set_session_id(sid);
    KickSessionResponse resp;
    bool ok = resp.ParseFromString(rpc("KickSession", r.SerializeAsString()));
    if (msg) *msg = resp.message();
    return ok && resp.success();
  }

  bool kickAllSessions(int *kicked = nullptr) {
    KickAllSessionsRequest r;
    KickAllSessionsResponse resp;
    bool ok = resp.ParseFromString(rpc("KickAllSessions", r.SerializeAsString()));
    if (!ok || !resp.success()) return false;
    if (kicked) *kicked = resp.kicked();
    return true;
  }

  bool changePassword(const std::string &newp, std::string *msg = nullptr) {
    ChangePasswordRequest r; r.set_new_password(newp);
    ChangePasswordResponse resp;
    bool ok = resp.ParseFromString(rpc("ChangePassword", r.SerializeAsString()));
    if (msg) *msg = resp.message();
    return ok && resp.success();
  }

private:
  ImClientConn conn_;
  PushQueue *q_ = nullptr;
};

// open + installPush + login + bind，一条连接贯穿设备生命周期。
bool onlineDevice(Client &c, const std::string &gw_ip, int gw_port, const std::string &u,
                  const std::string &p, const std::string &device_id, int device_type,
                  std::string *at, std::string *rt, std::string *err = nullptr) {
  if (!c.open(gw_ip, gw_port)) return false;
  c.installPush();
  if (!c.login(u, p, at, rt, err, device_id, device_type)) return false;
  return c.bind(*at, err);
}

pid_t startService(const std::string &bin, const std::vector<std::string> &args,
                   const std::string &log) {
  std::vector<std::string> argv{bin};
  argv.insert(argv.end(), args.begin(), args.end());
  return testutil::spawn(argv, log);
}

// 等待 etcd 里 "ImService" 稳定为恰好 addrs.size() 个节点且地址集合一致。
bool waitForImServices(const std::string &etcd, const std::vector<std::string> &addrs,
                       int timeout_ms = 60000) {
  ServiceDiscovery sd(etcd);
  std::set<std::string> want(addrs.begin(), addrs.end());
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    auto nodes = sd.discover("ImService");
    if (nodes && nodes->size() == want.size()) {
      std::set<std::string> got;
      for (const auto &n : *nodes) got.insert(n.address());
      if (got == want) return true;
    }
    sleepMs(500);
  }
  return false;
}

// 从雪花 msg_id 解出 worker_id（[41ms][10 worker_id][12 seq]，worker_id 在 bit12~21）。
// 两个 IM 节点用不同 worker_id，据此判断消息是否被分发到不同节点处理（去 pin）。
int workerIdOf(const std::string &msg_id) {
  uint64_t id = std::stoull(msg_id);
  return static_cast<int>((id >> 12) & 0x3FF);
}

// 校验 uid 的路由确实落在共享 Redis：im:route:{uid} 有 field，且每条 field
// 对应的反查索引 im:conn:{gw}:{conn} 指回 uid。
bool verifyRouteInRedis(RedisClient &redis, const std::string &uid) {
  auto fields = redis.hgetall("im:route:" + uid);
  if (fields.empty()) return false;
  for (const auto &kv : fields) {
    auto pos = kv.first.rfind(':');
    if (pos == std::string::npos) return false;
    std::string gw = kv.first.substr(0, pos);
    std::string conn = kv.first.substr(pos + 1);
    if (redis.get("im:conn:" + gw + ":" + conn) != uid) return false;
  }
  return true;
}

} // namespace

int main() {
  const std::string etcd = envStr("RPC_TEST_ETCD", "http://127.0.0.1:2379");
  const std::string redisIp = envStr("RPC_TEST_REDIS_IP", "127.0.0.1");
  const int redisPort = std::atoi(envStr("RPC_TEST_REDIS_PORT", "6379").c_str());
  const std::string mysqlHost = envStr("RPC_TEST_MYSQL_HOST", "127.0.0.1");
  const int mysqlPort = std::atoi(envStr("RPC_TEST_MYSQL_PORT", "3306").c_str());

  if (!testutil::etcdReachable(etcd) ||
      !testutil::tcpPortOpen(redisIp, static_cast<uint16_t>(redisPort)) ||
      !testutil::tcpPortOpen(mysqlHost, static_cast<uint16_t>(mysqlPort))) {
    std::printf("test_im_multi_node: SKIP (etcd/redis/mysql not ready)\n");
    return 0;
  }

  const std::string ts = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count());
  const std::string routeSvc = "RouteService." + ts;
  const std::string authSvc = "AuthService." + ts;

  const uint16_t routePort = testutil::pickFreePort();
  const uint16_t authPort = testutil::pickFreePort();
  const uint16_t im1Port = testutil::pickFreePort();
  const uint16_t im2Port = testutil::pickFreePort();
  const uint16_t gwClientPort = testutil::pickFreePort();
  const uint16_t gwRpcPort = testutil::pickFreePort();
  if (!routePort || !authPort || !im1Port || !im2Port || !gwClientPort || !gwRpcPort) {
    std::printf("FAILED: pickFreePort()\n");
    return 1;
  }

  const std::string logDir = "/tmp/myrpc_multi_" + ts;
  std::system(("mkdir -p " + logDir).c_str());
  std::printf("logs: %s\n", logDir.c_str());

  std::vector<pid_t> pids;
  auto stopAll = [&]() {
    for (auto p : pids) testutil::stopProcess(p);
    if (!std::getenv("KEEP_BIZ_LOGS")) std::system(("rm -rf " + logDir).c_str());
  };
  struct Cleanup { std::function<void()> fn; ~Cleanup() { fn(); } } cleanup{stopAll};

  // 1) route → 2) auth → 3) im1 + im2 → 4) gateway → 5) deliver
  pids.push_back(startService(
      ROUTE_SERVER_BIN,
      {"--server.ip=127.0.0.1", "--server.port=" + std::to_string(routePort),
       "--redis.ip=" + redisIp, "--redis.port=" + std::to_string(redisPort),
       "--etcd.endpoints=" + etcd, "--service.name=" + routeSvc},
      logDir + "/route.log"));
  sleepMs(500);

  pids.push_back(startService(
      AUTH_SERVER_BIN,
      {"--server.ip=127.0.0.1", "--server.port=" + std::to_string(authPort),
       "--redis.ip=" + redisIp, "--redis.port=" + std::to_string(redisPort),
       "--etcd.endpoints=" + etcd, "--service.name=" + authSvc,
       "--mysql.host=" + mysqlHost, "--mysql.port=" + std::to_string(mysqlPort),
       "--mysql.user=root", "--mysql.password=", "--mysql.db=myrpc_im"},
      logDir + "/auth.log"));
  sleepMs(500);

  auto imArgs = [&](const std::string &id, uint64_t worker, uint16_t port) {
    return std::vector<std::string>{
        "--server.ip=127.0.0.1", "--server.port=" + std::to_string(port),
        "--server.id=" + id, "--worker.id=" + std::to_string(worker),
        "--etcd.endpoints=" + etcd, "--route.service=" + routeSvc,
        "--auth.service=" + authSvc, "--redis.ip=" + redisIp,
        "--redis.port=" + std::to_string(redisPort), "--mysql.host=" + mysqlHost,
        "--mysql.port=" + std::to_string(mysqlPort), "--mysql.user=root",
        "--mysql.password=", "--mysql.db=myrpc_im", "--shared.secret=devsecret"};
  };
  pids.push_back(startService(IM_SERVER_BIN, imArgs("IM1", 1, im1Port), logDir + "/im1.log"));
  sleepMs(300);
  pids.push_back(startService(IM_SERVER_BIN, imArgs("IM2", 2, im2Port), logDir + "/im2.log"));
  sleepMs(500);

  pids.push_back(startService(
      GATEWAY_SERVER_BIN,
      {"--client.ip=127.0.0.1", "--client.port=" + std::to_string(gwClientPort),
       "--rpc.ip=127.0.0.1", "--rpc.port=" + std::to_string(gwRpcPort),
       "--gateway.id=GW1", "--etcd.endpoints=" + etcd, "--im.service=ImService",
       "--auth.service=" + authSvc, "--shared.secret=devsecret"},
      logDir + "/gateway.log"));
  sleepMs(500);

  pids.push_back(startService(
      DELIVER_SERVER_BIN,
      {"--server.id=deliver", "--worker.id=0", "--etcd.endpoints=" + etcd,
       "--route.service=" + routeSvc, "--redis.ip=" + redisIp,
       "--redis.port=" + std::to_string(redisPort), "--consumer.name=d1"},
      logDir + "/deliver.log"));

  bool ready = testutil::waitForPort("127.0.0.1", routePort) &&
               testutil::waitForPort("127.0.0.1", authPort) &&
               testutil::waitForPort("127.0.0.1", im1Port) &&
               testutil::waitForPort("127.0.0.1", im2Port) &&
               testutil::waitForPort("127.0.0.1", gwClientPort);
  ready = ready && waitForImServices(
                       etcd, {std::string("127.0.0.1:") + std::to_string(im1Port),
                              std::string("127.0.0.1:") + std::to_string(im2Port)});
  sleepMs(1000);
  if (!ready) {
    std::printf("FAILED: some service did not become ready\n");
    return 1;
  }

  // 校验共享路由用。
  RedisClient redis;
  if (!redis.connect(redisIp, redisPort)) {
    std::printf("FAILED: test redis connect\n");
    return 1;
  }

  const std::string gw = "127.0.0.1";
  const int gwPort = gwClientPort;

  const std::string alice = "mn_alice_" + ts;
  const std::string bob = "mn_bob_" + ts;
  const std::string carol = "mn_carol_" + ts;

  // ========================================================================
  // S1 注册登录（请求经 gateway 轮询到两个节点）
  // ========================================================================
  SECTION("S1 注册登录 + 路由落在共享 Redis");
  PushQueue pa, pb, pb2, pc;
  Client a(&pa), b(&pb), b2(&pb2), c(&pc);
  std::vector<std::string> msgIds; // 收集各次发送的 msg_id，S7 据此解 worker_id

  {
    Client r;
    r.open(gw, gwPort); r.registerUser(alice, "pw"); r.close();
    r.open(gw, gwPort); r.registerUser(bob, "pw"); r.close();
    r.open(gw, gwPort); r.registerUser(carol, "pw"); r.close();
  }

  std::string atA, rtA, atB, rtB, atB2, rtB2;
  CHECK(onlineDevice(a, gw, gwPort, alice, "pw", "dev-a", 2, &atA, &rtA), "S1 alice 上线");
  CHECK(onlineDevice(b, gw, gwPort, bob, "pw", "dev-b", 2, &atB, &rtB), "S1 bob 上线(设备1)");

  // 核心断言：路由 + 反查索引都在共享 Redis，而非节点本地内存。
  CHECK(verifyRouteInRedis(redis, alice), "S1 alice 路由在共享 Redis");
  CHECK(verifyRouteInRedis(redis, bob), "S1 bob 路由在共享 Redis");

  // ========================================================================
  // S2 在线收发 + ACK（发消息节点与投递解耦，投递查共享路由）
  // ========================================================================
  SECTION("S2 在线收发 + ACK");
  CHECK(a.addFriend(bob), "S2 alice 加 bob");
  std::string m1;
  CHECK(a.send(bob, "hello", &m1), "S2 alice 发消息");
  msgIds.push_back(m1);
  CHECK(pb.waitChat(alice, "hello", 8000), "S2 bob 收到在线推送");
  CHECK(b.ack(m1, bob, alice), "S2 bob 回 ACK");
  CHECK(pa.waitAck(8000), "S2 alice 收到 DELIVERY_ACK");

  // ========================================================================
  // S3 多端扇出（bob 第二设备上线，路由在共享 Redis 两条 field）
  // ========================================================================
  SECTION("S3 多端扇出");
  CHECK(onlineDevice(b2, gw, gwPort, bob, "pw", "dev-b2", 3, &atB2, &rtB2), "S3 bob 上线(设备2)");
  CHECK(verifyRouteInRedis(redis, bob), "S3 bob 双端路由在共享 Redis");
  CHECK(a.send(bob, "fanout", &m1), "S3 发多端消息");
  msgIds.push_back(m1);
  CHECK(pb.waitChat(alice, "fanout", 8000), "S3 bob 设备1 收到");
  CHECK(pb2.waitChat(alice, "fanout", 8000), "S3 bob 设备2 收到");

  // ========================================================================
  // S4 离线补推（对端离线时发，上线后任意节点补推）
  // ========================================================================
  SECTION("S4 离线自动补推");
  CHECK(a.send(carol, "offline", &m1), "S4 发给离线 carol");
  msgIds.push_back(m1);
  std::string atC, rtC;
  CHECK(onlineDevice(c, gw, gwPort, carol, "pw", "dev-c", 2, &atC, &rtC), "S4 carol 上线");
  CHECK(pc.waitChat(alice, "offline", 8000), "S4 carol 收到离线补推");

  // ========================================================================
  // S5 跨节点单设备踢（发起节点与目标连接注册节点可不同）
  // ========================================================================
  SECTION("S5 跨节点单设备踢");
  std::vector<Client::Session> ss;
  CHECK(b.listSessions(&ss), "S5 bob ListSessions");
  CHECK(ss.size() == 2, "S5 bob 有两个会话");
  std::string webSid;
  for (const auto &s : ss) if (s.device_type == 3) webSid = s.session_id;
  CHECK(!webSid.empty(), "S5 找到设备2 session_id");
  CHECK(b.kickSession(webSid), "S5 踢设备2");
  std::string notice;
  CHECK(pb2.waitNotice(8000, &notice), "S5 设备2 收到系统通知");
  std::vector<Client::Session> ss2;
  CHECK(b.listSessions(&ss2), "S5 踢后 ListSessions");
  CHECK(ss2.size() == 1 && ss2[0].device_type == 2, "S5 只剩设备1");
  sleepMs(300);
  CHECK(pb.countNotice() == 0, "S5 设备1 未被误踢");

  // ========================================================================
  // S6 改密踢人（跨节点：改密节点吊销所有会话并踢所有在线连接）
  // ========================================================================
  SECTION("S6 改密踢人");
  CHECK(a.changePassword("newpw"), "S6 alice 改密");
  CHECK(pa.waitNotice(8000), "S6 alice 设备1 收到改密通知");
  {
    Client l; std::string at, rt, msg;
    l.open(gw, gwPort);
    CHECK(!l.login(alice, "pw", &at, &rt, &msg), "S6 旧密码登录失败");
    CHECK(l.login(alice, "newpw", &at, &rt, &msg), "S6 新密码登录成功");
    l.close();
  }

  // ========================================================================
  // S7 去 pin：两个节点都实际处理过请求
  // ========================================================================
  SECTION("S7 去 pin：两个节点都处理过 SendMessage");
  std::set<int> workerIds;
  for (const auto &m : msgIds)
    if (!m.empty()) workerIds.insert(workerIdOf(m));
  std::printf("    (各消息 msg_id 解出的 worker_id: ");
  for (int w : workerIds) std::printf("%d ", w);
  std::printf(")\n");
  CHECK(workerIds.count(1) && workerIds.count(2),
        "S7 两个节点都处理过 SendMessage（worker_id 含 1 和 2）");

  a.close(); b.close(); b2.close(); c.close();

  std::printf("\n========================================\n");
  if (g_fail == 0) std::printf("ALL MULTI-NODE TESTS PASSED\n");
  else std::printf("FAILURES: %d\n", g_fail);
  return g_fail ? 1 : 0;
}
