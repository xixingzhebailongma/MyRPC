// IM 业务级完整测试：在 test_im_e2e（happy path 全链路）之外，补齐业务边界与
// 多端/账号生命周期/好友/消息可靠性等场景。起 route/auth/im/gateway/deliver
// 五个真实服务进程，用 ImClientConn 驱动，覆盖：
//
//   S1 认证边界：重复注册 / 错误密码
//   S2 消息可靠性：在线推送 / ACK / 幂等去重 / 多端扇出 / 离线自动补推 + 显式拉取
//   S3 多端会话：ListSessions / KickSession（单设备踢）/ KickAllSessions
//   S4 同端互踢：同 device_type 二次登录吊销旧会话
//   S5 账号生命周期：ChangePassword / Refresh 轮换 / Logout
//   S6 好友边界 + 在线状态：加自己/加不存在/重复加、is_online 切换
//   S7 安全：未认证连接发消息被网关丢弃
//
// 前置：etcd / redis(7.x) / mysql 已就绪（apps/im/deploy/docker-compose.yml）。
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

// 全局失败计数：任一 CHECK 失败最终返回非 0，但不会中断后续场景，便于一次看全。
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

// 线程安全的推送收集器：push 回调在 ImClientConn 收帧线程里触发。
class PushQueue {
public:
  struct Item {
    ServerPushEnvelope::PayloadType type = ServerPushEnvelope::CHAT_MESSAGE;
    ChatMessage chat;
    MessageAck ack;
    SystemNotice notice;
  };

  void add(Item it) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      q_.push_back(std::move(it));
    }
    cv_.notify_all();
  }

  bool waitChat(const std::string &expect_from, const std::string &expect_content,
                int timeout_ms) {
    std::unique_lock<std::mutex> lk(mu_);
    return cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::CHAT_MESSAGE &&
            it.chat.from_user_id() == expect_from &&
            it.chat.content() == expect_content)
          return true;
      return false;
    });
  }

  bool waitAck(int timeout_ms) {
    std::unique_lock<std::mutex> lk(mu_);
    return cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::DELIVERY_ACK)
          return true;
      return false;
    });
  }

  // 等待一条系统通知（改密/踢人），并回填文案。
  bool waitNotice(int timeout_ms, std::string *text_out = nullptr) {
    std::unique_lock<std::mutex> lk(mu_);
    bool ok = cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms), [&] {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::SYSTEM_NOTICE)
          return true;
      return false;
    });
    if (ok && text_out) {
      for (const auto &it : q_)
        if (it.type == ServerPushEnvelope::SYSTEM_NOTICE) {
          *text_out = it.notice.message();
          break;
        }
    }
    return ok;
  }

  int countChat(const std::string &content) {
    std::lock_guard<std::mutex> lk(mu_);
    int n = 0;
    for (const auto &it : q_)
      if (it.type == ServerPushEnvelope::CHAT_MESSAGE &&
          it.chat.content() == content)
        ++n;
    return n;
  }

  int countNotice() {
    std::lock_guard<std::mutex> lk(mu_);
    int n = 0;
    for (const auto &it : q_)
      if (it.type == ServerPushEnvelope::SYSTEM_NOTICE)
        ++n;
    return n;
  }

  void clear() {
    std::lock_guard<std::mutex> lk(mu_);
    q_.clear();
  }

private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Item> q_;
};

// 一个 IM 客户端会话：封装 ImClientConn + 常用业务调用。
class Client {
public:
  struct Session {
    std::string session_id;
    std::string device_id;
    int device_type = 0;
  };

  explicit Client(PushQueue *q = nullptr) : q_(q) {}
  ~Client() { conn_.close(); }

  void setPushQueue(PushQueue *q) { q_ = q; }
  bool open(const std::string &ip, int port) { return conn_.connect(ip, port); }
  void close() { conn_.close(); }

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
      else if (env.type() == ServerPushEnvelope::SYSTEM_NOTICE)
        it.notice.ParseFromString(env.payload());
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
             std::string *rt, std::string *msg = nullptr,
             const std::string &device_id = "e2e-device", int device_type = 0) {
    LoginRequest r;
    r.set_username(u);
    r.set_password(p);
    r.set_device_id(device_id);
    r.set_device_type(device_type);
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
    std::string ticket;
    if (!issueTicket(at, &ticket, err))
      return false;
    ConnectRequest cr;
    cr.set_ticket(ticket);
    ConnectResponse cresp;
    if (!cresp.ParseFromString(rpc("Connect", cr.SerializeAsString())) ||
        !cresp.success()) {
      if (err)
        *err = cresp.message();
      return false;
    }
    return true;
  }

  bool issueTicket(const std::string &at, std::string *ticket = nullptr,
                   std::string *msg = nullptr) {
    IssueTicketRequest tr;
    tr.set_access_token(at);
    IssueTicketResponse tresp;
    if (!tresp.ParseFromString(rpc("IssueTicket", tr.SerializeAsString())) ||
        !tresp.success()) {
      if (msg)
        *msg = tresp.message();
      return false;
    }
    if (ticket)
      *ticket = tresp.ticket();
    return true;
  }

  bool addFriend(const std::string &fid, std::string *msg = nullptr) {
    AddFriendRequest r;
    r.set_friend_id(fid); // user_id 服务端从 conn 推导
    AddFriendResponse resp;
    bool ok = resp.ParseFromString(rpc("AddFriend", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    return ok && resp.success();
  }

  bool getFriendList(std::vector<FriendInfo> *friends) {
    GetFriendListRequest r;
    GetFriendListResponse resp;
    if (!resp.ParseFromString(rpc("GetFriendList", r.SerializeAsString())) ||
        !resp.success())
      return false;
    friends->clear();
    for (const auto &f : resp.friends())
      friends->push_back(f);
    return true;
  }

  bool send(const std::string &to, const std::string &content,
            std::string *msg_id = nullptr, std::string *msg = nullptr,
            const std::string &client_request_id = "") {
    SendMessageRequest r;
    ChatMessage *m = r.mutable_msg();
    m->set_to_user_id(to);
    m->set_content(content);
    m->set_chat_type(0);
    r.set_client_request_id(client_request_id.empty() ? generateRequestId()
                                                       : client_request_id);
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
    a->set_from_user_id(from);
    a->set_to_user_id(to);
    a->set_status(im::MessageStatus::DELIVERED);
    AckMessageResponse resp;
    return resp.ParseFromString(rpc("AckMessage", r.SerializeAsString())) &&
           resp.success();
  }

  bool listSessions(std::vector<Session> *out, std::string *msg = nullptr) {
    ListSessionsRequest r; // user_id 服务端从 conn 推导
    ListSessionsResponse resp;
    if (!resp.ParseFromString(rpc("ListSessions", r.SerializeAsString())) ||
        !resp.success()) {
      if (msg)
        *msg = resp.message();
      return false;
    }
    out->clear();
    for (const auto &s : resp.sessions())
      out->push_back({s.session_id(), s.device_id(),
                      static_cast<int>(s.device_type())});
    return true;
  }

  bool kickSession(const std::string &session_id, std::string *msg = nullptr) {
    KickSessionRequest r;
    r.set_session_id(session_id);
    KickSessionResponse resp;
    bool ok = resp.ParseFromString(rpc("KickSession", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    return ok && resp.success();
  }

  bool kickAllSessions(int *kicked = nullptr, std::string *msg = nullptr) {
    KickAllSessionsRequest r;
    KickAllSessionsResponse resp;
    bool ok = resp.ParseFromString(rpc("KickAllSessions", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    if (!ok || !resp.success())
      return false;
    if (kicked)
      *kicked = resp.kicked();
    return true;
  }

  bool changePassword(const std::string &newp, std::string *msg = nullptr) {
    ChangePasswordRequest r;
    r.set_new_password(newp); // user_id 服务端从 conn 推导
    ChangePasswordResponse resp;
    bool ok = resp.ParseFromString(rpc("ChangePassword", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    return ok && resp.success();
  }

  bool refresh(const std::string &rt, std::string *new_at = nullptr,
               std::string *new_rt = nullptr, std::string *msg = nullptr) {
    RefreshRequest r;
    r.set_refresh_token(rt);
    RefreshResponse resp;
    bool ok = resp.ParseFromString(rpc("Refresh", r.SerializeAsString()));
    if (msg)
      *msg = resp.message();
    if (!ok || !resp.success())
      return false;
    if (new_at)
      *new_at = resp.access_token();
    if (new_rt)
      *new_rt = resp.refresh_token();
    return true;
  }

  bool logout(const std::string &at) {
    LogoutRequest r;
    r.set_access_token(at);
    LogoutResponse resp;
    return resp.ParseFromString(rpc("Logout", r.SerializeAsString())) &&
           resp.success();
  }

  bool pullOffline(std::vector<ChatMessage> *out) {
    PullOfflienMessagesRequest r; // user_id 服务端从 conn 推导
    PullOfflienMessagesResponse resp;
    if (!resp.ParseFromString(rpc("PullOfflineMessages", r.SerializeAsString())) ||
        !resp.success())
      return false;
    out->clear();
    for (const auto &m : resp.messages())
      out->push_back(m);
    return true;
  }

private:
  ImClientConn conn_;
  PushQueue *q_ = nullptr;
};

// 便捷：open + installPush + login + bind，返回 access_token（失败返回空）。
// 一条连接贯穿整个设备生命周期（login 是 pre-auth，bind 在同一连接上完成绑定）。
bool onlineDevice(Client &c, const std::string &gw_ip, int gw_port,
                  const std::string &u, const std::string &p,
                  const std::string &device_id, int device_type,
                  std::string *at, std::string *rt, std::string *err = nullptr) {
  if (!c.open(gw_ip, gw_port))
    return false;
  c.installPush();
  if (!c.login(u, p, at, rt, err, device_id, device_type))
    return false;
  return c.bind(*at, err);
}

// 轮询好友在线状态直到期望值（在线状态经 Route 事件广播异步收敛）。
bool waitFriendOnline(Client &c, const std::string &fid, bool want,
                      int timeout_ms = 8000) {
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    std::vector<FriendInfo> fs;
    if (c.getFriendList(&fs)) {
      for (const auto &f : fs)
        if (f.user_id() == fid && f.is_online() == want)
          return true;
    }
    sleepMs(200);
  }
  return false;
}

// 启动一个服务进程，返回 pid；失败返回 -1。
pid_t startService(const std::string &bin, const std::vector<std::string> &args,
                   const std::string &log) {
  std::vector<std::string> argv{bin};
  argv.insert(argv.end(), args.begin(), args.end());
  return testutil::spawn(argv, log);
}

// 等待 "ImService" 在 etcd 里稳定为恰好 1 个节点且地址 == myAddr。
bool waitForImService(const std::string &etcd, const std::string &myAddr,
                      int timeout_ms = 60000) {
  ServiceDiscovery sd(etcd);
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    auto nodes = sd.discover("ImService");
    if (nodes && nodes->size() == 1 && (*nodes)[0].address() == myAddr)
      return true;
    sleepMs(500);
  }
  return false;
}

} // namespace

int main() {
  const std::string etcd = envStr("RPC_TEST_ETCD", "http://127.0.0.1:2379");
  const std::string redisIp = envStr("RPC_TEST_REDIS_IP", "127.0.0.1");
  const int redisPort = std::atoi(envStr("RPC_TEST_REDIS_PORT", "6379").c_str());
  const std::string mysqlHost = envStr("RPC_TEST_MYSQL_HOST", "127.0.0.1");
  const int mysqlPort = std::atoi(envStr("RPC_TEST_MYSQL_PORT", "3306").c_str());

  // 前置依赖探测：缺任一直接 SKIP。
  if (!testutil::etcdReachable(etcd) ||
      !testutil::tcpPortOpen(redisIp, static_cast<uint16_t>(redisPort)) ||
      !testutil::tcpPortOpen(mysqlHost, static_cast<uint16_t>(mysqlPort))) {
    std::printf("test_im_business: SKIP (etcd/redis/mysql not ready)\n");
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
  if (!routePort || !authPort || !imPort || !gwClientPort || !gwRpcPort) {
    std::printf("FAILED: pickFreePort()\n");
    return 1;
  }

  const std::string logDir = "/tmp/myrpc_biz_" + ts;
  std::system(("mkdir -p " + logDir).c_str());
  std::printf("logs: %s\n", logDir.c_str());

  std::vector<pid_t> pids;
  auto stopAll = [&]() {
    for (auto p : pids)
      testutil::stopProcess(p);
    // KEEP_BIZ_LOGS=1 保留日志目录用于排查（默认删除）。
    if (!std::getenv("KEEP_BIZ_LOGS"))
      std::system(("rm -rf " + logDir).c_str());
  };
  struct Cleanup {
    std::function<void()> fn;
    ~Cleanup() { fn(); }
  } cleanup{stopAll};

  // 1) route → 2) auth → 3) im → 4) gateway → 5) deliver
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

  pids.push_back(startService(
      IM_SERVER_BIN,
      {"--server.ip=127.0.0.1", "--server.port=" + std::to_string(imPort),
       "--server.id=IM1", "--worker.id=1", "--etcd.endpoints=" + etcd,
       "--route.service=" + routeSvc, "--auth.service=" + authSvc,
       "--redis.ip=" + redisIp, "--redis.port=" + std::to_string(redisPort),
       "--mysql.host=" + mysqlHost, "--mysql.port=" + std::to_string(mysqlPort),
       "--mysql.user=root", "--mysql.password=", "--mysql.db=myrpc_im",
       "--shared.secret=devsecret"},
      logDir + "/im.log"));
  sleepMs(500);

  pids.push_back(startService(
      GATEWAY_SERVER_BIN,
      {"--client.ip=127.0.0.1", "--client.port=" + std::to_string(gwClientPort),
       "--rpc.ip=127.0.0.1", "--rpc.port=" + std::to_string(gwRpcPort),
       "--gateway.id=GW1", "--etcd.endpoints=" + etcd,
       "--im.service=ImService", "--auth.service=" + authSvc,
       "--shared.secret=devsecret"},
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
               testutil::waitForPort("127.0.0.1", imPort) &&
               testutil::waitForPort("127.0.0.1", gwClientPort);
  ready = ready && waitForImService(etcd, std::string("127.0.0.1:") +
                                               std::to_string(imPort));
  sleepMs(1000);
  if (!ready) {
    std::printf("FAILED: some service did not become ready\n");
    return 1;
  }

  const std::string gw = "127.0.0.1";
  const int gwPort = gwClientPort;

  // ========================================================================
  // S1 认证边界
  // ========================================================================
  SECTION("S1 认证边界（重复注册 / 错误密码）");
  {
    const std::string u = "biz_dup_" + ts;
    Client c;
    CHECK(c.open(gw, gwPort), "S1 open");
    std::string msg;
    CHECK(c.registerUser(u, "pw123", &msg), "S1 register ok");
    c.close();

    Client c2;
    c2.open(gw, gwPort);
    CHECK(!c2.registerUser(u, "pw123", &msg), "S1 重复注册被拒");
    CHECK(msg.find("already exists") != std::string::npos,
          "S1 重复注册文案 username already exists");
    c2.close();

    Client c3;
    c3.open(gw, gwPort);
    std::string at, rt;
    CHECK(!c3.login(u, "wrong", &at, &rt, &msg), "S1 错误密码登录失败");
    CHECK(msg.find("invalid username or password") != std::string::npos,
          "S1 错误密码文案");
    c3.close();
  }

  // ========================================================================
  // S2 消息可靠性（幂等 / 多端扇出 / 离线）
  // ========================================================================
  SECTION("S2 消息可靠性（在线推送 / ACK / 幂等 / 多端 / 离线）");
  {
    const std::string alice = "biz_alice_" + ts;
    const std::string bob = "biz_bob_" + ts;
    const std::string carol = "biz_carol_" + ts;

    PushQueue pa, pb, pb2, pc;
    Client a(&pa), b(&pb), b2(&pb2), c(&pc);

    // 注册（register 不需要登录态）
    {
      Client r;
      r.open(gw, gwPort);
      r.registerUser(alice, "pw");
      r.close();
      r.open(gw, gwPort);
      r.registerUser(bob, "pw");
      r.close();
      r.open(gw, gwPort);
      r.registerUser(carol, "pw");
      r.close();
    }

    std::string atA, rtA, atB, rtB, atB2, rtB2;
    CHECK(onlineDevice(a, gw, gwPort, alice, "pw", "dev-a", 2, &atA, &rtA),
          "S2 alice 上线");
    CHECK(onlineDevice(b, gw, gwPort, bob, "pw", "dev-b", 2, &atB, &rtB),
          "S2 bob 上线(设备1)");

    // 加好友
    CHECK(a.addFriend(bob), "S2 alice 加 bob");

    // 在线推送 + ACK
    std::string m1;
    CHECK(a.send(bob, "hello", &m1), "S2 alice 发消息");
    CHECK(!m1.empty(), "S2 msg_id 非空");
    CHECK(pb.waitChat(alice, "hello", 8000), "S2 bob 收到在线推送");
    CHECK(b.ack(m1, bob, alice), "S2 bob 回 ACK");
    CHECK(pa.waitAck(8000), "S2 alice 收到 DELIVERY_ACK");

    // 幂等去重：同 client_request_id 重发 → 同 msg_id 且不重复投递
    std::string rid = "biz-dedup-" + ts;
    std::string md1, md2;
    CHECK(a.send(bob, "dedup", &md1, nullptr, rid), "S2 首次发送(带 request_id)");
    CHECK(pb.waitChat(alice, "dedup", 8000), "S2 首次投递送达");
    CHECK(a.send(bob, "dedup", &md2, nullptr, rid), "S2 重发同 request_id");
    CHECK(md2 == md1, "S2 重发返回相同 msg_id（幂等）");
    sleepMs(600);
    CHECK(pb.countChat("dedup") == 1, "S2 收端只收到一条（无重复投递）");

    // 多端扇出：bob 第二设备（不同 device_type）上线，消息扇出到两端
    CHECK(onlineDevice(b2, gw, gwPort, bob, "pw", "dev-b2", 3, &atB2, &rtB2),
          "S2 bob 上线(设备2)");
    {
      std::vector<Client::Session> ss;
      CHECK(b.listSessions(&ss), "S2 bob ListSessions");
      CHECK(ss.size() == 2, "S2 bob 有两个会话");
    }
    CHECK(a.send(bob, "fanout", &m1), "S2 发多端消息");
    CHECK(pb.waitChat(alice, "fanout", 8000), "S2 bob 设备1 收到");
    CHECK(pb2.waitChat(alice, "fanout", 8000), "S2 bob 设备2 收到");

    // 离线自动补推：alice 发给未上线的 carol
    CHECK(a.send(carol, "offline-1", &m1), "S2 发给离线 carol");
    std::string atC, rtC;
    CHECK(onlineDevice(c, gw, gwPort, carol, "pw", "dev-c", 2, &atC, &rtC),
          "S2 carol 上线");
    CHECK(pc.waitChat(alice, "offline-1", 8000), "S2 carol 收到离线自动补推");
    // 显式拉取：自动补推后队列应已清空，不再重复
    {
      std::vector<ChatMessage> msgs;
      CHECK(c.pullOffline(&msgs), "S2 carol 显式 pull-offline");
      CHECK(msgs.empty(), "S2 补推后离线队列为空（不重复下发）");
    }

    a.close();
    b.close();
    b2.close();
    c.close();
  }

  // ========================================================================
  // S3 多端会话管理（ListSessions / KickSession / KickAllSessions）
  // ========================================================================
  SECTION("S3 多端会话管理（单设备踢 / 全量踢）");
  {
    const std::string u = "biz_multi_" + ts;
    PushQueue p1, p2;
    Client dev1(&p1), dev2(&p2);

    {
      Client r;
      r.open(gw, gwPort);
      r.registerUser(u, "pw");
      r.close();
    }

    std::string at1, rt1, at2, rt2;
    CHECK(onlineDevice(dev1, gw, gwPort, u, "pw", "android", 2, &at1, &rt1),
          "S3 设备1(android) 上线");
    CHECK(onlineDevice(dev2, gw, gwPort, u, "pw", "web", 3, &at2, &rt2),
          "S3 设备2(web) 上线");

    std::vector<Client::Session> ss;
    CHECK(dev1.listSessions(&ss), "S3 ListSessions");
    CHECK(ss.size() == 2, "S3 两个会话");

    // 找出 web 会话并踢掉
    std::string webSid;
    for (const auto &s : ss)
      if (s.device_type == 3)
        webSid = s.session_id;
    CHECK(!webSid.empty(), "S3 找到 web session_id");
    CHECK(dev1.kickSession(webSid), "S3 踢 web 会话");

    std::string notice;
    CHECK(p2.waitNotice(8000, &notice), "S3 web 端收到系统通知");
    CHECK(!notice.empty(), "S3 踢人通知文案非空");

    // android 端不受影响：仍能 ListSessions 且只剩自己
    std::vector<Client::Session> ss2;
    CHECK(dev1.listSessions(&ss2), "S3 踢后 ListSessions");
    CHECK(ss2.size() == 1 && ss2[0].device_type == 2, "S3 只剩 android 会话");
    sleepMs(300);
    CHECK(p1.countNotice() == 0, "S3 android 端未被误踢");

    // 全量踢：android 是唯一剩余会话，踢人后自身也收到通知并被 force_close。
    // 注意：调用方自己被踢后连接已关闭，无法再用它 listSessions，故只校验
    // 吊销数 + 通知送达（通知送达本身即证明 force_close 生效）。
    int kicked = -1;
    CHECK(dev1.kickAllSessions(&kicked), "S3 KickAllSessions");
    CHECK(kicked == 1, "S3 全量踢吊销 1 个会话");
    CHECK(p1.waitNotice(8000), "S3 android 端收到系统通知");

    dev1.close();
    dev2.close();
  }

  // ========================================================================
  // S4 同端互踢
  // ========================================================================
  SECTION("S4 同端互踢（同 device_type 二次登录吊销旧会话）");
  {
    const std::string u = "biz_samedev_" + ts;
    {
      Client r;
      r.open(gw, gwPort);
      r.registerUser(u, "pw");
      r.close();
    }

    Client dev1, dev2;
    std::string at1, rt1, at2, rt2;
    dev1.open(gw, gwPort);
    CHECK(dev1.login(u, "pw", &at1, &rt1, nullptr, "same-dev", 2),
          "S4 设备A 登录(device_type=2)");
    dev2.open(gw, gwPort);
    CHECK(dev2.login(u, "pw", &at2, &rt2, nullptr, "same-dev", 2),
          "S4 设备B 登录(同 device_type=2)");

    // 旧会话被吊销：旧 access_token 无法再换票
    std::string msg;
    CHECK(!dev1.issueTicket(at1, nullptr, &msg), "S4 旧 access_token 已失效");

    // 新会话可用，且 ListSessions 只有 1 个会话
    CHECK(dev2.bind(at2), "S4 设备B 可正常绑定");
    std::vector<Client::Session> ss;
    CHECK(dev2.listSessions(&ss), "S4 设备B ListSessions");
    CHECK(ss.size() == 1 && ss[0].device_type == 2, "S4 仅剩 1 个会话");

    dev1.close();
    dev2.close();
  }

  // ========================================================================
  // S5 账号生命周期（ChangePassword / Refresh / Logout）
  // ========================================================================
  SECTION("S5 账号生命周期（改密 / 续期 / 登出）");

  // S5a 改密：全端踢 + 旧密码失效 + 新密码生效 + 旧 token 失效
  {
    const std::string u = "biz_chpwd_" + ts;
    PushQueue p1, p2;
    Client c1(&p1), c2(&p2);
    {
      Client r;
      r.open(gw, gwPort);
      r.registerUser(u, "oldpw");
      r.close();
    }
    std::string at1, rt1, at2, rt2;
    CHECK(onlineDevice(c1, gw, gwPort, u, "oldpw", "dev1", 2, &at1, &rt1),
          "S5a 设备1 上线");
    CHECK(onlineDevice(c2, gw, gwPort, u, "oldpw", "dev2", 3, &at2, &rt2),
          "S5a 设备2 上线");

    CHECK(c1.changePassword("newpw"), "S5a 改密成功");
    CHECK(p1.waitNotice(8000), "S5a 设备1 收到改密通知");
    CHECK(p2.waitNotice(8000), "S5a 设备2 收到改密通知");

    // 旧密码登录失败，新密码登录成功
    {
      Client l;
      std::string at, rt, msg;
      l.open(gw, gwPort);
      bool okOld = l.login(u, "oldpw", &at, &rt, &msg);
      CHECK(!okOld, "S5a 旧密码登录失败");
      CHECK(okOld == false && l.login(u, "newpw", &at, &rt, &msg),
            "S5a 新密码登录成功");
      l.close();
    }
    // 旧 access_token 已被吊销
    {
      std::string msg;
      CHECK(!c1.issueTicket(at1, nullptr, &msg), "S5a 旧 access_token 失效");
    }
    c1.close();
    c2.close();
  }

  // S5b 续期：token 轮换
  {
    const std::string u = "biz_refresh_" + ts;
    Client c;
    {
      Client r;
      r.open(gw, gwPort);
      r.registerUser(u, "pw");
      r.close();
    }
    std::string at1, rt1;
    c.open(gw, gwPort);
    CHECK(c.login(u, "pw", &at1, &rt1), "S5b 登录");

    std::string at2, rt2;
    CHECK(c.refresh(rt1, &at2, &rt2), "S5b 续期成功");
    CHECK(!at2.empty() && !rt2.empty() && at2 != at1, "S5b 返回新 token");
    // 旧 refresh_token 轮换后失效
    std::string msg;
    CHECK(!c.refresh(rt1, nullptr, nullptr, &msg), "S5b 旧 refresh_token 失效");
    // 旧 access_token 失效，新 access_token 有效
    CHECK(!c.issueTicket(at1, nullptr, &msg), "S5b 旧 access_token 失效");
    CHECK(c.issueTicket(at2), "S5b 新 access_token 可换票");
    c.close();
  }

  // S5c 登出：会话吊销
  {
    const std::string u = "biz_logout_" + ts;
    Client c;
    {
      Client r;
      r.open(gw, gwPort);
      r.registerUser(u, "pw");
      r.close();
    }
    std::string at, rt;
    CHECK(onlineDevice(c, gw, gwPort, u, "pw", "dev", 2, &at, &rt),
          "S5c 上线");
    CHECK(c.logout(at), "S5c 登出成功");
    std::string msg;
    CHECK(!c.issueTicket(at, nullptr, &msg), "S5c 登出后 token 失效");
    c.close();
  }

  // ========================================================================
  // S6 好友边界 + 在线状态
  // ========================================================================
  SECTION("S6 好友边界 + 在线状态");
  {
    const std::string u = "biz_friend_" + ts;
    const std::string other = "biz_friend2_" + ts;
    const std::string ghost = "biz_nobody_" + ts;

    PushQueue pu;
    Client cu(&pu);
    {
      Client r;
      r.open(gw, gwPort);
      r.registerUser(u, "pw");
      r.close();
      r.open(gw, gwPort);
      r.registerUser(other, "pw");
      r.close();
    }

    std::string at, rt;
    CHECK(onlineDevice(cu, gw, gwPort, u, "pw", "dev", 2, &at, &rt),
          "S6 用户上线");

    std::string msg;
    CHECK(!cu.addFriend(u, &msg) &&
              msg.find("cannot add self") != std::string::npos,
          "S6 加自己被拒");
    CHECK(!cu.addFriend(ghost, &msg) &&
              msg.find("user not found") != std::string::npos,
          "S6 加不存在用户被拒");
    CHECK(cu.addFriend(other), "S6 加好友成功");
    CHECK(!cu.addFriend(other, &msg) &&
              msg.find("already friends") != std::string::npos,
          "S6 重复加好友被拒");

    // 在线状态切换：other 离线 -> 在线 -> 离线
    CHECK(waitFriendOnline(cu, other, false), "S6 初始 other 离线");
    {
      PushQueue po;
      Client co(&po);
      std::string ato, rto;
      CHECK(onlineDevice(co, gw, gwPort, other, "pw", "dev-o", 2, &ato, &rto),
            "S6 other 上线");
      CHECK(waitFriendOnline(cu, other, true), "S6 other 上线后 is_online=true");
      co.close();
    }
    CHECK(waitFriendOnline(cu, other, false), "S6 other 下线后 is_online=false");

    cu.close();
  }

  // ========================================================================
  // S7 安全：未认证连接发消息被网关丢弃
  // ========================================================================
  SECTION("S7 未认证请求被网关丢弃");
  {
    Client raw;
    CHECK(raw.open(gw, gwPort), "S7 连接网关");
    SendMessageRequest r;
    r.mutable_msg()->set_from_user_id("victim");
    r.mutable_msg()->set_to_user_id("biz_alice_" + ts);
    r.mutable_msg()->set_content("unauth");
    r.mutable_msg()->set_chat_type(0);
    std::string resp = raw.rpc("SendMessage", r.SerializeAsString(), 1500);
    CHECK(resp.empty(), "S7 未认证 SendMessage 无响应（被丢弃）");
    raw.close();
  }

  std::printf("\n========================================\n");
  if (g_fail == 0) {
    std::printf("ALL BUSINESS TESTS PASSED\n");
  } else {
    std::printf("FAILURES: %d\n", g_fail);
  }
  return g_fail ? 1 : 0;
}
