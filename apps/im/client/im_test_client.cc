#include "im.pb.h"
#include "im_client_conn.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace im;

static std::string g_ip = "127.0.0.1";
static int g_port = 9001;
static const char *kStateFile = "/tmp/im_test_client.state";
static std::atomic<bool> g_running{true};

static void onSignal(int) { g_running = false; }

static void usage() {
  std::cout
      << "im_test_client <command> [args] [--server.ip=] [--server.port=]\n"
         "  register <user> <pass>\n"
         "  login <user> <pass> [device_id] [device_type]\n"
         "  issue-ticket\n"
         "  connect <ticket>          # 绑定后保持监听，收推送直到 Ctrl-C\n"
         "  send <to_user> <content>\n"
         "  friends\n"
         "  add-friend <friend_id>\n"
         "  passwd <new_password>\n"
         "  refresh\n"
         "  logout\n"
         "  pull-offline\n"
         "  spoof-send <to_user> <content>   # "
         "不登录直接伪造身份发消息（应被拒）\n";
}

static bool saveTokens(const std::string &at, const std::string &rt) {
  std::ofstream f(kStateFile, std::ios::trunc);
  if (!f)
    return false;
  f << "access_token =" << at << "\n";
  f << "refresh_toekn=" << rt << "\n";
  return true;
}

static bool loadTokens(std::string *at, std::string *rt) {
  std::ifstream f(kStateFile);
  if (!f)
    return false;
  std::string line;
  while (std::getline(f, line)) {
    if (line.rfind("access_token=", 0) == 0)
      *at = line.substr(13);
    else if (line.rfind("refresh_token=", 0) == 0)
      *rt = line.substr(14);
  }
  return !at->empty();
}

static void printPush(const std::string &payload) {
  ServerPushEnvelope env;
  if (!env.ParseFromString(payload)) {
    std::cout << "[push] <unparseable>\n";
    return;
  }
  switch (env.type()) {
  case ServerPushEnvelope::CHAT_MESSAGE: {
    ChatMessage m;
    if (m.ParseFromString(env.payload())) {
      std::cout << "[push] CHAT from=" << m.from_user_id()
                << " to=" << m.to_user_id() << " content=" << m.content()
                << "\n";
    }
    break;
  }
  case ServerPushEnvelope::DELIVERY_ACK: {
    MessageAck a;
    if (a.ParseFromString(env.payload())) {
      std::cout << "[push] ACK msg_id=" << a.msg_id()
                << " status=" << static_cast<int>(a.status()) << "\n";
    }
    break;
  }
  case ServerPushEnvelope::SYSTEM_NOTICE: {
    SystemNotice n;
    if (n.ParseFromString(env.payload())) {
      std::cout << "[push] NOTICE: " << n.message() << "\n";
    }
    break;
  }
  default:
    std::cout << "[push] type=" << static_cast<int>(env.type()) << "\n";
  }
}

// 用 access_token 换票 + 绑定连接（断线重连标准流程）
static bool bindViaTicket(ImClientConn &c, const std::string &at,
                          std::string *err) {
  IssueTicketRequest treq;
  treq.set_access_token(at);
  std::string tbody =
      c.call("ImService", "IssueTicket", treq.SerializeAsString());
  IssueTicketResponse tresp;
  if (!tresp.ParseFromString(tbody) || !tresp.success()) {
    *err = tresp.success() ? "issue-ticket parse error" : tresp.message();
    return false;
  }
  ConnectRequest creq;
  creq.set_ticket(tresp.ticket());
  std::string cbody = c.call("ImService", "Connect", creq.SerializeAsString());
  ConnectResponse cresp;
  if (!cresp.ParseFromString(cbody) || !cresp.success()) {
    *err = cresp.success() ? "connect parse error" : cresp.message();
    return false;
  }
  return true;
}

int main(int argc, char **argv) {
  // 解析 --server.ip / --server.port
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.rfind("--server.ip=", 0) == 0)
      g_ip = arg.substr(12);
    else if (arg.rfind("--server.port=", 0) == 0)
      g_port = std::stoi(arg.substr(14));
  }

  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--server.", 0) != 0)
      args.push_back(a);
  }
  if (args.empty()) {
    usage();
    return 1;
  }
  const std::string &cmd = args[0];

  // ---- register ----
  if (cmd == "register") {
    if (args.size() < 3) {
      std::cout << "usage: register <user> <pass>\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    RegisterRequest r;
    r.set_username(args[1]);
    r.set_password(args[2]);
    RegisterResponse resp;
    resp.ParseFromString(
        c.call("ImService", "Register", r.SerializeAsString()));
    std::cout << "register: success=" << resp.success() << " " << resp.message()
              << "\n";
    c.close();
    return 0;
  }

  // ---- login ----
  if (cmd == "login") {
    if (args.size() < 3) {
      std::cout << "usage: login <user> <pass> [device_id] [device_type]\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    LoginRequest r;
    r.set_username(args[1]);
    r.set_password(args[2]);
    if (args.size() >= 4)
      r.set_device_id(args[3]);
    if (args.size() >= 5)
      r.set_device_type(std::stoi(args[4]));
    LoginResponse resp;
    resp.ParseFromString(c.call("ImService", "Login", r.SerializeAsString()));
    std::cout << "login: success=" << resp.success() << " " << resp.message()
              << "\n";
    if (resp.success()) {
      std::cout << "  access_token=" << resp.access_token() << "\n";
      std::cout << "  refresh_token=" << resp.refresh_token() << "\n";
      std::cout << "  expires_in=" << resp.expires_in() << "\n";
      saveTokens(resp.access_token(), resp.refresh_token());
      std::cout << "  (tokens saved to " << kStateFile << ")\n";
    }
    c.close();
    return 0;
  }

  // ---- issue-ticket ----
  if (cmd == "issue-ticket") {
    std::string at, rt;
    if (!loadTokens(&at, &rt)) {
      std::cout << "no tokens; run login first\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    IssueTicketRequest r;
    r.set_access_token(at);
    IssueTicketResponse resp;
    resp.ParseFromString(
        c.call("ImService", "IssueTicket", r.SerializeAsString()));
    std::cout << "issue-ticket: success=" << resp.success()
              << " ticket=" << resp.ticket() << "\n";
    c.close();
    return 0;
  }

  // ---- connect（用票绑定并保持监听） ----
  if (cmd == "connect") {
    if (args.size() < 2) {
      std::cout << "usage: connect <ticket>\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    c.setPushHandler(printPush);
    ConnectRequest r;
    r.set_ticket(args[1]);
    ConnectResponse resp;
    resp.ParseFromString(c.call("ImService", "Connect", r.SerializeAsString()));
    std::cout << "connect: success=" << resp.success()
              << " user_id=" << resp.user_id() << " " << resp.message() << "\n";
    if (!resp.success()) {
      c.close();
      return 1;
    }
    signal(SIGINT, onSignal);
    signal(SIGTERM, onSignal);
    std::cout << "(listening, Ctrl-C to quit)\n";
    while (g_running)
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    c.close();
    return 0;
  }

  // ---- send（票据重连后发消息） ----
  if (cmd == "send") {
    if (args.size() < 3) {
      std::cout << "usage: send <to_user> <content>\n";
      return 1;
    }
    std::string at, rt;
    if (!loadTokens(&at, &rt)) {
      std::cout << "no tokens; run login first\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    std::string err;
    if (!bindViaTicket(c, at, &err)) {
      std::cout << "bind failed: " << err << "\n";
      c.close();
      return 1;
    }
    SendMessageRequest r;
    ChatMessage *m = r.mutable_msg();
    m->set_to_user_id(args[1]);
    m->set_content(args[2]);
    m->set_chat_type(0);
    m->set_from_user_id("__spoof_should_be_overridden__"); // 服务端会覆盖
    SendMessageResponse resp;
    resp.ParseFromString(
        c.call("ImService", "SendMessage", r.SerializeAsString()));
    std::cout << "send: success=" << resp.success()
              << " msg_id=" << resp.msg_id() << " " << resp.message() << "\n";
    c.close();
    return 0;
  }

  // ---- friends ----
  if (cmd == "friends") {
    std::string at, rt;
    if (!loadTokens(&at, &rt)) {
      std::cout << "no tokens; run login first\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    std::string err;
    if (!bindViaTicket(c, at, &err)) {
      std::cout << "bind failed: " << err << "\n";
      c.close();
      return 1;
    }
    GetFriendListRequest r; // user_id 留空，服务端从 conn 推导
    GetFriendListResponse resp;
    resp.ParseFromString(
        c.call("ImService", "GetFriendList", r.SerializeAsString()));
    std::cout << "friends: success=" << resp.success()
              << " count=" << resp.friends_size() << "\n";
    for (const auto &f : resp.friends())
      std::cout << "  " << f.user_id() << " online=" << f.is_online() << "\n";
    c.close();
    return 0;
  }

  // ---- add-friend ----
  if (cmd == "add-friend") {
    if (args.size() < 2) {
      std::cout << "usage: add-friend <friend_id>\n";
      return 1;
    }
    std::string at, rt;
    if (!loadTokens(&at, &rt)) {
      std::cout << "no tokens; run login first\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    std::string err;
    if (!bindViaTicket(c, at, &err)) {
      std::cout << "bind failed: " << err << "\n";
      c.close();
      return 1;
    }
    AddFriendRequest r;
    r.set_friend_id(args[1]); // user_id 服务端从 conn 推导
    AddFriendResponse resp;
    resp.ParseFromString(
        c.call("ImService", "AddFriend", r.SerializeAsString()));
    std::cout << "add-friend: success=" << resp.success() << " "
              << resp.message() << "\n";
    c.close();
    return 0;
  }

  // ---- passwd ----
  if (cmd == "passwd") {
    if (args.size() < 2) {
      std::cout << "usage: passwd <new_password>\n";
      return 1;
    }
    std::string at, rt;
    if (!loadTokens(&at, &rt)) {
      std::cout << "no tokens; run login first\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    std::string err;
    if (!bindViaTicket(c, at, &err)) {
      std::cout << "bind failed: " << err << "\n";
      c.close();
      return 1;
    }
    ChangePasswordRequest r;
    r.set_new_password(args[1]); // user_id 服务端从 conn 推导
    ChangePasswordResponse resp;
    resp.ParseFromString(
        c.call("ImService", "ChangePassword", r.SerializeAsString()));
    std::cout << "passwd: success=" << resp.success() << " " << resp.message()
              << "\n";
    c.close();
    return 0;
  }

  // ---- refresh ----
  if (cmd == "refresh") {
    std::string at, rt;
    if (!loadTokens(&at, &rt)) {
      std::cout << "no tokens; run login first\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    RefreshRequest r;
    r.set_refresh_token(rt);
    RefreshResponse resp;
    resp.ParseFromString(c.call("ImService", "Refresh", r.SerializeAsString()));
    std::cout << "refresh: success=" << resp.success() << " " << resp.message()
              << "\n";
    if (resp.success()) {
      std::cout << "  new access_token=" << resp.access_token() << "\n";
      std::cout << "  new refresh_token=" << resp.refresh_token() << "\n";
      saveTokens(resp.access_token(), resp.refresh_token());
    }
    c.close();
    return 0;
  }

  // ---- logout ----
  if (cmd == "logout") {
    std::string at, rt;
    if (!loadTokens(&at, &rt)) {
      std::cout << "no tokens; run login first\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    LogoutRequest r;
    r.set_access_token(at);
    LogoutResponse resp;
    resp.ParseFromString(c.call("ImService", "Logout", r.SerializeAsString()));
    std::cout << "logout: success=" << resp.success() << " " << resp.message()
              << "\n";
    c.close();
    return 0;
  }

  // ---- pull-offline ----
  if (cmd == "pull-offline") {
    std::string at, rt;
    if (!loadTokens(&at, &rt)) {
      std::cout << "no tokens; run login first\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    std::string err;
    if (!bindViaTicket(c, at, &err)) {
      std::cout << "bind failed: " << err << "\n";
      c.close();
      return 1;
    }
    PullOfflienMessagesRequest r; // user_id 服务端从 conn 推导
    PullOfflienMessagesResponse resp;
    resp.ParseFromString(
        c.call("ImService", "PullOfflineMessages", r.SerializeAsString()));
    std::cout << "pull-offline: success=" << resp.success()
              << " count=" << resp.messages_size() << "\n";
    for (const auto &m : resp.messages())
      std::cout << "  " << m.from_user_id() << ": " << m.content() << "\n";
    c.close();
    return 0;
  }

  // ---- spoof-send（不登录，直接伪造身份发消息，应被拒） ----
  if (cmd == "spoof-send") {
    if (args.size() < 3) {
      std::cout << "usage: spoof-send <to_user> <content>\n";
      return 1;
    }
    ImClientConn c;
    if (!c.connect(g_ip, g_port)) {
      std::cout << "connect failed\n";
      return 1;
    }
    // 故意不登录、不绑定，直接伪造身份发消息
    SendMessageRequest r;
    ChatMessage *m = r.mutable_msg();
    m->set_from_user_id("victim");
    m->set_to_user_id(args[1]);
    m->set_content(args[2]);
    m->set_chat_type(0);
    SendMessageResponse resp;
    resp.ParseFromString(
        c.call("ImService", "SendMessage", r.SerializeAsString()));
    std::cout << "spoof-send: success=" << resp.success() << " "
              << resp.message() << "\n";
    std::cout << "  (期望 success=false 且 message=unauthenticated)\n";
    c.close();
    return 0;
  }

  usage();
  return 1;
}