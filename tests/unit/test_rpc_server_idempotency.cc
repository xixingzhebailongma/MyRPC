// RpcServer 幂等去重的端到端集成：带 request_id 的请求经 claim/execute/
// complete 缓存后，同 key 重放不再执行 handler；非法 request_id 被拒；
// 结果型 handler 成功才缓存、失败可重试；caller_id 隔离不同调用方。
#include "rpc_channel.h"
#include "rpc_error_code.h"
#include "rpc_header.pb.h"
#include "rpc_protocol.h"
#include "rpc_server.h"
#include "service_manager.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>

namespace {

constexpr const char *kIp = "127.0.0.1";
constexpr const char *kService = "IdemService";

uint16_t pickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return 0;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }
  socklen_t len = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
    ::close(fd);
    return 0;
  }
  uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

bool waitForServer(uint16_t port) {
  for (int i = 0; i < 100; ++i) {
    RpcChannel ch(kIp, port);
    std::string resp;
    int32_t ec = 0;
    if (ch.Call(kService, "Ping", "ping", resp, ec) && ec == 0 &&
        resp == "pong") {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return false;
}

int fail(const char *msg) {
  std::fprintf(stderr, "FAILED: %s\n", msg);
  return 1;
}

} // namespace

int main() {
  uint16_t port = pickFreePort();
  if (port == 0)
    return fail("pickFreePort()");

  RpcServer server(kIp, port, 2, 4);

  // 普通 handler：每次执行自增计数，返回计数串，用于断言「是否真的重跑」。
  std::atomic<int> counter{0};
  server.serviceManager().registerMethod(
      kService, "Count", [&counter](const std::string &) {
        int n = counter.fetch_add(1) + 1;
        return std::to_string(n);
      });
  server.serviceManager().registerMethod(kService, "Ping",
                                         [](const std::string &) {
                                           return std::string("pong");
                                         });

  // 结果型 handler：成功（error_code=0）才缓存；失败不缓存、可重试。
  std::atomic<int> okCounter{0};
  std::atomic<int> failCounter{0};
  server.serviceManager().registerMethodWithResult(
      kService, "Ok", [&okCounter](const std::string &) -> RpcMethodResult {
        int n = okCounter.fetch_add(1) + 1;
        return {0, "ok-" + std::to_string(n)};
      });
  server.serviceManager().registerMethodWithResult(
      kService, "Flaky",
      [&failCounter](const std::string &) -> RpcMethodResult {
        int n = failCounter.fetch_add(1) + 1;
        return {42, "flaky-" + std::to_string(n)};
      });

  // caller_id 隔离：gateway_id 参与 cache key。
  server.setCallerIdExtractor(
      [](const RpcHeader &h) { return h.gateway_id(); });

  std::thread serverThread([&server] { server.start(); });
  if (!waitForServer(port)) {
    server.stop();
    serverThread.join();
    return fail("server did not become ready");
  }

  RpcChannel ch(kIp, port);

  // 1) 同 request_id 重放：handler 只执行一次，响应为缓存
  {
    std::string resp;
    int32_t ec = -1;
    if (!ch.Call(kService, "Count", "", resp, ec, -1, "req-1"))
      return fail("Count(req-1) transport failed");
    if (ec != 0 || resp != "1")
      return fail("Count(req-1) first call mismatch");
    if (!ch.Call(kService, "Count", "", resp, ec, -1, "req-1"))
      return fail("Count(req-1) replay transport failed");
    if (ec != 0 || resp != "1")
      return fail("Count(req-1) replay should return cached '1'");
    if (counter.load() != 1)
      return fail("Count handler re-executed on replay");
  }

  // 2) 不同 request_id：重新执行
  {
    std::string resp;
    int32_t ec = -1;
    if (!ch.Call(kService, "Count", "", resp, ec, -1, "req-2"))
      return fail("Count(req-2) transport failed");
    if (ec != 0 || resp != "2")
      return fail("Count(req-2) should execute and return '2'");
  }

  // 3) 非法 request_id（含空格）：拒绝，error_code=INVALID_REQUEST_ID，不执行
  {
    std::string resp;
    int32_t ec = 0;
    int before = counter.load();
    if (!ch.Call(kService, "Count", "", resp, ec, -1, "bad id!"))
      return fail("invalid request_id transport failed");
    if (ec != static_cast<int32_t>(RpcError::INVALID_REQUEST_ID))
      return fail("invalid request_id error_code != INVALID_REQUEST_ID");
    if (counter.load() != before)
      return fail("invalid request_id should not execute handler");
  }

  // 4) 结果型成功：缓存，重放不再执行
  {
    std::string resp;
    int32_t ec = -1;
    if (!ch.Call(kService, "Ok", "", resp, ec, -1, "req-ok"))
      return fail("Ok transport failed");
    if (ec != 0 || resp != "ok-1")
      return fail("Ok first call mismatch");
    if (!ch.Call(kService, "Ok", "", resp, ec, -1, "req-ok"))
      return fail("Ok replay transport failed");
    if (ec != 0 || resp != "ok-1")
      return fail("Ok replay should return cached 'ok-1'");
    if (okCounter.load() != 1)
      return fail("Ok handler re-executed on replay");
  }

  // 5) 结果型失败：不缓存，同 request_id 重试会真正重执行
  {
    std::string resp;
    int32_t ec = 0;
    if (!ch.Call(kService, "Flaky", "", resp, ec, -1, "req-flaky"))
      return fail("Flaky transport failed");
    if (ec != 42)
      return fail("Flaky error_code != 42");
    if (!ch.Call(kService, "Flaky", "", resp, ec, -1, "req-flaky"))
      return fail("Flaky retry transport failed");
    if (ec != 42)
      return fail("Flaky retry error_code != 42");
    if (failCounter.load() != 2)
      return fail("Flaky failure should re-execute on retry");
  }

  // 6) caller_id 隔离：同 request_id、不同 gateway_id 互不影响，各执行一次
  {
    std::string resp;
    int32_t ec = -1;
    RpcMessage a = buildRequest(kService, "Count", 0, "");
    a.mutable_header()->set_request_id("req-gw");
    a.mutable_header()->set_gateway_id("GW1");
    RpcMessage b = buildRequest(kService, "Count", 0, "");
    b.mutable_header()->set_request_id("req-gw");
    b.mutable_header()->set_gateway_id("GW2");
    if (!ch.CallMessage(a, resp, ec))
      return fail("GW1 Count transport failed");
    if (ec != 0)
      return fail("GW1 Count error_code != 0");
    if (!ch.CallMessage(b, resp, ec))
      return fail("GW2 Count transport failed");
    if (ec != 0)
      return fail("GW2 Count error_code != 0");
    int expected = counter.load();
    if (expected != 4) // req-1, req-2, GW1, GW2 各执行一次
      return fail("caller_id isolation did not execute both callers");
  }

  server.stop();
  serverThread.join();

  std::printf("PASSED\n");
  return 0;
}
