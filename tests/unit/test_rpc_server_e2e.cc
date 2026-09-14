#include "rpc_channel.h"
#include "rpc_client.h"
#include "rpc_error_code.h"
#include "rpc_server.h"
#include "service_manager.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>

namespace {

constexpr const char *kIp = "127.0.0.1";
constexpr const char *kService = "EchoService";

// 找一个空闲的本地回环端口：bind 到 :0 让内核分配，读出端口后关闭。
// 存在轻微竞态（关闭到 RpcServer 绑定之间可能被占用），测试可接受。
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

// 反复调 Ping 直到成功，确认服务器已开始 accept（start() 在后台线程跑）。
bool waitForServer(uint16_t port) {
  for (int i = 0; i < 100; ++i) {
    RpcClient client(kIp, port);
    std::string resp;
    int32_t ec = 0;
    if (client.Call(kService, "Ping", "ping", resp, ec) && ec == 0 &&
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
  auto &mgr = server.serviceManager();
  mgr.registerMethod(kService, "Ping",
                     [](const std::string &) { return std::string("pong"); });
  mgr.registerMethod(kService, "Echo",
                     [](const std::string &b) { return b; });
  mgr.registerMethodWithResult(kService, "Busy",
                               [](const std::string &) -> RpcMethodResult {
                                 return {42, "busy"};
                               });
  mgr.registerMethod(kService, "Slow", [](const std::string &) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    return std::string("late");
  });

  std::thread serverThread([&server] { server.start(); });

  if (!waitForServer(port)) {
    server.stop();
    serverThread.join();
    return fail("server did not become ready");
  }

  // 1) 成功回环
  {
    RpcClient client(kIp, port);
    std::string resp;
    int32_t ec = -1;
    if (!client.Call(kService, "Echo", "hello", resp, ec))
      return fail("echo transport failed");
    if (ec != 0)
      return fail("echo error_code != 0");
    if (resp != "hello")
      return fail("echo response mismatch");
  }

  // 2) 业务错误码透传（Result 型 handler）
  {
    RpcClient client(kIp, port);
    std::string resp;
    int32_t ec = -1;
    if (!client.Call(kService, "Busy", "", resp, ec))
      return fail("busy transport failed");
    if (ec != 42)
      return fail("busy error_code != 42");
    if (resp != "busy")
      return fail("busy body mismatch");
  }

  // 3) 方法不存在：transport 成功、error_code == -1
  {
    RpcClient client(kIp, port);
    std::string resp;
    int32_t ec = 0;
    if (!client.Call(kService, "NoSuch", "", resp, ec))
      return fail("method-not-found transport failed");
    if (ec != -1)
      return fail("method-not-found error_code != -1");
  }

  // 4) 超时：慢 handler + 短超时（RpcClient 不暴露 timeout，用 RpcChannel）
  {
    RpcChannel ch(kIp, port, /*timeout_ms=*/100, /*connect_timeout_ms=*/1000);
    std::string resp;
    int32_t ec = 0;
    if (ch.Call(kService, "Slow", "", resp, ec, 100))
      return fail("slow call unexpectedly succeeded");
    if (ec != static_cast<int32_t>(RpcError::TIMEOUT))
      return fail("slow call error_code != TIMEOUT");
  }

  // 5) 大包回环（~1MB，含 \0 与高位字节，验证二进制安全 + 分帧）
  {
    std::string big(1024 * 1024, 'x');
    for (size_t i = 0; i < big.size(); i += 997)
      big[i] = static_cast<char>(i % 251); // 覆盖 0~250 全部字节值
    RpcClient client(kIp, port);
    std::string resp;
    int32_t ec = -1;
    if (!client.Call(kService, "Echo", big, resp, ec))
      return fail("big echo transport failed");
    if (ec != 0)
      return fail("big echo error_code != 0");
    if (resp != big)
      return fail("big echo response mismatch");
  }

  server.stop();
  serverThread.join();

  std::printf("PASSED\n");
  return 0;
}
