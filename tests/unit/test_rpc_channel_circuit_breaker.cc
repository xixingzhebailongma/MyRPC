// RpcChannel 熔断器 + 重连退避回归测试。
//
// 验证：连续失败达到阈值后进入熔断态；熔断期内 Call 走 fail-fast 不触发
// connect（失败计数不再增长）；recordSuccess 复位熔断。

#include "rpc_channel.h"

#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

namespace {

// 找一个「确定无人监听」的本地端口：bind port 0 拿到端口后立即关闭，
// 之后 connect 该端口会立刻 ECONNREFUSED。
int pickDeadPort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return -1;
  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(0);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (::bind(fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) <
      0) {
    ::close(fd);
    return -1;
  }
  socklen_t alen = sizeof(addr);
  if (::getsockname(fd, reinterpret_cast<struct sockaddr *>(&addr), &alen) <
      0) {
    ::close(fd);
    return -1;
  }
  int port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

} // namespace

int main() {
  int port = pickDeadPort();
  if (port < 0) {
    perror("pickDeadPort");
    return 1;
  }
  const int kTimeoutMs = 2000; // 死端口会 ECONNREFUSED 秒回，超时只是兜底

  RpcChannel ch("127.0.0.1", static_cast<uint16_t>(port), kTimeoutMs);

  // 前 3 次失败：每次 connect 都 ECONNREFUSED，recordFailure 累计计数。
  for (int i = 0; i < 3; ++i) {
    std::string body;
    int32_t err = -1;
    if (ch.Call("Echo", "Echo", "hello", body, err)) {
      fprintf(stderr, "FAILED: 对死端口 Call 竟然成功\n");
      return 1;
    }
  }

  if (!ch.isCircuitOpen()) {
    fprintf(stderr, "FAILED: 连续失败 3 次后未熔断\n");
    return 1;
  }
  printf("连续失败 3 次后进入熔断态（consecutiveFailures=%d）\n",
         ch.consecutiveFailures());

  // 熔断期 Call 应 fail-fast：失败计数保持 3（证明没再 connect、没再记失败）。
  {
    std::string body;
    int32_t err = -1;
    if (ch.Call("Echo", "Echo", "hello", body, err)) {
      fprintf(stderr, "FAILED: 熔断期 Call 返回成功\n");
      return 1;
    }
    if (ch.consecutiveFailures() != 3) {
      fprintf(stderr, "FAILED: 熔断期 Call 未走 fail-fast（失败计数变成 %d）\n",
              ch.consecutiveFailures());
      return 1;
    }
    printf("熔断期快速失败：失败计数保持 %d（未重复 connect）\n",
           ch.consecutiveFailures());
  }

  // recordSuccess 复位熔断。
  ch.recordSuccess();
  if (ch.isCircuitOpen() || ch.consecutiveFailures() != 0) {
    fprintf(stderr, "FAILED: recordSuccess 未复位熔断\n");
    return 1;
  }
  printf("recordSuccess 后熔断复位（consecutiveFailures=%d）\n",
         ch.consecutiveFailures());

  ch.close();
  printf("PASSED\n");
  return 0;
}