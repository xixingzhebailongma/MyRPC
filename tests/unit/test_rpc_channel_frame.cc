#include "rpc_channel.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

// 注意：必须定义在全局命名空间，才能匹配 rpc_channel.h 里的 friend 声明
struct RpcChannelFrameTest {
  static void run() {
    // 用 socketpair 建一条内存中的已连接通道，避免真实网络和线程
    int sv[2];
    assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) == 0);

    // 在另一端写入一个非法长度前缀（0xFFFFFFFF ≈ 4GB）的坏帧
    uint32_t bad = 0xFFFFFFFFu;
    char hdr[4];
    std::memcpy(hdr, &bad, 4);
    assert(::write(sv[1], hdr, 4) == 4);
    assert(::write(sv[1], "xx", 2) == 2);

    // friend 授权访问私有 readFrame；读到长度后应立即判非法并返回 false，
    // 且 payload 保持为空（说明没有发生 resize 巨量分配）
    RpcChannel ch("", 0, 1000);
    std::string payload;
    assert(!ch.readFrame(sv[0], payload));
    assert(payload.empty());

    ::close(sv[0]);
    ::close(sv[1]);
  }
};

int main() {
  RpcChannelFrameTest::run();
  std::printf("all tests passed\n");
  return 0;
}