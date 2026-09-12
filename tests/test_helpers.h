#pragma once
// 集成测试共享工具：端口选择、etcd 探测、子进程编排（spawn/stop）、TCP 就绪探测。
// 供需要起真实 IM 服务二进制（gateway/route/auth/im/deliver）的测试复用。
#include "service_discovery.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

namespace testutil {

// 找一个空闲回环端口（内核分配后释放；存在轻微竞态，测试可接受）。
inline uint16_t pickFreePort() {
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

// 探测 etcd 是否可达（对不存在的 prefix 做一次 discover）。
inline bool etcdReachable(const std::string &etcd) {
  ServiceDiscovery sd(etcd);
  for (int i = 0; i < 20; ++i) {
    if (sd.discover("__myrpc_probe__").has_value())
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

// fork + execv；成功返回子进程 pid，失败返回 -1。
// 子进程 stdout/stderr 重定向到 logfile（空串则 /dev/null）。
inline pid_t spawn(const std::vector<std::string> &argv,
                   const std::string &logfile = "") {
  pid_t pid = ::fork();
  if (pid < 0)
    return -1;
  if (pid == 0) {
    int fd = ::open(logfile.empty() ? "/dev/null" : logfile.c_str(),
                    O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
      ::dup2(fd, STDOUT_FILENO);
      ::dup2(fd, STDERR_FILENO);
      if (fd > 2)
        ::close(fd);
    }
    std::vector<char *> cargv;
    cargv.reserve(argv.size() + 1);
    for (const auto &a : argv)
      cargv.push_back(const_cast<char *>(a.c_str()));
    cargv.push_back(nullptr);
    ::execv(cargv[0], cargv.data());
    ::_exit(127); // exec 失败
  }
  return pid;
}

// 发 SIGTERM 优雅退出，等最多 ~5s；超时则 SIGKILL。返回是否已回收。
inline void stopProcess(pid_t pid) {
  if (pid <= 0)
    return;
  ::kill(pid, SIGTERM);
  int status = 0;
  for (int i = 0; i < 50; ++i) {
    pid_t r = ::waitpid(pid, &status, WNOHANG);
    if (r == pid)
      return;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ::kill(pid, SIGKILL);
  ::waitpid(pid, &status, 0);
}

// TCP 探测：端口是否已可连接。
inline bool tcpPortOpen(const std::string &ip, uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return false;
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
  int r = ::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
  ::close(fd);
  return r == 0;
}

// 轮询等待端口就绪，超时返回 false。
inline bool waitForPort(const std::string &ip, uint16_t port, int tries = 100) {
  for (int i = 0; i < tries; ++i) {
    if (tcpPortOpen(ip, port))
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

} // namespace testutil
