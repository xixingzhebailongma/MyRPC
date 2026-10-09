// 长连接承载能力基准：量化「N 个 EventLoop 线程用 epoll 承载 M 条长连接」。
//   - 服务端：P 个监听端口 + 单线程 poll/accept，接受并保持 M 条 TCP 连接。
//   - 客户端：N 个 EventLoop 线程，M 个 TcpClient 按 round-robin 分发到各
//     loop 与各端口，全部建连后保持 hold 时长，统计建立数 / 掉线数 / 内存。
//
// 为什么要多个监听端口：单对 (src_ip, src_port, dst_ip, dst_port) 的连接数
// 受 ip_local_port_range 的临时源端口数限制（本机 ~28k），单个监听端口最多
// 只能承载约 2.8 万条客户端连接；拆到多个端口才能测到 10 万级别的长连接。
//
// 用法：./bench_conn_capacity [--threads=N] [--connections=M] [--duration=S]
//   threads     客户端 EventLoop 线程数（默认 8）
//   connections 目标连接数（默认 100000）
//   duration    建连后保持时长（秒，默认 3）
//
// 输出：threads / connections / established / dropped / 建连耗时 / RSS。
#include "EventLoop.h"
#include "InetAddress.h"
#include "TcpClient.h"

#include <arpa/inet.h>
#include <cerrno>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

int argInt(int argc, char **argv, const char *key, int def) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind(key, 0) == 0)
      return std::atoi(a.c_str() + std::strlen(key));
  }
  return def;
}

// 单个监听端口可承载的连接数上限（本机 ip_local_port_range ≈ 28231）。
constexpr int kPerPortCap = 20000;

// 读取当前进程 RSS（KB），量化「承载 M 连接」的内存代价。
long rssKb() {
  FILE *f = ::fopen("/proc/self/status", "r");
  if (!f)
    return -1;
  char line[256];
  long rss = -1;
  while (::fgets(line, sizeof(line), f)) {
    if (::strncmp(line, "VmRSS:", 6) == 0) {
      rss = ::atol(line + 6);
      break;
    }
  }
  ::fclose(f);
  return rss;
}

} // namespace

int main(int argc, char **argv) {
  const int kThreads = argInt(argc, argv, "--threads=", 8);
  const int kConnections = argInt(argc, argv, "--connections=", 100000);
  const int kDuration = argInt(argc, argv, "--duration=", 3);
  constexpr const char *kIp = "127.0.0.1";

  // 按每端口 kPerPortCap 拆分监听端口，突破单端口源端口上限。
  const int kPorts =
      std::max(1, (kConnections + kPerPortCap - 1) / kPerPortCap);

  // ---------- 服务端：kPorts 个监听端口 + 单线程 poll/accept ----------
  std::vector<int> listenfds;
  listenfds.reserve(kPorts);
  for (int i = 0; i < kPorts; ++i) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      std::perror("socket");
      return 1;
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    ::fcntl(fd, F_SETFL, O_NONBLOCK); // accept 排空依赖非阻塞返回 EAGAIN
    sockaddr_in srv{};
    srv.sin_family = AF_INET;
    srv.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    srv.sin_port = 0; // 内核分配端口
    if (::bind(fd, reinterpret_cast<sockaddr *>(&srv), sizeof(srv)) != 0) {
      std::perror("bind");
      return 1;
    }
    // 非阻塞 accept + 大 backlog：poll 到可读后一次性排空队列，避免 SYN 溢出。
    ::listen(fd, 65535);
    listenfds.push_back(fd);
  }
  std::vector<uint16_t> ports(kPorts);
  for (int i = 0; i < kPorts; ++i) {
    sockaddr_in a{};
    socklen_t l = sizeof(a);
    ::getsockname(listenfds[i], reinterpret_cast<sockaddr *>(&a), &l);
    ports[i] = ntohs(a.sin_port);
  }

  std::atomic<uint64_t> accepted{0};
  std::vector<int> serverFds;
  serverFds.reserve(kConnections);
  std::mutex srvMutex;
  std::thread acceptor([&] {
    std::vector<pollfd> pfds(kPorts);
    for (int i = 0; i < kPorts; ++i) {
      pfds[i].fd = listenfds[i];
      pfds[i].events = POLLIN;
    }
    while (accepted.load() < static_cast<uint64_t>(kConnections)) {
      int n = ::poll(pfds.data(), pfds.size(), 1000);
      if (n < 0) {
        if (errno == EINTR)
          continue;
        break;
      }
      for (int i = 0;
           i < kPorts &&
           accepted.load() < static_cast<uint64_t>(kConnections);
           ++i) {
        if (!(pfds[i].revents & POLLIN))
          continue;
        // 排空该 listen 队列（accept 返回 EAGAIN 表示本次已清空）。
        for (;;) {
          int fd = ::accept(listenfds[i], nullptr, nullptr);
          if (fd < 0) {
            if (errno == EINTR)
              continue;
            break; // EAGAIN 或其它：交给下一轮 poll
          }
          {
            std::lock_guard<std::mutex> lk(srvMutex);
            serverFds.push_back(fd);
          }
          accepted.fetch_add(1);
        }
      }
    }
  });

  // ---------- 客户端：N 个 EventLoop，M 个 TcpClient 分发到 loop/端口 ----------
  std::vector<std::unique_ptr<EventLoop>> loops;
  loops.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i)
    loops.emplace_back(new EventLoop(false));
  std::vector<std::thread> loopThreads;
  for (auto &l : loops)
    loopThreads.emplace_back([loop = l.get()] { loop->run(); });

  std::atomic<uint64_t> established{0};
  std::atomic<uint64_t> dropped{0};

  std::vector<std::unique_ptr<TcpClient>> clients;
  clients.reserve(kConnections);
  auto t0 = std::chrono::steady_clock::now();
  // 滑动窗口：限制在途连接数，避免瞬时打满 accept 队列触发退避重试风暴。
  constexpr int kInFlightWindow = 2048;
  uint64_t fired = 0;
  for (int i = 0; i < kConnections; ++i) {
    while (fired - established.load() >=
           static_cast<uint64_t>(kInFlightWindow)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    InetAddress srvAddr(kIp, ports[i % kPorts]);
    auto c = std::make_unique<TcpClient>(loops[i % kThreads].get(), srvAddr,
                                         "conn" + std::to_string(i));
    c->setConnectionCallback([&](spConnection) { established.fetch_add(1); });
    c->setCloseCallback([&](spConnection) { dropped.fetch_add(1); });
    c->connect();
    clients.push_back(std::move(c));
    ++fired;
  }

  // 等全部建连（60s 上限，防止对端异常时挂死）。
  const auto kStart = std::chrono::steady_clock::now();
  const auto kDeadline = kStart + std::chrono::seconds(60);
  while (established.load() < static_cast<uint64_t>(kConnections) &&
         std::chrono::steady_clock::now() < kDeadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  auto connectMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();

  // 保持 hold 时长，验证「承载」而非瞬时建连。
  std::this_thread::sleep_for(std::chrono::seconds(kDuration));

  uint64_t est = established.load();
  uint64_t drp = dropped.load();
  uint64_t acc = accepted.load();
  long rss = rssKb();

  std::printf("========== 长连接承载能力基准 ==========\n");
  std::printf("threads=%d connections=%d ports=%d duration=%ds\n", kThreads,
              kConnections, kPorts, kDuration);
  std::printf("客户端已建连: %llu   掉线: %llu\n", (unsigned long long)est,
              (unsigned long long)drp);
  std::printf("服务端已接受: %llu\n", (unsigned long long)acc);
  std::printf("建连耗时: %lld ms\n", (long long)connectMs);
  std::printf("进程 RSS: %ld KB (~%.1f MB)\n", rss,
              rss > 0 ? double(rss) / 1024.0 : -1.0);

  // ---------- 清理 ----------
  for (auto &c : clients)
    c->disconnect();
  for (auto &l : loops)
    l->stop();
  for (auto &t : loopThreads)
    t.join();
  clients.clear();
  for (int fd : serverFds)
    ::close(fd);
  for (int fd : listenfds)
    ::close(fd);
  acceptor.join();
  return 0;
}
