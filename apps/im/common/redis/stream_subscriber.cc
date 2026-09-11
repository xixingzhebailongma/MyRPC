#include "stream_subscriber.h"
#include "Logger.h"
#include <chrono>
#include <thread>

namespace {
// Stream 条目承载消息体的字段名，必须与生产者（xaddTrimmed 写入端）保持一致。
// 与 stream_producer.cc / stream_consumer.cc 一样本地声明，避免 redis 库依赖 mq。
constexpr const char *kBodyField = "body";
constexpr const char *kConsumer = "c0"; // 每个组只有一个消费者（本节点线程）
constexpr int kCount = 100;             // 每轮最多取回条数
constexpr int kBlockMs = 500;           // 无消息时的阻塞等待（毫秒）
constexpr int kReclaimIntervalSec = 5;  // 回收扫描周期
constexpr int64_t kMinIdleMs = 10000;   // 闲置多久回收（崩溃重投）
} // namespace

StreamSubscriber::~StreamSubscriber() { stop(); }

void StreamSubscriber::start(
    const std::string &ip, int port, const std::string &stream,
    const std::string &group,
    std::function<void(const std::string &payload)> on_message) {
  if (thread_.joinable())
    return;
  stop_ = false;
  thread_ = std::thread(&StreamSubscriber::run, this, ip, port, stream, group,
                        std::move(on_message));
}

void StreamSubscriber::stop() {
  stop_ = true;
  if (thread_.joinable())
    thread_.join();
}

int StreamSubscriber::handleEntries(
    const std::string &stream, const std::string &group,
    std::vector<StreamEntry> &entries,
    const std::function<void(const std::string &)> &on_message) {
  int n = 0;
  for (auto &e : entries) {
    const std::string *body = nullptr;
    for (const auto &kv : e.fields) {
      if (kv.first == kBodyField) {
        body = &kv.second;
        break;
      }
    }
    if (!body) {
      // 异常数据：缺 body 字段，直接 ACK 丢弃，避免永久卡在 PEL
      LOG_ERROR("StreamSubscriber: entry %s missing body field, acked",
                e.id.c_str());
      redis_.xack(stream, group, e.id);
      continue;
    }
    on_message(*body);
    redis_.xack(stream, group, e.id);
    ++n;
  }
  return n;
}

void StreamSubscriber::run(
    std::string ip, int port, std::string stream, std::string group,
    std::function<void(const std::string &payload)> on_message) {
  using namespace std::chrono;
  // 1. 连接 Redis（即便失败 pool 也已 initialized，后续 acquire 会自愈拨号）
  redis_.connect(ip, port);
  // 2. 幂等建组（从 $ 起，新节点只读未来事件）；失败重试直到成功/停止
  while (!stop_.load()) {
    if (redis_.xgroupCreateFromNow(stream, group))
      break;
    LOG_ERROR("StreamSubscriber: create group %s on %s failed, retry in 1s",
              group.c_str(), stream.c_str());
    std::this_thread::sleep_for(seconds(1));
  }
  if (stop_.load())
    return;
  // 3. 消费循环：阻塞读新消息 + 周期回收滞留消息
  auto last_reclaim = steady_clock::now();
  while (!stop_.load()) {
    std::vector<StreamEntry> entries =
        redis_.xreadgroup(group, kConsumer, stream, kCount, kBlockMs);
    handleEntries(stream, group, entries, on_message);

    auto now = steady_clock::now();
    if (now - last_reclaim >= seconds(kReclaimIntervalSec)) {
      last_reclaim = now;
      XAutoClaimResult r = redis_.xautoclaim(stream, group, kConsumer,
                                             kMinIdleMs, kCount, "0-0");
      handleEntries(stream, group, r.claimed, on_message);
    }
  }
}
