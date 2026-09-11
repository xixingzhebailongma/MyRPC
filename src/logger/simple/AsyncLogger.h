// 学习参考：双缓冲简化版 AsyncLogger（不参与构建，见同目录 README.md）
#pragma once
#include <atomic>
#include <condition_variable>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class AsyncLogger {
public:
  static AsyncLogger &instance();
  void start(const std::string &filePath);
  void stop();
  void append(const char *data, size_t len);

private:
  void threadFunc();

  std::mutex mutex_;
  std::condition_variable cond_;
  std::string curBuffer_;              // 前端直接写入的当前缓冲
  std::vector<std::string> filledBuffers_; // 写满待落盘的缓冲
  std::ofstream file_;
  std::unique_ptr<std::thread> thread_;
  std::atomic<bool> running_{false};

  static constexpr size_t kMaxBufferSize = 4 * 1024 * 1024; // 4MB 才切缓冲
  static constexpr int kFlushIntervalMs = 3000;              // 3s 兜底刷盘
};
