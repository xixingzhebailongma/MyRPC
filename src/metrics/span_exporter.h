#pragma once
#include <cstdint>
#include <mutex>
#include <string>

// 单个 span 的导出记录。exportSpan 在调用线程内同步把 Span 序列化成一行 JSONL
// （Span 及其中 const char* 字段只在本调用内有效），随后写文件；绝不在队列里
// 保存 Span 对象或其字段指针。service 字段取自 Logger::serviceName()（进程级）。
struct Span {
  std::string trace_id;
  std::string span_id;
  std::string parent_span_id; // 空 = 根
  std::string method;         // RPC method 或手动 span 名
  uint64_t start_us = 0;
  uint64_t end_us = 0;
  std::string status = "OK";  // OK / ERROR
};

// 极简 span 导出器：序列化在调用线程，写文件（append，mutex 保护）。
// 每个服务进程独立一个 spans.jsonl；轮转留待后续。
class SpanExporter {
public:
  static SpanExporter &instance();
  // 打开 spans.jsonl；不成功则静默禁用（不崩）。
  void init(const std::string &filePath);
  void exportSpan(const Span &span);

private:
  SpanExporter() = default;
  SpanExporter(const SpanExporter &) = delete;
  SpanExporter &operator=(const SpanExporter &) = delete;

  std::mutex mutex_;
  int fd_ = -1;
};
