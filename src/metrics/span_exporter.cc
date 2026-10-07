#include "span_exporter.h"
#include "Logger.h"
#include <chrono>
#include <fcntl.h>
#include <unistd.h>

// 墙钟微秒（system_clock）：跨进程粗略排序用，不参与拼树（拼树只靠 parent_span_id）。
static uint64_t wallUs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count());
}

SpanExporter &SpanExporter::instance() {
  static SpanExporter inst;
  return inst;
}

void SpanExporter::init(const std::string &filePath) {
  std::lock_guard<std::mutex> lk(mutex_);
  fd_ = ::open(filePath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
}

void SpanExporter::exportSpan(const Span &span) {
  // 在调用线程序列化：Span 及其字段只在本次调用内有效，绝不入队保存。
  std::string line;
  line.reserve(160);
  line += "{\"trace_id\":\"";
  line += Logger::jsonEscape(span.trace_id);
  line += "\",\"span_id\":\"";
  line += Logger::jsonEscape(span.span_id);
  line += "\",\"parent_span_id\":\"";
  line += Logger::jsonEscape(span.parent_span_id);
  line += "\",\"service\":\"";
  line += Logger::jsonEscape(Logger::instance().serviceName());
  line += "\",\"method\":\"";
  line += Logger::jsonEscape(span.method);
  line += "\",\"start_us\":";
  line += std::to_string(span.start_us);
  line += ",\"end_us\":";
  line += std::to_string(span.end_us);
  line += ",\"status\":\"";
  line += Logger::jsonEscape(span.status);
  line += "\",\"wall_us\":";
  line += std::to_string(wallUs());
  line += "}\n";

  std::lock_guard<std::mutex> lk(mutex_);
  if (fd_ >= 0) {
    ::write(fd_, line.data(), line.size());
  }
}
