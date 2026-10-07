#include "Logger.h"
#include "AsyncLogger.h"
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <sys/syscall.h>
#include <unistd.h>

static const char *levelLabel(LogLevel lv) {
  switch (lv) {
  case LogLevel::DEBUG:
    return "DEBUG";
  case LogLevel::INFO:
    return "INFO";
  case LogLevel::WARN:
    return "WARN";
  case LogLevel::ERROR:
    return "ERROR";
  }
  return "?????";
}

// 旧文本格式时间戳：保持逐字节一致（不改成 ISO）。
static std::string timestamp() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
  auto tt = system_clock::to_time_t(now);
  struct tm tm_buf;
  localtime_r(&tt, &tm_buf);
  char buf[24];
  snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%03ld",
           tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
           tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec, ms.count());
  return buf;
}

// JSON 格式时间戳：ISO 8601，带毫秒与时区偏移（如 "2026-10-07T16:20:30.123+08:00"）。
static std::string timestampIso() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
  auto tt = system_clock::to_time_t(now);
  struct tm tm_buf;
  localtime_r(&tt, &tm_buf);
  char base[32];
  strftime(base, sizeof(base), "%Y-%m-%dT%H:%M:%S", &tm_buf);
  char zone[16];
  strftime(zone, sizeof(zone), "%z", &tm_buf); // "+0800" / "-0500" / "+0000"
  std::string tz(zone);
  if (tz.size() == 5 && (tz[0] == '+' || tz[0] == '-')) {
    tz.insert(3, 1, ':'); // "+0800" -> "+08:00"
  } else if (tz.empty()) {
    tz = "+00:00";
  }
  char out[40];
  snprintf(out, sizeof(out), "%s.%03ld%s", base, ms.count(), tz.c_str());
  return out;
}

static std::string fileBasename(const char *path) {
  const char *p = strrchr(path, '/');
  return p ? std::string(p + 1) : std::string(path);
}

// 结构化字段：null/空 → "-"
static std::string fieldStr(const char *p) {
  return (p && *p) ? std::string(p) : std::string("-");
}

Logger &Logger::instance() {
  static Logger inst;
  return inst;
}

Logger::~Logger() = default;

void Logger::init(LogLevel level, const std::string &logFile, bool console,
                  bool dropOnOverflow, LogLevel minGuaranteedLevel) {
  std::lock_guard<std::mutex> lock(mutex_);
  level_ = level;
  console_ = console;

  // 日志格式：默认 JSON；MYRPC_LOG_FORMAT=text 回退旧文本格式。
  format_ = Format::kJson;
  if (const char *env = std::getenv("MYRPC_LOG_FORMAT")) {
    if (std::string(env) == "text")
      format_ = Format::kText;
  }

  // service 未设置时告警（不静默落 unknown）。
  if (service_.empty()) {
    std::fprintf(stderr,
                 "[Logger] WARN: service name not set, logs will use "
                 "\"unknown\" as service field\n");
  }

  if (!logFile.empty()) {
    AsyncLogger::instance().setDropOnOverflow(dropOnOverflow);
    AsyncLogger::instance().setMinGuaranteedLevel(minGuaranteedLevel);
    AsyncLogger::instance().start(logFile);
    fileLoggingEnabled_ = true;
  }
}

LogLevel Logger::levelFromString(const std::string &s) {
  if (s == "DEBUG" || s == "debug")
    return LogLevel::DEBUG;
  if (s == "INFO" || s == "info")
    return LogLevel::INFO;
  if (s == "WARN" || s == "warn")
    return LogLevel::WARN;
  if (s == "ERROR" || s == "error")
    return LogLevel::ERROR;
  return LogLevel::INFO; // 不认识的一律当 INFO
}

std::string Logger::jsonEscape(const std::string &s) {
  std::string out;
  out.reserve(s.size() + 8);
  for (unsigned char c : s) {
    switch (c) {
    case '"':
      out += "\\\"";
      break;
    case '\\':
      out += "\\\\";
      break;
    case '\n':
      out += "\\n";
      break;
    case '\r':
      out += "\\r";
      break;
    case '\t':
      out += "\\t";
      break;
    default:
      if (c < 0x20) {
        char buf[8];
        snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned>(c));
        out += buf;
      } else {
        out += static_cast<char>(c);
      }
    }
  }
  return out;
}

std::string Logger::assembleJsonLine(LogLevel level, const char *file, int line,
                                     const LogContext &ctx,
                                     const std::string &message) const {
  std::string out;
  out.reserve(256 + message.size());
  out += "{\"ts\":\"";
  out += timestampIso();
  out += "\",\"level\":\"";
  out += levelLabel(level);
  out += "\",\"service\":\"";
  out += jsonEscape(service_.empty() ? "unknown" : service_);
  out += "\",\"file\":\"";
  out += jsonEscape(fileBasename(file));
  out += "\",\"line\":";
  out += std::to_string(line);
  out += ",\"trace_id\":\"";
  out += jsonEscape(fieldStr(ctx.trace_id));
  out += "\",\"span_id\":\"";
  out += jsonEscape(fieldStr(ctx.span_id));
  out += "\",\"request_id\":\"";
  out += jsonEscape(fieldStr(ctx.request_id));
  out += "\",\"msg_id\":\"";
  out += jsonEscape(fieldStr(ctx.msg_id));
  out += "\",\"message\":\"";
  out += jsonEscape(message);
  out += "\"}";
  return out;
}

void Logger::log(LogLevel level, const char *file, int line, const char *fmt,
                 ...) {
  va_list args;
  va_start(args, fmt);
  vlog(level, file, line, nullptr, fmt, args);
  va_end(args);
}

void Logger::logCtx(LogLevel level, const char *file, int line,
                    const LogContext &ctx, const char *fmt, ...) {
  va_list args;
  va_start(args, fmt);
  vlog(level, file, line, &ctx, fmt, args);
  va_end(args);
}

void Logger::vlog(LogLevel level, const char *file, int line,
                  const LogContext *ctx, const char *fmt, va_list args) {
  if (level < level_)
    return;
  char msg[4096];
  int n = vsnprintf(msg, sizeof(msg), fmt, args);
  const char *trunc = (n >= static_cast<int>(sizeof(msg))) ? "..." : "";

  std::string log_line;
  log_line.reserve(256);
  if (format_ == Format::kJson) {
    // 在调用线程完成 JSON 拼装（含转义），再入队；消费线程只写。
    LogContext empty;
    std::string message = std::string(msg) + trunc;
    log_line = assembleJsonLine(level, file, line, ctx ? *ctx : empty, message);
    log_line += '\n';
  } else {
    // 旧文本格式：逐字节保持。
    log_line += '[';
    log_line += timestamp();
    log_line += "] [";
    log_line += std::to_string(syscall(SYS_gettid));
    log_line += "] [";
    log_line += levelLabel(level);
    log_line += "] ";
    log_line += file;
    log_line += ':';
    log_line += std::to_string(line);
    log_line += " - ";
    log_line += msg;
    log_line += trunc;
    log_line += '\n';
  }

  if (fileLoggingEnabled_) {
    AsyncLogger::instance().append(level, log_line.data(), log_line.size());
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (console_) {
    if (level >= LogLevel::ERROR) {
      std::cerr << log_line;
    } else {
      std::cout << log_line;
    }
  }
}

Logger::LogStream::LogStream(LogLevel level, const char *file, int line)
    : level_(level), file_(file), line_(line) {}

Logger::LogStream::~LogStream() {
  std::string s = buf_.str();
  if (!s.empty()) {
    Logger::instance().log(level_, file_, line_, "%s", s.c_str());
  }
}
