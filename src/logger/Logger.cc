#include "Logger.h"
#include "AsyncLogger.h"
#include <chrono>
#include <cstdio>
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

Logger &Logger::instance() {
  static Logger inst;
  return inst;
}

Logger::~Logger() = default;

void Logger::init(LogLevel level, const std::string &logFile, bool console) {
  std::lock_guard<std::mutex> lock(mutex_);
  level_ = level;
  console_ = console;

  if (!logFile.empty()) {
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

void Logger::log(LogLevel level, const char *file, int line, const char *fmt,
                 ...) {
  va_list args;
  va_start(args, fmt);
  vlog(level, file, line, fmt, args);
  va_end(args);
}

void Logger::vlog(LogLevel level, const char *file, int line, const char *fmt,
                  va_list args) {
  if (level < level_)
    return;
  char msg[4096];
  int n = vsnprintf(msg, sizeof(msg), fmt, args);
  const char *trunc = (n >= static_cast<int>(sizeof(msg))) ? "..." : "";

  // 3. 拼装完整日志行
  std::string log_line; // 改名
  log_line.reserve(256);
  log_line += '[';
  log_line += timestamp();
  log_line += "] [";
  log_line += std::to_string(syscall(SYS_gettid));
  log_line += "] [";
  log_line += levelLabel(level);
  log_line += "] ";
  log_line += file;
  log_line += ':';
  log_line += std::to_string(line); // 现在 line 就是 int 行号，正确
  log_line += " - ";
  log_line += msg;
  log_line += trunc;
  log_line += '\n';

  if (fileLoggingEnabled_) {
    AsyncLogger::instance().append(log_line.data(), log_line.size());
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