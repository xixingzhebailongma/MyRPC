// 学习参考：前端 Logger（printf 风格 + 流式，不参与构建，见同目录 README.md）
#pragma once
#include "AsyncLogger.h"
#include <cstdarg>
#include <cstdint>
#include <mutex>
#include <sstream>
#include <string>

enum class LogLevel : uint8_t { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 };

class Logger {
public:
  static Logger &instance();
  void init(LogLevel level, const std::string &logFile, bool console);

  void log(LogLevel level, const char *file, int line, const char *fmt, ...);
  static LogLevel levelFromString(const std::string &s);

  class LogStream {
  public:
    LogStream(LogLevel level, const char *file, int line);
    ~LogStream();
    template <typename T> LogStream &operator<<(const T &val) {
      buf_ << val;
      return *this;
    }
    LogStream(const LogStream &) = delete;
    LogStream &operator=(const LogStream &) = delete;

  private:
    LogLevel level_;
    const char *file_;
    int line_;
    std::ostringstream buf_;
  };

private:
  Logger() = default;
  ~Logger();
  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  void vlog(LogLevel level, const char *file, int line, const char *fmt,
            va_list args);

  std::mutex mutex_;
  LogLevel level_{LogLevel::WARN};
  bool console_{true};
  bool fileLoggingEnabled_{false};
};

#define LOG_DEBUG(fmt, ...)                                                    \
  Logger::instance().log(LogLevel::DEBUG, __FILE__, __LINE__, fmt,             \
                         ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)                                                     \
  Logger::instance().log(LogLevel::INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)                                                     \
  Logger::instance().log(LogLevel::WARN, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...)                                                    \
  Logger::instance().log(LogLevel::ERROR, __FILE__, __LINE__, fmt,             \
                         ##__VA_ARGS__)

#define LOG_DS Logger::LogStream(LogLevel::DEBUG, __FILE__, __LINE__)
#define LOG_IS Logger::LogStream(LogLevel::INFO, __FILE__, __LINE__)
#define LOG_WS Logger::LogStream(LogLevel::WARN, __FILE__, __LINE__)
#define LOG_ES Logger::LogStream(LogLevel::ERROR, __FILE__, __LINE__)
