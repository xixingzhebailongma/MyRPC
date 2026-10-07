#pragma once
#include "AsyncLogger.h"
#include "LogLevel.h"
#include <cstdarg>
#include <cstdint>
#include <mutex>
#include <sstream>
#include <string>

// 结构化日志上下文：供带 trace/span/request/msg 的入口日志使用。
// 生命周期：这些 const char* 仅指向调用方栈上/临时对象的字符串（如
// hdr.trace_id().c_str()），只在调用 logCtx() 的同一完整表达式内有效。
// logCtx() 在调用线程内同步完成 JSON 拼装；LogContext 及其字段指针绝不存入
// 异步队列——入队的只有拼好的 std::string。
struct LogContext {
  const char *trace_id = nullptr;   // null/空 → "-"
  const char *span_id = nullptr;
  const char *request_id = nullptr;
  const char *msg_id = nullptr;
};

class Logger {
public:
  static Logger &instance();
  void init(LogLevel level, const std::string &logFile, bool console,
            bool dropOnOverflow = true,
            LogLevel minGuaranteedLevel = LogLevel::WARN);

  // 进程内服务名：各 main 启动时设一次，框架层日志自动带上。
  void setServiceName(const std::string &name) { service_ = name; }
  const std::string &serviceName() const { return service_; }

  void log(LogLevel, const char *file, int line, const char *fmt, ...);
  void logCtx(LogLevel, const char *file, int line, const LogContext &ctx,
              const char *fmt, ...);
  static LogLevel levelFromString(const std::string &s);

  // JSON 转义（测试/工具用）：" \ \n \r \t 及 <0x20 控制字符（含 \0）。
  static std::string jsonEscape(const std::string &s);
  // 组装一条 JSON 日志行（不含末尾换行），供单测断言；测试/诊断用。
  std::string assembleJsonLine(LogLevel level, const char *file, int line,
                               const LogContext &ctx,
                               const std::string &message) const;

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

  enum class Format { kText, kJson };
  void vlog(LogLevel level, const char *file, int line, const LogContext *ctx,
            const char *fmt, va_list args);

  std::mutex mutex_;
  LogLevel level_{LogLevel::INFO};
  bool console_{false};
  bool fileLoggingEnabled_{false};
  Format format_{Format::kJson};
  std::string service_;
};

#ifdef NDEBUG
#define LOG_DEBUG(fmt, ...) ((void)0)
#define LOG_DEBUG_CTX(ctx, fmt, ...) ((void)0)
#else
#define LOG_DEBUG(fmt, ...)                                                    \
  Logger::instance().log(LogLevel::DEBUG, __FILE__, __LINE__, fmt,             \
                         ##__VA_ARGS__)
#define LOG_DEBUG_CTX(ctx, fmt, ...)                                           \
  Logger::instance().logCtx(LogLevel::DEBUG, __FILE__, __LINE__, ctx, fmt,     \
                            ##__VA_ARGS__)
#endif
#define LOG_INFO(fmt, ...)                                                     \
  Logger::instance().log(LogLevel::INFO, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_INFO_CTX(ctx, fmt, ...)                                            \
  Logger::instance().logCtx(LogLevel::INFO, __FILE__, __LINE__, ctx, fmt,      \
                            ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)                                                     \
  Logger::instance().log(LogLevel::WARN, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define LOG_WARN_CTX(ctx, fmt, ...)                                            \
  Logger::instance().logCtx(LogLevel::WARN, __FILE__, __LINE__, ctx, fmt,      \
                            ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...)                                                    \
  Logger::instance().log(LogLevel::ERROR, __FILE__, __LINE__, fmt,             \
                         ##__VA_ARGS__)
#define LOG_ERROR_CTX(ctx, fmt, ...)                                           \
  Logger::instance().logCtx(LogLevel::ERROR, __FILE__, __LINE__, ctx, fmt,     \
                            ##__VA_ARGS__)

#define LOG_DS Logger::LogStream(LogLevel::DEBUG, __FILE__, __LINE__)
#define LOG_IS Logger::LogStream(LogLevel::INFO, __FILE__, __LINE__)
#define LOG_WS Logger::LogStream(LogLevel::WARN, __FILE__, __LINE__)
#define LOG_ES Logger::LogStream(LogLevel::ERROR, __FILE__, __LINE__)
