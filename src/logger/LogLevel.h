#pragma once
#include <cstdint>

// 日志等级，数值越大优先级越高（用于背压时的分级丢弃判定）。
// 独立成头文件，供 Logger.h 与 AsyncLogger.h 共享（避免二者循环包含）。
enum class LogLevel : uint8_t { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3 };
