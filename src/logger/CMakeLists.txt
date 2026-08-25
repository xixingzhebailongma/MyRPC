add_library(logger STATIC
    Logger.cc
    AsyncLogger.cc
)

target_include_directories(logger
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} # = src/logger/ 目录
)

target_link_libraries(logger
    PUBLIC Threads::Threads
)
