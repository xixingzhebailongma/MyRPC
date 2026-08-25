add_library(logger STATIC
    Logger.cpp
    AsyncLogger.cpp
)

target_include_directories(logger
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} # = Logger/ 目录
)

target_link_libraries(logger
    PUBLIC Threads::Threads
)
