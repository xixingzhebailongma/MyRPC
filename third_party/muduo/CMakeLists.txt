set(MUDUO_SOURCES
    src/Acceptor.cc
    src/Buffer.cc
    src/Channel.cc

    src/Connection.cc
    src/Epoll.cc
    src/EventLoop.cc
    src/Socket.cc
    src/TcpServer.cc
    src/ThreadPool.cc
    src/InetAddress.cc
    src/TimerQueue.cc
)

add_library(muduo STATIC ${MUDUO_SOURCES})

target_include_directories(muduo
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_link_libraries(muduo
    PUBLIC logger # ← 传递性获得 Logger.h 的 include 路径
    PUBLIC Threads::Threads
    PUBLIC OpenSSL::SSL # ← 新增：TLS 依赖，经 muduo 传递给 gateway_server
)
