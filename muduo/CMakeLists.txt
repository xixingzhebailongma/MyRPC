set(MUDUO_SOURCES
     src/Acceptor.cpp
     src/Buffer.cpp
     src/Channel.cc
     src/Connection.cc
     src/Epoll.cpp
     src/EventLoop.cpp
     src/Socket.cpp
     src/TcpServer.cpp
     src/ThreadPool.cpp
     src/InetAddress.cpp
     src/TimerQueue.cpp
 )

 add_library(muduo STATIC ${MUDUO_SOURCES})

 target_include_directories(muduo
     PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include
 )
 target_link_libraries(muduo
     PUBLIC logger              # ← 传递性获得 Logger.h 的 include 路径
     PUBLIC Threads::Threads
 )