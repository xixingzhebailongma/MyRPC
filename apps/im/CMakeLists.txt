find_package(PkgConfig REQUIRED)
pkg_check_modules(hiredis REQUIRED IMPORTED_TARGET hiredis)
find_library(MYSQLCLIENT_LIB mysqlclient REQUIRED)
find_library(MYSQLCPPCONN_LIB mysqlcppconn REQUIRED)

# ====== im 协议 ======
protobuf_generate_cpp(IM_PROTO_SRCS IM_PROTO_HDRS proto/im.proto)
add_library(im_proto STATIC ${IM_PROTO_SRCS})
target_include_directories(im_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(im_proto PUBLIC protobuf::libprotobuf)

# ====== auth 协议 ======
# 独立成库：认证契约与 IM 契约解耦，改认证不触发 im.pb.cc 全量重编。
protobuf_generate_cpp(AUTH_PROTO_SRCS AUTH_PROTO_HDRS proto/auth.proto)
add_library(auth_proto STATIC ${AUTH_PROTO_SRCS})
target_include_directories(auth_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(auth_proto PUBLIC protobuf::libprotobuf)

# ====== auth 静态库（SessionStore + AuthServer） ======
add_library(auth STATIC
    auth/session_store.cc
    auth/auth_server.cc
)
target_include_directories(auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/auth)
target_link_libraries(auth
    PUBLIC redis
    PUBLIC db
    PUBLIC rpc_server
    PUBLIC im_proto
    PUBLIC auth_proto
    PUBLIC logger
)

# ====== redis 静态库（从 route 拆出；文件位置不动，仍在 route/ 下） ======
# 只依赖 hiredis + logger，刻意不依赖 rpc_server / im_proto，
# 这样 auth 库可以用 Redis 而不被迫拖上 RouteServer。
add_library(redis STATIC
    route/redis_client.cc
    route/redis_reply.cc
    route/redis_pool.cc
    route/redis_subscriber.cc
)
target_include_directories(redis PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/route)
target_link_libraries(redis
    PUBLIC PkgConfig::hiredis
    PUBLIC logger
)

# ====== route 静态库（只剩 RouteServer 本身） ======
add_library(route STATIC
    route/route_server.cc
)
target_include_directories(route PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/route)
target_link_libraries(route
    PUBLIC redis
    PUBLIC rpc_server
    PUBLIC logger
    PUBLIC im_proto
)

# ====== db 静态库（MySQL） ======
add_library(db STATIC
    db/user_dao.cc
    db/mysql_pool.cc
)
target_include_directories(db PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/db)
target_link_libraries(db
    PUBLIC logger
    PUBLIC ${MYSQLCPPCONN_LIB}
    PUBLIC ${MYSQLCLIENT_LIB}
)

# ====== message_store 静态库 ======
add_library(message_store STATIC
    server/message_store.cc
)
target_include_directories(message_store PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/server)
target_link_libraries(message_store
    PUBLIC redis
    PUBLIC im_proto
    PUBLIC logger
    PUBLIC PkgConfig::hiredis
)

# ====== im_server 可执行文件 ======
add_executable(im_server
    server/im_server.cc
    server/im_server_main.cc
    server/user_manager.cc
    server/route_cache.cc
)
target_include_directories(im_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/server)
target_link_libraries(im_server
    im_proto
    redis
    rpc_server
    lb_rpc_client
    rpc_client
    registry
    load_balancer
    protocol
    muduo
    logger
    protobuf::libprotobuf
    message_store
    db
    auth_proto
)

# ====== route_server 可执行文件 ======
add_executable(route_server
    route/route_server_main.cc
)
target_link_libraries(route_server route)

# ====== auth_server 可执行文件 ======
add_executable(auth_server
    auth/auth_server_main.cc
)
target_link_libraries(auth_server auth)

# ====== 测试客户端连接原语 ======
add_library(im_client_conn STATIC
    client/im_client_conn.cc
)
target_include_directories(im_client_conn PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/client)
target_link_libraries(im_client_conn PUBLIC protocol)

# ====== IM 测试客户端 ======
add_executable(im_test_client
    client/im_test_client.cc
)
target_include_directories(im_test_client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/client)
target_link_libraries(im_test_client
    im_client_conn
    im_proto
    protobuf::libprotobuf
)
