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
    services/auth/session_store.cc
    services/auth/auth_server.cc
)
target_include_directories(auth PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/services/auth)
target_link_libraries(auth
    PUBLIC redis
    PUBLIC db
    PUBLIC rpc_server
    PUBLIC im_proto
    PUBLIC auth_proto
    PUBLIC logger
)

# ====== redis 静态库（从 route 拆出，文件位于 common/redis/ 下） ======
# 只依赖 hiredis + logger，刻意不依赖 rpc_server / im_proto，
# 这样 auth 库可以用 Redis 而不被迫拖上 RouteServer。
add_library(redis STATIC
    common/redis/redis_client.cc
    common/redis/redis_reply.cc
    common/redis/redis_pool.cc
    common/redis/redis_subscriber.cc
)
target_include_directories(redis PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/common/redis)
target_link_libraries(redis
    PUBLIC PkgConfig::hiredis
    PUBLIC logger
)

# ====== idempotency_redis 静态库（跨节点共享的 request_id 幂等存储） ======
add_library(idempotency_redis STATIC
    common/redis/idempotency_redis.cc
)
target_include_directories(idempotency_redis PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/common/redis)
target_link_libraries(idempotency_redis
    PUBLIC redis
    PUBLIC rpc_server # 仅为 idempotency_store.h 的 include 路径（纯接口头，无运行时符号依赖）
    PUBLIC logger
)
# ====== mq 静态库（消息队列抽象：当前后端 Redis Stream，接口对齐未来 RocketMQ） ======
add_library(mq STATIC
    common/mq/stream_producer.cc
    common/mq/stream_consumer.cc
)
target_include_directories(mq PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/common/mq)
target_link_libraries(mq
    PUBLIC redis
    PUBLIC logger
)
# ====== sig 静态库（Gateway↔IM 共享的 HMAC 身份签名） ======
add_library(sig STATIC
    common/sig/gateway_sign.cc
)
target_include_directories(sig PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/common/sig)
target_link_libraries(sig
    PUBLIC protocol
    PUBLIC OpenSSL::Crypto
)
# ====== route 静态库（只剩 RouteServer 本身） ======
add_library(route STATIC
    services/route/route_server.cc
)
target_include_directories(route PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/services/route)
target_link_libraries(route
    PUBLIC redis
    PUBLIC rpc_server
    PUBLIC logger
    PUBLIC im_proto
)

# ====== db 静态库（MySQL） ======
add_library(db STATIC
    common/db/user_dao.cc
    common/db/mysql_pool.cc
)
target_include_directories(db PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/common/db)
target_link_libraries(db
    PUBLIC logger
    PUBLIC ${MYSQLCPPCONN_LIB}
    PUBLIC ${MYSQLCLIENT_LIB}
)

# ====== message_store 静态库 ======
add_library(message_store STATIC
    services/im/message_store.cc
)
target_include_directories(message_store PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/services/im)
target_link_libraries(message_store
    PUBLIC redis
    PUBLIC im_proto
    PUBLIC logger
    PUBLIC PkgConfig::hiredis
)

# ====== im_server 可执行文件 ======
add_executable(im_server
    services/im/im_server.cc
    services/im/im_server_main.cc
    services/im/user_manager.cc
    services/im/route_cache.cc
)
target_include_directories(im_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/services/im)
target_link_libraries(im_server
    im_proto
    redis
    idempotency_redis
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
    mq
    sig
)

# ====== route_server 可执行文件 ======
add_executable(route_server
    services/route/route_server_main.cc
)
target_link_libraries(route_server route)

# ====== auth_server 可执行文件 ======
add_executable(auth_server
    services/auth/auth_server_main.cc
)
target_link_libraries(auth_server auth)
# ====== deliver_server 可执行文件（消息投递 worker，里程碑 3：只消费打印） ======
add_executable(deliver_server
    services/deliver/deliver_server.cc
    services/deliver/deliver_server_main.cc
    services/im/route_cache.cc
)
target_include_directories(deliver_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/services/deliver)
target_link_libraries(deliver_server
    mq
    message_store
    im_proto
    lb_rpc_client
    rpc_client
    registry
    load_balancer
    protocol
    logger
    protobuf::libprotobuf
)
# ====== 测试客户端连接原语 ======
add_library(im_client_conn STATIC
    client/im_client_conn.cc
)
target_include_directories(im_client_conn PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/client)
target_link_libraries(im_client_conn PUBLIC protocol OpenSSL::SSL)

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

# ====== gateway_server 可执行文件 ======
add_executable(gateway_server
    services/gateway/gateway_server.cc
    services/gateway/gateway_main.cc
)
target_include_directories(gateway_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/services/gateway)
target_link_libraries(gateway_server
    im_proto
    lb_rpc_client
    rpc_client
    rpc_server
    registry
    load_balancer
    protocol
    muduo
    logger
    sig
    auth_proto
    protobuf::libprotobuf
)
