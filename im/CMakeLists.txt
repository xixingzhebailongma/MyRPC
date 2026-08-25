# 在现有内容前添加:
protobuf_generate_cpp(IM_PROTO_SRCS IM_PROTO_HDRS proto/im.proto)

add_library(im_proto STATIC ${IM_PROTO_SRCS})
target_include_directories(im_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(im_proto PUBLIC protobuf::libprotobuf)
# ====== message_store 静态库 ======
add_library(message_store STATIC
    server/message_store.cc
)
target_include_directories(message_store
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/server
    PUBLIC ${PROJECT_SOURCE_DIR}
)
target_link_libraries(message_store
    PUBLIC route
    PUBLIC im_proto
    PUBLIC logger
    PUBLIC PkgConfig::hiredis
)
# 然后添加 im_server 可执行文件:
add_executable(im_server
    server/im_server.cc
    server/im_server_main.cc
    server/user_manager.cpp
    server/route_cache.cc
)
target_include_directories(im_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/server)
target_link_libraries(im_server
    im_proto
    route # RedisClient
    rpc_server # RpcServer
    lb_rpc_client
    rpc_client # LbRpcClient, RpcChannel
    registry # ServiceDiscovery
    load_balancer # ILoadBalancer
    protocol # rpc_protocol (encode/decode/buildRequest/buildResponse)
    muduo # TcpServer, Connection
    logger
    protobuf::libprotobuf
    message_store
    db
)


find_package(PkgConfig REQUIRED)
pkg_check_modules(hiredis REQUIRED IMPORTED_TARGET hiredis)

add_library(route STATIC
    route/redis_client.cc
    route/redis_reply.cc
    route/redis_pool.cc
    route/redis_subscriber.cc
    route/route_server.cc
)

target_include_directories(route
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PUBLIC ${PROJECT_SOURCE_DIR}
)

target_link_libraries(route
    PUBLIC PkgConfig::hiredis
    PUBLIC rpc_server
    PUBLIC logger
    PUBLIC im_proto
)

add_executable(route_server
    route/route_server_main.cc
)
target_link_libraries(route_server
    route
)
# ====== user_dao 静态库（Connector/C++） ======
find_library(MYSQLCLIENT_LIB mysqlclient REQUIRED)
find_library(MYSQLCPPCONN_LIB mysqlcppconn REQUIRED)
add_library(db STATIC
    db/user_dao.cc
    db/mysql_pool.cc
)
target_include_directories(db
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PUBLIC ${PROJECT_SOURCE_DIR}
)
target_link_libraries(db
    PUBLIC logger
    PUBLIC ${MYSQLCPPCONN_LIB}
    PUBLIC ${MYSQLCLIENT_LIB}
)
