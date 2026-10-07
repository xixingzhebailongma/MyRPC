# 共享测试辅助头 test_helpers.h 位于 tests/ 根目录，子目录里的 .cc 靠这个 include 路径引用。
include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_executable(test_queue unit/test_queue.cc)
target_link_libraries(test_queue
    PRIVATE logger
    PRIVATE Threads::Threads
)

add_executable(test_logger unit/test_logger.cc)
target_link_libraries(test_logger
    PRIVATE logger
    PRIVATE Threads::Threads
)

add_executable(test_log_json unit/test_log_json.cc)
target_link_libraries(test_log_json
    PRIVATE logger
)

add_executable(test_buffer unit/test_buffer.cc)
target_link_libraries(test_buffer
    PRIVATE muduo
    PRIVATE protocol
)

add_executable(test_im_conn_concurrency unit/test_im_conn_concurrency.cc)
target_link_libraries(test_im_conn_concurrency
    PRIVATE im_client_conn
    PRIVATE Threads::Threads
)

add_executable(test_eventloop_functor_reentry unit/test_eventloop_functor_reentry.cc)
target_link_libraries(test_eventloop_functor_reentry
    PRIVATE muduo
)

add_executable(test_rpc_channel_frame unit/test_rpc_channel_frame.cc)
target_link_libraries(test_rpc_channel_frame
    PRIVATE rpc_client
)

add_executable(test_rpc_channel_concurrency unit/test_rpc_channel_concurrency.cc)
target_link_libraries(test_rpc_channel_concurrency
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_channel_pool_reap unit/test_rpc_channel_pool_reap.cc)
target_link_libraries(test_rpc_channel_pool_reap
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_threadpool unit/test_threadpool.cc)
target_link_libraries(test_threadpool muduo)

add_executable(test_load_balancer unit/test_load_balancer.cc)
target_link_libraries(test_load_balancer PRIVATE load_balancer)

add_executable(test_rpc_channel_circuit_breaker unit/test_rpc_channel_circuit_breaker.cc)
target_link_libraries(test_rpc_channel_circuit_breaker
    PRIVATE rpc_client
)

add_executable(test_rpc_channel_heartbeat unit/test_rpc_channel_heartbeat.cc)
target_link_libraries(test_rpc_channel_heartbeat
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_channel_metrics unit/test_rpc_channel_metrics.cc)
target_link_libraries(test_rpc_channel_metrics
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_idempotency_lru unit/test_idempotency_lru.cc)
target_link_libraries(test_idempotency_lru
    PRIVATE rpc_server
    PRIVATE Threads::Threads
)
add_executable(test_idempotency_redis unit/test_idempotency_redis.cc)
target_link_libraries(test_idempotency_redis
    PRIVATE idempotency_redis
    PRIVATE Threads::Threads
)
add_executable(test_snowflake_id unit/test_snowflake_id.cc)
target_link_libraries(test_snowflake_id
    PRIVATE message_store
    PRIVATE Threads::Threads
)
add_executable(test_async_rpc_client unit/test_async_rpc_client.cc)
target_link_libraries(test_async_rpc_client
    PRIVATE rpc_async_client
    PRIVATE protocol
    PRIVATE Threads::Threads
)

add_executable(test_async_lb_rpc_client unit/test_async_lb_rpc_client.cc)
target_link_libraries(test_async_lb_rpc_client
    PRIVATE async_lb_rpc_client
    PRIVATE rpc_server
    PRIVATE Threads::Threads
)

# ---- 框架端到端集成测试（无外部依赖）----
add_executable(test_rpc_server_e2e unit/test_rpc_server_e2e.cc)
target_link_libraries(test_rpc_server_e2e
    PRIVATE rpc_server
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_server_concurrency unit/test_rpc_server_concurrency.cc)
target_link_libraries(test_rpc_server_concurrency
    PRIVATE rpc_server
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_server_idempotency unit/test_rpc_server_idempotency.cc)
target_link_libraries(test_rpc_server_idempotency
    PRIVATE rpc_server
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_server_metrics unit/test_server_metrics.cc)
target_link_libraries(test_server_metrics
    PRIVATE rpc_server
    PRIVATE rpc_client
    PRIVATE metrics
    PRIVATE Threads::Threads
)

add_executable(test_client_metrics unit/test_client_metrics.cc)
target_link_libraries(test_client_metrics
    PRIVATE metrics
)

add_executable(test_rpc_lb_failover_etcd unit/test_rpc_lb_failover_etcd.cc)
target_link_libraries(test_rpc_lb_failover_etcd
    PRIVATE rpc_server
    PRIVATE lb_rpc_client
    PRIVATE Threads::Threads
)

# ---- watch 增量/续传/compacted/断连阈值（自管一个独立 etcd 实例）----
add_executable(test_watch_incremental unit/test_watch_incremental.cc)
target_link_libraries(test_watch_incremental
    PRIVATE rpc_server
    PRIVATE lb_rpc_client
    PRIVATE registry
    PRIVATE load_balancer
    PRIVATE Threads::Threads
)

# ---- TLS 握手端到端：spawn gateway_server（客户端侧 TLS） + ImClientConn ----
add_executable(test_gateway_tls unit/test_gateway_tls.cc)
target_include_directories(test_gateway_tls PRIVATE ${CMAKE_SOURCE_DIR}/apps/im/client)
target_link_libraries(test_gateway_tls
    PRIVATE im_client_conn
    PRIVATE registry
    PRIVATE Threads::Threads
)
# 依赖真实 gateway_server 二进制，注入其绝对路径；确保先构建。
add_dependencies(test_gateway_tls gateway_server)
target_compile_definitions(test_gateway_tls
    PRIVATE GATEWAY_SERVER_BIN="$<TARGET_FILE:gateway_server>")

# ---- CTest 注册 ----
add_test(NAME test_queue COMMAND test_queue)
add_test(NAME test_logger COMMAND test_logger)
add_test(NAME test_log_json COMMAND test_log_json)
add_test(NAME test_buffer COMMAND test_buffer)
add_test(NAME test_im_conn_concurrency COMMAND test_im_conn_concurrency)
add_test(NAME test_eventloop_functor_reentry COMMAND test_eventloop_functor_reentry)
add_test(NAME test_rpc_channel_frame COMMAND test_rpc_channel_frame)
add_test(NAME test_rpc_channel_concurrency COMMAND test_rpc_channel_concurrency)
add_test(NAME test_rpc_channel_pool_reap COMMAND test_rpc_channel_pool_reap)
add_test(NAME test_threadpool COMMAND test_threadpool)
add_test(NAME test_load_balancer COMMAND test_load_balancer)
add_test(NAME test_rpc_channel_circuit_breaker COMMAND test_rpc_channel_circuit_breaker)
add_test(NAME test_rpc_channel_heartbeat COMMAND test_rpc_channel_heartbeat)
add_test(NAME test_rpc_channel_metrics COMMAND test_rpc_channel_metrics)
add_test(NAME test_idempotency_lru COMMAND test_idempotency_lru)
add_test(NAME test_idempotency_redis COMMAND test_idempotency_redis)
add_test(NAME test_snowflake_id COMMAND test_snowflake_id)
add_test(NAME test_async_rpc_client COMMAND test_async_rpc_client)
add_test(NAME test_async_lb_rpc_client COMMAND test_async_lb_rpc_client)
add_test(NAME test_rpc_server_e2e COMMAND test_rpc_server_e2e)
add_test(NAME test_rpc_server_concurrency COMMAND test_rpc_server_concurrency)
add_test(NAME test_rpc_server_idempotency COMMAND test_rpc_server_idempotency)
add_test(NAME test_server_metrics COMMAND test_server_metrics)
add_test(NAME test_client_metrics COMMAND test_client_metrics)
add_test(NAME test_rpc_lb_failover_etcd COMMAND test_rpc_lb_failover_etcd)
add_test(NAME test_watch_incremental COMMAND test_watch_incremental)
add_test(NAME test_gateway_tls COMMAND test_gateway_tls)

# ---- IM 全链路 E2E：spawn route/auth/im/gateway/deliver + ImClientConn ----
add_executable(test_im_e2e business/test_im_e2e.cc)
target_link_libraries(test_im_e2e
    PRIVATE im_client_conn
    PRIVATE im_proto
    PRIVATE registry
    PRIVATE protobuf::libprotobuf
    PRIVATE Threads::Threads
)
# 依赖真实服务二进制，注入绝对路径；确保先构建。
add_dependencies(test_im_e2e route_server auth_server im_server gateway_server deliver_server)
target_compile_definitions(test_im_e2e PRIVATE
    ROUTE_SERVER_BIN="$<TARGET_FILE:route_server>"
    AUTH_SERVER_BIN="$<TARGET_FILE:auth_server>"
    IM_SERVER_BIN="$<TARGET_FILE:im_server>"
    GATEWAY_SERVER_BIN="$<TARGET_FILE:gateway_server>"
    DELIVER_SERVER_BIN="$<TARGET_FILE:deliver_server>")
add_test(NAME test_im_e2e COMMAND test_im_e2e)

# ---- IM 业务级完整测试：多端会话/账号生命周期/好友/消息可靠性/安全边界 ----
add_executable(test_im_business business/test_im_business.cc)
target_link_libraries(test_im_business
    PRIVATE im_client_conn
    PRIVATE im_proto
    PRIVATE registry
    PRIVATE protobuf::libprotobuf
    PRIVATE Threads::Threads
)
add_dependencies(test_im_business route_server auth_server im_server gateway_server deliver_server)
target_compile_definitions(test_im_business PRIVATE
    ROUTE_SERVER_BIN="$<TARGET_FILE:route_server>"
    AUTH_SERVER_BIN="$<TARGET_FILE:auth_server>"
    IM_SERVER_BIN="$<TARGET_FILE:im_server>"
    GATEWAY_SERVER_BIN="$<TARGET_FILE:gateway_server>"
    DELIVER_SERVER_BIN="$<TARGET_FILE:deliver_server>")
add_test(NAME test_im_business COMMAND test_im_business)

# ---- IM 多节点一致性 E2E：2 个 IM 节点 + 共享 Redis 路由 + 去 pin 轮询 ----
add_executable(test_im_multi_node multi-node/test_im_multi_node.cc)
target_link_libraries(test_im_multi_node
    PRIVATE im_client_conn
    PRIVATE im_proto
    PRIVATE redis
    PRIVATE registry
    PRIVATE protobuf::libprotobuf
    PRIVATE Threads::Threads
)
add_dependencies(test_im_multi_node route_server auth_server im_server gateway_server deliver_server)
target_compile_definitions(test_im_multi_node PRIVATE
    ROUTE_SERVER_BIN="$<TARGET_FILE:route_server>"
    AUTH_SERVER_BIN="$<TARGET_FILE:auth_server>"
    IM_SERVER_BIN="$<TARGET_FILE:im_server>"
    GATEWAY_SERVER_BIN="$<TARGET_FILE:gateway_server>"
    DELIVER_SERVER_BIN="$<TARGET_FILE:deliver_server>")
add_test(NAME test_im_multi_node COMMAND test_im_multi_node)

# 三个 IM E2E 测试共用固定服务名 "ImService"（各自 fork 服务进程），并行会互抢
# etcd 同名节点导致互相等待/串扰，故三者串行运行（RUN_SERIAL 只限制彼此，不影响
# 与其它无外部依赖的框架测试并行）。
set_tests_properties(test_im_e2e test_im_business test_im_multi_node
    PROPERTIES RUN_SERIAL TRUE)

# 压测基准（不注册为 CTest，手动运行）。
add_subdirectory(bench)
