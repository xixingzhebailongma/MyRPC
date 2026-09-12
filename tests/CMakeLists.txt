add_executable(test_queue test_queue.cc)
target_link_libraries(test_queue
    PRIVATE logger
    PRIVATE Threads::Threads
)

add_executable(test_buffer test_buffer.cc)
target_link_libraries(test_buffer
    PRIVATE muduo
    PRIVATE protocol
)

add_executable(test_im_conn_concurrency test_im_conn_concurrency.cc)
target_link_libraries(test_im_conn_concurrency
    PRIVATE im_client_conn
    PRIVATE Threads::Threads
)

add_executable(test_eventloop_functor_reentry test_eventloop_functor_reentry.cc)
target_link_libraries(test_eventloop_functor_reentry
    PRIVATE muduo
)

add_executable(test_rpc_channel_frame test_rpc_channel_frame.cc)
target_link_libraries(test_rpc_channel_frame
    PRIVATE rpc_client
)

add_executable(test_rpc_channel_concurrency test_rpc_channel_concurrency.cc)
target_link_libraries(test_rpc_channel_concurrency
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_channel_pool_reap test_rpc_channel_pool_reap.cc)
target_link_libraries(test_rpc_channel_pool_reap
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_threadpool test_threadpool.cc)
target_link_libraries(test_threadpool muduo)

add_executable(test_load_balancer test_load_balancer.cc)
target_link_libraries(test_load_balancer PRIVATE load_balancer)

add_executable(test_rpc_channel_circuit_breaker test_rpc_channel_circuit_breaker.cc)
target_link_libraries(test_rpc_channel_circuit_breaker
    PRIVATE rpc_client
)

add_executable(test_rpc_channel_heartbeat test_rpc_channel_heartbeat.cc)
target_link_libraries(test_rpc_channel_heartbeat
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_channel_metrics test_rpc_channel_metrics.cc)
target_link_libraries(test_rpc_channel_metrics
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_idempotency_lru test_idempotency_lru.cc)
target_link_libraries(test_idempotency_lru
    PRIVATE rpc_server
    PRIVATE Threads::Threads
)
add_executable(test_idempotency_redis test_idempotency_redis.cc)
target_link_libraries(test_idempotency_redis
    PRIVATE idempotency_redis
    PRIVATE Threads::Threads
)
add_executable(test_async_rpc_client test_async_rpc_client.cc)
target_link_libraries(test_async_rpc_client
    PRIVATE rpc_async_client
    PRIVATE protocol
    PRIVATE Threads::Threads
)

add_executable(test_async_lb_rpc_client test_async_lb_rpc_client.cc)
target_link_libraries(test_async_lb_rpc_client
    PRIVATE async_lb_rpc_client
    PRIVATE rpc_server
    PRIVATE Threads::Threads
)

# ---- 框架端到端集成测试（无外部依赖）----
add_executable(test_rpc_server_e2e test_rpc_server_e2e.cc)
target_link_libraries(test_rpc_server_e2e
    PRIVATE rpc_server
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_server_concurrency test_rpc_server_concurrency.cc)
target_link_libraries(test_rpc_server_concurrency
    PRIVATE rpc_server
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_server_idempotency test_rpc_server_idempotency.cc)
target_link_libraries(test_rpc_server_idempotency
    PRIVATE rpc_server
    PRIVATE rpc_client
    PRIVATE Threads::Threads
)

add_executable(test_rpc_lb_failover_etcd test_rpc_lb_failover_etcd.cc)
target_link_libraries(test_rpc_lb_failover_etcd
    PRIVATE rpc_server
    PRIVATE lb_rpc_client
    PRIVATE Threads::Threads
)

# ---- TLS 握手端到端：spawn gateway_server（客户端侧 TLS） + ImClientConn ----
add_executable(test_gateway_tls test_gateway_tls.cc)
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
add_test(NAME test_async_rpc_client COMMAND test_async_rpc_client)
add_test(NAME test_async_lb_rpc_client COMMAND test_async_lb_rpc_client)
add_test(NAME test_rpc_server_e2e COMMAND test_rpc_server_e2e)
add_test(NAME test_rpc_server_concurrency COMMAND test_rpc_server_concurrency)
add_test(NAME test_rpc_server_idempotency COMMAND test_rpc_server_idempotency)
add_test(NAME test_rpc_lb_failover_etcd COMMAND test_rpc_lb_failover_etcd)
add_test(NAME test_gateway_tls COMMAND test_gateway_tls)

# ---- IM 全链路 E2E：spawn route/auth/im/gateway/deliver + ImClientConn ----
add_executable(test_im_e2e test_im_e2e.cc)
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
