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
