# 压测/基准工具（不注册为 CTest，手动运行）。
add_executable(bench_async_rpc bench_async_rpc.cc)
target_link_libraries(bench_async_rpc
    PRIVATE rpc_async_client
    PRIVATE rpc_server
    PRIVATE Threads::Threads
)

add_executable(bench_log_format bench_log_format.cc)
target_link_libraries(bench_log_format
    PRIVATE rpc_client
    PRIVATE rpc_server
    PRIVATE Threads::Threads
)

add_executable(bench_conn_capacity bench_conn_capacity.cc)
target_link_libraries(bench_conn_capacity
    PRIVATE muduo
    PRIVATE Threads::Threads
)

add_executable(bench_span_export bench_span_export.cc)
target_link_libraries(bench_span_export
    PRIVATE rpc_client
    PRIVATE rpc_server
    PRIVATE metrics
    PRIVATE Threads::Threads
)
