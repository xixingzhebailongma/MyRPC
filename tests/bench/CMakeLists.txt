# 压测/基准工具（不注册为 CTest，手动运行）。
add_executable(bench_async_rpc bench_async_rpc.cc)
target_link_libraries(bench_async_rpc
    PRIVATE rpc_async_client
    PRIVATE rpc_server
    PRIVATE Threads::Threads
)
