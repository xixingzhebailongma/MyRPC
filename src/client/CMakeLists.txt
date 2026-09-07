add_library(rpc_client STATIC
    rpc_channel.cc
    rpc_channel.h
    rpc_channel_pool.h
    rpc_client.h
)

target_include_directories(rpc_client
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(rpc_client
    PUBLIC protocol
    PUBLIC logger
)


add_library(lb_rpc_client STATIC
    lb_rpc_client.cc
)

target_include_directories(lb_rpc_client
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(lb_rpc_client
    PUBLIC rpc_client
    PUBLIC registry
    PUBLIC load_balancer
)

add_library(rpc_async_client STATIC
    async_rpc_channel.cc
    async_rpc_client.cc
)

target_include_directories(rpc_async_client
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(rpc_async_client
    PUBLIC muduo
    PUBLIC protocol
    PUBLIC logger
)


add_library(async_lb_rpc_client STATIC
    async_lb_rpc_client.cc
)

target_include_directories(async_lb_rpc_client
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(async_lb_rpc_client
    PUBLIC rpc_async_client
    PUBLIC registry
    PUBLIC load_balancer
)
