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
