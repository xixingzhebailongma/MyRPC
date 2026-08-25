add_library(rpc_server STATIC
    rpc_server.cc
)

target_include_directories(rpc_server
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(rpc_server
    PUBLIC muduo
    PUBLIC protocol
    PUBLIC registry
)
