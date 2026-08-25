find_package(etcd-cpp-api REQUIRED)

add_library(registry STATIC
    etcd_client.cc
    service_discovery.cc
    service_registry.cc
)

target_include_directories(registry
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PUBLIC ${PROJECT_SOURCE_DIR}
)

target_link_libraries(registry
    PUBLIC etcd-cpp-api
    PUBLIC logger
)
