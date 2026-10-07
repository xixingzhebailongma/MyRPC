add_library(metrics STATIC
    metrics_registry.cc
    metrics_http_server.cc
    span_exporter.cc
)

target_include_directories(metrics PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(metrics
    PUBLIC muduo
    PUBLIC logger
)
