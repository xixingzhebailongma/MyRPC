add_library(load_balancer STATIC
    load_balancer.cc
)
target_include_directories(load_balancer
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)
