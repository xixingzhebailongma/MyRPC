add_executable(test_queue test_queue.cc)
target_link_libraries(test_queue
    PRIVATE logger
    PRIVATE Threads::Threads
)
