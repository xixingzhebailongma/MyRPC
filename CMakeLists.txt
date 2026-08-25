cmake_minimum_required(VERSION 3.16)
project(MyRPC VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra)
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG")

find_package(Threads REQUIRED)
find_package(Protobuf REQUIRED)
find_package(PkgConfig REQUIRED)

add_subdirectory(Logger)

add_subdirectory(muduo)
add_subdirectory(protocol)
add_subdirectory(registry)
add_subdirectory(loadbalance)
add_subdirectory(client)
add_subdirectory(im)
add_subdirectory(server)
add_subdirectory(example)
