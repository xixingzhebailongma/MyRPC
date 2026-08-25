# 从 user.proto 生成 C++ 代码
 protobuf_generate_cpp(USER_PROTO_SRCS USER_PROTO_HDRS user.proto)

  add_executable(user_server user_server.cc ${USER_PROTO_SRCS} ${USER_PROTO_HDRS})
  target_include_directories(user_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
  target_link_libraries(user_server rpc_server protocol protobuf::libprotobuf)

  add_executable(user_client user_client.cc ${USER_PROTO_SRCS} ${USER_PROTO_HDRS})
  target_include_directories(user_client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
  target_link_libraries(user_client rpc_client protocol protobuf::libprotobuf)

  add_executable(user_server_with_etcd user_server_with_etcd.cc ${USER_PROTO_SRCS} ${USER_PROTO_HDRS})
  target_include_directories(user_server_with_etcd PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
  target_link_libraries(user_server_with_etcd rpc_server protocol protobuf::libprotobuf)

  # etcd 发现版客户端
  add_executable(user_client_with_etcd user_client_with_etcd.cc ${USER_PROTO_SRCS} ${USER_PROTO_HDRS})
  target_include_directories(user_client_with_etcd PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
  target_link_libraries(user_client_with_etcd lb_rpc_client protocol protobuf::libprotobuf)