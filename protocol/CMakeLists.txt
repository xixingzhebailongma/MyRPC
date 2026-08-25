 protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS rpc_header.proto)

 add_library(protocol STATIC ${PROTO_SRCS} ${PROTO_HDRS} codec.cc)

 target_include_directories(protocol
     PUBLIC ${CMAKE_CURRENT_BINARY_DIR}
     PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
 )
 target_link_libraries(protocol PUBLIC protobuf::libprotobuf)