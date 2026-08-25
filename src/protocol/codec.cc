#include "rpc_header.pb.h"
#include "rpc_protocol.h"
#include <cstdint>
#include <cstring>
std::string encodeMessage(const RpcMessage& msg){
    std::string body;
    msg.SerializeToString(&body);

    uint32_t len = body.size();
    std::string result;
    result.append(reinterpret_cast<const char*>(&len),4);
    result.append(body);
    return result;
}

bool decodeMessage(const std::string &wire_data,RpcMessage&msg){
    return msg.ParseFromString(wire_data);
}

RpcMessage buildRequest(const std::string& service_name, const std::string &method_name, uint64_t sequence_id, const std::string &body){
    RpcMessage msg;
    auto* header = msg.mutable_header();
    header->set_service_name(service_name);
    header->set_method_name(method_name);
    header->set_sequence_id(sequence_id);
    header->set_error_code(0);
    msg.set_body(body);
    return msg;

}


RpcMessage buildResponse(uint64_t sequence_id,int32_t error_code,
                        const std::string &body){
                            RpcMessage msg;
                            auto* header = msg.mutable_header();
                            header->set_sequence_id(sequence_id);
                            header->set_error_code(error_code);
                            msg.set_body(body);
                            return msg;
                        }