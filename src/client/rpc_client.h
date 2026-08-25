#pragma once
#include "rpc_channel.h"
class RpcClient{
    public:
    RpcClient(const std::string& server_ip,uint16_t server_port):
    channel_(server_ip,server_port){

    }

    //调用远端方法，返回true，表示成功
    bool Call(const std::string& service_name,
            const std::string& method_name,
            const std::string& request_body,
            std::string& response_body,
            int32_t& error_code){
                return channel_.Call(service_name,method_name,request_body,response_body,error_code);
            }

    private:
    RpcChannel channel_;
};