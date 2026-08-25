#include "rpc_client.h"
#include "user.pb.h"
#include "Logger.h"
#include <iostream>

int main(){
    Logger::instance().init(LogLevel::DEBUG,"client.log",true);

    RpcClient client("127.0.0.1",8888);

    //构造请求
    LoginRequest req;
    req.set_username("admin");
    req.set_password("123456");

    std::string request_body;
    req.SerializeToString(&request_body);

    //发起RPC调用
    std::string response_body;
    int32_t error_code = 0;

    if(client.Call("UserService","Login",request_body,response_body,error_code)){
        LoginResponse resp;
        if(resp.ParseFromString(response_body)){
            std::cout << "Login result: " << (resp.success() ? "SUCCESS" : "FAILED")
                        << " — " << resp.message() << std::endl;
        }else{
            LOG_ERROR("Failed to parse response");
        }
    }else{
         LOG_ERROR("RPC call failed, error_code=%d", error_code);
    }
    return 0;
}