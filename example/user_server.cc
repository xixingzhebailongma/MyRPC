#include "rpc_server.h"
#include "service_manager.h"
#include "user.pb.h"
#include "Logger.h"
#include <string>

// Login 的处理函数：输入是序列化的 LoginRequest，输出是序列化的 LoginResponse
std::string handleLogin(const std::string& request_body){
    LoginRequest req;
    if(!req.ParseFromString(request_body)){
        LoginResponse resp;
        resp.set_message("parse requset failed");
        std::string out;
        resp.SerializeToString(&out);
        return out;
    }

    LoginResponse resp;
    if(req.username() == "admin"&& req.password() == "123456"){
        resp.set_success(true);
        resp.set_message("Welcome,"+req.username()+"!");
    }else{
        resp.set_success(false);
        resp.set_message("Invalid username or password");
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

int main(){
    Logger::instance().init(LogLevel::DEBUG,"server.log",true);

    RpcServer server("0.0.0.0",8888);
    server.serviceManager().registerMethod("UserService", "Login", handleLogin);

    LOG_INFO("RPC Server started on 0.0.0.0:8888");
    server.start();
    return 0;
}