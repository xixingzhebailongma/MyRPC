#include "redis_reply.h"
#include <hiredis/hiredis.h>

RedisReply::RedisReply(redisReply* reply):reply_(reply){}

RedisReply::~RedisReply(){
    if(reply_){
        freeReplyObject(reply_);
    }
}

RedisReply::RedisReply(RedisReply&& o)noexcept:reply_(o.reply_){
    o.reply_ = nullptr;
}

RedisReply& RedisReply::operator=(RedisReply&&o)noexcept{
    if(this!=&o){
        if(reply_){
            freeReplyObject(reply_);
        }
        reply_ = o.reply_;
        o.reply_ = nullptr;
    }
    return *this;
}

