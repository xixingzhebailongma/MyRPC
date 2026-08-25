#include "../include/ThreadPool.h"
#include "Logger.h"

ThreadPool::ThreadPool(size_t threadnum,const std::string &threadtype):stop_(false),threadtype_(threadtype){
    for(size_t ii = 0;ii<threadnum;++ii){
        threads_.emplace_back([this]{
            LOG_INFO("create %s thread(%ld).", threadtype_.c_str(),
               syscall(SYS_gettid));
               while(true){
                std::function<void()>task;
                {
                    std::unique_lock<std::mutex>lock(this->mutex_);
                    this->condition_.wait(lock,[this]{
                        return ((this->stop_ == true)||(this->taskqueue_.empty() == false));
                    });
                    if((this->stop_ == true)&&(this->taskqueue_.empty() == true))return;
                    task = std::move(this->taskqueue_.front());
                    this->taskqueue_.pop();
                }
                task();
               }
        });
    }
}

void ThreadPool::addtask(std::function<void()>task){
    {
        std::lock_guard<std::mutex>lock(mutex_);
        taskqueue_.push(task);
    }
    condition_.notify_one();
}

void ThreadPool::stop(){
    if(stop_)return;

    stop_ = true;
    condition_.notify_all();
    for(std::thread& th:threads_){
        if(th.joinable()){
            th.join();
        }
    }
}

ThreadPool::~ThreadPool(){stop();}

size_t ThreadPool::size(){return threads_.size();}