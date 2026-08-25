#pragma once
#include "Channel.h"
#include "Timer.h"
#include <set>
#include <vector>
#include <sys/timerfd.h>
class EventLoop;

class TimerQueue{

    public:
    using TimerCallback = std::function<void()>;
    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerId runAfter(double delay,TimerCallback cb);    //一次性
    TimerId runEvery(double interval,TimerCallback cb); //重复
    TimerId runAt(Timestamp when,TimerCallback cb);    //一次性（绝对时间)
    void cancel(TimerId id);

    private:
    struct Entry{
        Timestamp expiration;
        int64_t sequence;   //直接存序列号，比较器不解引用 Timer*（避免哨兵解引用 UB）
        Timer* timer;
        bool operator<(const Entry&rhs)const{
            if(expiration!=rhs.expiration) return expiration<rhs.expiration;
            return sequence<rhs.sequence;
        }
    };
    using TimerList = std::set<Entry>;
    using ActiveTimer = std::pair<Timer*,int64_t>;
    using ActiveTimerSet = std::set<ActiveTimer>;

    void handleRead();
    std::vector<Entry>getExpired(Timestamp now);
    void reset(const std::vector<Entry>& expired,Timestamp now);
    bool insert(Timer* timer);
    TimerId addTimer(TimerCallback cb,Timestamp when,double interval);  //run*的统一入口

    EventLoop* loop_;
    const int timerfd_;             // 注意声明在 timerfdChannel_ 之前（后者用它初始化）
    Channel timerfdchannel_;        //按值持有，对齐Acceptor::acceptchannel_的写法
    TimerList timers_;                  // 按到期时间排序
    ActiveTimerSet activeTimers_;           // 与 timers_ 同步，供 cancel 快速查找
    bool callingExpiredTimers_ = false;
    ActiveTimerSet cancelingTimers_;    //处理回调内自取消的重入场景
};