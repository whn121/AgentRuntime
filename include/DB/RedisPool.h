#pragma once

#include <hiredis/hiredis.h>
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <memory>


class RedisPool
{
public:
    RedisPool(const std::string& host, int port, int pool_size = 4);
    ~RedisPool();

    std::shared_ptr<redisContext> getConnection();
    void releaseConnection(redisContext* conn);

private:
    struct State 
    {
        std::queue<redisContext*> pool;
        std::mutex mutex;
        std::condition_variable cv;
        bool closed{false};
        ~State();
    };
    std::shared_ptr<State> state_;
    static void release(const std::shared_ptr<State>& state, redisContext* conn);

};