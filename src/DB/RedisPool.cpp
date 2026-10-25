#include "DB/RedisPool.h"
#include "Logger/AsyncLogger.h"
#include <chrono>
#include <stdexcept>

RedisPool::State::~State()
{
    while (!pool.empty()) 
    {
        redisFree(pool.front());
        pool.pop();
    }
}

RedisPool::RedisPool(const std::string& host, int port, int pool_size)
    : state_(std::make_shared<State>())
{
    if (host.empty() || port < 1 || port > 65535 || pool_size < 1)
        throw std::invalid_argument("invalid Redis pool configuration");
    timeval timeout{1, 0};
    for (int i = 0; i < pool_size; ++i)
    {
        redisContext* conn = redisConnectWithTimeout(host.c_str(), port, timeout);
        if (!conn || conn->err || redisSetTimeout(conn, timeout) != REDIS_OK) 
        {
            if (conn) redisFree(conn);
            continue;
        }
        state_->pool.push(conn);
    }
    if (state_->pool.empty()) throw std::runtime_error("Redis connection pool initialization failed");
}

RedisPool::~RedisPool()
{
    {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->closed = true;
    }
    state_->cv.notify_all();
}

std::shared_ptr<redisContext> RedisPool::getConnection()
{
    auto state = state_;
    std::unique_lock<std::mutex> lock(state->mutex);
    state->cv.wait_for(lock, std::chrono::milliseconds(500),
        [&] { return state->closed || !state->pool.empty(); });
    if (state->closed || state->pool.empty()) return nullptr;
    auto* conn = state->pool.front();
    state->pool.pop();
    lock.unlock();
    // hiredis 上下文可使用初始的服务端地址与超时参数进行重连。
    if (conn->err && redisReconnect(conn) != REDIS_OK) 
    {
        lock.lock();
        state->pool.push(conn);
        state->cv.notify_one();
        return nullptr;
    }
    return std::shared_ptr<redisContext>(conn, [state](redisContext* c) { release(state, c); });
}

void RedisPool::release(const std::shared_ptr<State>& state, redisContext* conn)
{
    if (!conn) return;
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->closed) 
    {
        redisFree(conn);
        return;
    }
    state->pool.push(conn);
    state->cv.notify_one();
}

void RedisPool::releaseConnection(redisContext* conn)
{
    release(state_, conn);
}