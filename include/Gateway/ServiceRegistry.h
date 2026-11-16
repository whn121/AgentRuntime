#pragma once

#include "ServiceRegistry/ServiceInstance.h"
#include "DB/RedisPool.h"
#include <vector>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <atomic>
#include <memory>
#include <string>


class ServiceRegistry
{
public:
    ServiceRegistry(std::shared_ptr<RedisPool> pool, const std::string& service_name);
    ~ServiceRegistry();

    ServiceRegistry(const ServiceRegistry&) = delete;
    ServiceRegistry& operator=(const ServiceRegistry&) = delete;

    void Start(int refresh_interval_sec = 2);
    void Stop();

    // 线程安全：返回当前快照
    std::vector<ServiceInstance> GetSnapshot() const;
    size_t Size() const;

    // 启动时注入静态后端（Redis 不可用时的兜底）；非空才生效
    void SeedStatic(const std::vector<ServiceInstance>& seeds);

private:
    void Refresh(); // 从redis把服务注册的信息读入内存
    void RefreshLoop(int interval_sec);

    std::shared_ptr<RedisPool> pool_;
    std::string service_name_;

    mutable std::mutex mutex_;
    std::vector<ServiceInstance> instances_;

    std::thread refresh_thread_;
    std::size_t empty_streak_{0};   // 连续空快照计数，防 Redis 数据丢失导致误删
    std::atomic<bool> stop_ {false};
    std::mutex lifecycle_mutex_;
    std::mutex wait_mutex_;
    std::condition_variable wait_cv_;
};
