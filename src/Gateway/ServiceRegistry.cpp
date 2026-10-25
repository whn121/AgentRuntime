#include "Gateway/ServiceRegistry.h"
#include "Logger/AsyncLogger.h"
#include <chrono>
#include <charconv>
#include <arpa/inet.h>
#include <stdexcept>
#include <unordered_set>


ServiceRegistry::ServiceRegistry(std::shared_ptr<RedisPool> pool, const std::string& service_name)
                                    :pool_ (std::move(pool)), service_name_ (service_name) {}


ServiceRegistry::~ServiceRegistry()
{
    Stop();
}

void ServiceRegistry::Start(int refresh_interval_sec)
{
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    if (refresh_thread_.joinable()) return;
    if (refresh_interval_sec < 1 || !pool_) throw std::invalid_argument("invalid discovery configuration");
    stop_ = false;
    Refresh();

    refresh_thread_ = std::thread([this, refresh_interval_sec]
    {
        RefreshLoop(refresh_interval_sec);
    });
}

void ServiceRegistry::Stop()
{
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    {
        std::lock_guard<std::mutex> lock(wait_mutex_);
        stop_ = true;
    }
    wait_cv_.notify_all();
    if (refresh_thread_.joinable()) refresh_thread_.join();
}

std::vector<ServiceInstance> ServiceRegistry::GetSnapshot() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return instances_; // 返回拷贝
}

 size_t ServiceRegistry::Size() const
 {
    std::lock_guard<std::mutex> lock(mutex_);
    return instances_.size();
 }

 void ServiceRegistry::RefreshLoop(int interval_sec)
 {
    while (!stop_)
    {
        {
            std::unique_lock<std::mutex> lock(wait_mutex_);
            if (wait_cv_.wait_for(lock, std::chrono::seconds(interval_sec), 
                [this] { return stop_.load(); })) break;
        }
        try { Refresh(); }
        catch (const std::exception& e) { LOG_ERROR("Discovery refresh failed: " + std::string(e.what())); }
    }
 }

 void ServiceRegistry::Refresh()
 {
    auto conn = pool_->getConnection();
    if (!conn)
    {
        LOG_WARN("ServiceRegistry: Redis unavailable");
        return;
    }

    const std::string prefix = "services:" + service_name_ + ":";
    const std::string pattern = prefix + "*";
    std::unordered_set<std::string> keys;
    std::string cursor = "0";
    do {
        if (stop_) return;
        std::unique_ptr<redisReply, decltype(&freeReplyObject)> reply(
            static_cast<redisReply*>(redisCommand(conn.get(), "SCAN %s MATCH %s COUNT 100", cursor.c_str(), pattern.c_str())), freeReplyObject);
        if (!reply || reply->type != REDIS_REPLY_ARRAY || reply->elements != 2) return;
        auto* next = reply->element[0];
        auto* entries = reply->element[1];
        if (!next || next->type != REDIS_REPLY_STRING || !next->str
            || !entries || entries->type != REDIS_REPLY_ARRAY) return;
        cursor.assign(next->str, next->len);
        for (size_t i = 0; i < entries->elements; ++i) {
            auto* item = entries->element[i];
            if (item && item->type == REDIS_REPLY_STRING && item->str)
                keys.emplace(item->str, item->len);
        }
    } while (cursor != "0");

    std::vector<ServiceInstance> new_instances;
    for (const auto& key : keys)
    {
        if (stop_) return;
        if (key.compare(0, prefix.size(), prefix) != 0 || key.size() == prefix.size()) continue;
        std::string instance_id = key.substr(prefix.size());

        // 2.GET services:backend_service:node-9001  获得值
        std::unique_ptr<redisReply, decltype(&freeReplyObject)> val(
            static_cast<redisReply*>(redisCommand(conn.get(), "GET %b", key.data(), key.size())), freeReplyObject);

        if (!val || val->type == REDIS_REPLY_ERROR) return;
        if (val->type != REDIS_REPLY_STRING || !val->str) continue;

        std::string addr(val->str, val->len);

        // 3.解析"host:port"
        auto pos = addr.find(':');
        if (pos == std::string::npos) continue;

        ServiceInstance inst;
        inst.service_name = service_name_;
        inst.instance_id = instance_id;
        inst.host = addr.substr(0, pos);
        unsigned port = 0;
        const char* begin = addr.data() + pos + 1;
        auto parsed = std::from_chars(begin, addr.data() + addr.size(), port);
        in_addr ipv4{};
        if (parsed.ec != std::errc{} || parsed.ptr != addr.data() + addr.size()
            || port == 0 || port > 65535 || inst.host.find('\0') != std::string::npos
            || inet_pton(AF_INET, inst.host.c_str(), &ipv4) != 1) continue;
        inst.port = static_cast<uint16_t>(port);

        new_instances.push_back(std::move(inst));
    }

    // 4.更新本地缓存
    {
        std::lock_guard<std::mutex> lock(mutex_);
        instances_ = std::move(new_instances);
    }

    LOG_INFO("ServiceRegistry refreshed: " + std::to_string(Size()) + " instances");

 }