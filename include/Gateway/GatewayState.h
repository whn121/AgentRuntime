#pragma once

#include <string>

#include "Gateway/PendingRequest.h"
#include "Protocol/MyProtocol.h"



//网关状态,把要转发的打包到一起,方便,不会漏
struct GatewayState
{
    // 后端: 地址 + Connection 支持后来后端获得
    struct BackendEntry
    {
        std::string addr;
        std::shared_ptr<Connection> conn;

        // 健康状态
        long long last_pong_ms{std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count()}; // 最后收到heartbeat_resp的时间
        bool healthy{true};
        uint64_t request_count{0}; // 这个后端处理了多少请求
    };

    // 用于轮询
    std::vector<BackendEntry> backends;
    std::mutex backends_mutex;

    // 请求映射:newid->(客户端连接 + id)
    std::unordered_map<uint32_t, PendingRequest> pending;
    std::mutex pending_mutex;

    // 全局requestId 生成器
    std::atomic<uint64_t> next_new_id{1};

    // 轮询索引
    std::atomic<size_t> rr_index{0};

    // 所有后端的fd (用于区分消息时客户端来的还是后端来的)
    std::unordered_map<int, std::weak_ptr<Connection>> backend_fds;
    std::mutex backend_fds_mutex;

    // 选一个后端 (轮询)
    std::shared_ptr<Connection> pick_backend()
    {
        std::lock_guard<std::mutex> lock(backends_mutex);
        if (backends.empty()) return nullptr;

        // 跳过unhealthy, 最多尝试size次
        size_t n = backends.size();
        for (size_t i = 0; i < n; ++i)
        {
            size_t idx = rr_index++ % n;
            if (backends[idx].healthy && backends[idx].conn && backends[idx].conn->isConnected())
            {
                backends[idx].request_count++;
                return backends[idx].conn;
            }
        }

        return nullptr; // 全部不健康
    }

    // 统计指标
    struct Metrics
    {
        std::atomic<uint64_t> total_requests{0}; // 客户端请求总数
        std::atomic<uint64_t> forwarded{0}; // 成功转发数
        std::atomic<uint64_t> no_backend{0}; // 请求因没有健康后端的数量
        std::atomic<uint64_t> backend_responses{0}; // 后端相应数
        std::atomic<uint64_t> pending_timeouts{0}; // pending表超时数
        std::atomic<uint64_t> unknown_responses{0}; // 未知相应 (没人等的相应)
        std::atomic<uint64_t> dropped_client_gone{0}; // 客户端已经断开 (响应到了但客户端断了)
    };
    Metrics metrics;

    void fail_backend(const std::shared_ptr<Connection>& failed_conn) 
    {
        if (!failed_conn) return;

        std::vector<std::pair<std::shared_ptr<Connection>, uint32_t>> failed;
        {
            std::lock_guard<std::mutex> lock(pending_mutex);
            for (auto it = pending.begin(); it != pending.end();) 
            {
                auto backend = it->second.backend_conn.lock();
                if (backend && backend == failed_conn) 
                {
                    if (auto client = it->second.client_conn.lock())
                        failed.emplace_back(client, it->second.original_id);
                    it = pending.erase(it);
                } 
                else ++it;
            }
        }
        for (auto& entry : failed)
            entry.first->sendResponse(MyMessage{MyType::ERROR_RESP, entry.second, "BACKEND_DISCONNECTED"});
    }

    // 检查地址是否已经链接
    bool has_addr(const std::string& addr)
    {
        std::lock_guard<std::mutex> lock(backends_mutex);
        for (auto& e : backends) if (e.addr == addr) return true;
        return false;
    }
};
