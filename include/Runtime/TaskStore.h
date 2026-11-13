#pragma once

#include "DB/MySQLPool.h"
#include "DB/RedisPool.h"

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace store
{
    enum class TaskStatus : int
    {
        Pending = 0,
        Running = 1,
        Succeeded = 2,
        Failed = 3,
        Cancelled = 4
    };

    struct TaskRecord
    {
        uint32_t request_id{0};
        uint64_t session_id{0};
        uint64_t client_id{0};
        TaskStatus status{TaskStatus::Pending};
        std::string result;
        std::string error;
    };

    // 任务持久化 + 幂等 + 会话计数 + 缓存。
    //
    // 四件事各自解决一个真问题：
    //   1) 幂等（TryClaim）：同一 request_id 只受理一次，事务里"插任务 + 会话计数+1"；
    //   2) 会话计数：agent_session.task_count，和任务插入在【同一事务】，不会出现
    //      "任务有了但计数没加"；
    //   3) 终态落库：异步写（队列满丢弃计数），不拖慢引擎线程；
    //   4) 缓存：cache-aside（先更库、再删缓存），空结果也缓存（防穿透）。
    class TaskStore
    {
    public:
        explicit TaskStore(std::shared_ptr<db::MySQLPool> pool,
                           std::shared_ptr<RedisPool> redis = nullptr);
        ~TaskStore();

        TaskStore(const TaskStore&) = delete;
        TaskStore& operator=(const TaskStore&) = delete;

        bool Ready() const { return ready_; }
        const std::string& Error() const { return error_; }

        // 受理：幂等占位 + 会话计数（同一事务）。
        // 返回 false 表示重复提交（已存在该 request_id）。
        bool TryClaim(uint32_t request_id, uint64_t session_id, uint64_t client_id);

        void SubmitTerminal(uint32_t request_id, uint64_t session_id, uint64_t client_id,
                            TaskStatus status, const std::string& result, const std::string& error);

        // 查询：先查缓存，未命中查库并回填（空结果也缓存，TTL 更短）
        bool Query(uint32_t request_id, TaskRecord& out);

        std::uint64_t Written() const { return written_.load(); }
        std::uint64_t Dropped() const { return dropped_.load(); }
        std::uint64_t FailedWrites() const { return failed_writes_.load(); }
        std::uint64_t CacheHits() const { return cache_hits_.load(); }
        std::uint64_t CacheMisses() const { return cache_misses_.load(); }

        static constexpr std::size_t kMaxQueue = 4096;

    private:
        void WriterLoop();
        bool WriteOne(const TaskRecord& rec);
        bool EnsureSchema();
        void CacheInvalidate(uint32_t request_id);

        std::shared_ptr<db::MySQLPool> pool_;
        std::shared_ptr<RedisPool> redis_;
        bool ready_{false};
        std::string error_;

        mutable std::mutex mutex_;
        std::condition_variable cv_;
        std::deque<TaskRecord> queue_;
        bool stop_{false};
        std::thread writer_;

        std::atomic<std::uint64_t> written_{0};
        std::atomic<std::uint64_t> dropped_{0};
        std::atomic<std::uint64_t> failed_writes_{0};
        std::atomic<std::uint64_t> cache_hits_{0};
        std::atomic<std::uint64_t> cache_misses_{0};
    };
}
