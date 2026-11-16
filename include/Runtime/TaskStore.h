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
        int attempt{0};
        std::string result;
        std::string error;
    };

    // 受理结果：把"重复提交"从二值改成有语义的状态机。
    //
    //   New        —— 第一次受理，应该执行
    //   Retry      —— 上次失败且未超重试上限，允许重新领取
    //   Processing —— 正在执行（Pending/Running），不要重复执行
    //   Done       —— 已有终态，直接返回结果
    //   Rejected   —— 失败且超过重试上限
    //   Error      —— DB 不可用等内部错误
    enum class ClaimOutcome
    {
        New,
        Retry,
        Processing,
        Done,
        Rejected,
        Error
    };

    struct ClaimResult
    {
        ClaimOutcome outcome{ClaimOutcome::Error};
        TaskRecord record;   // Done/Processing/Rejected 时带状态
    };

    class TaskStore
    {
    public:
        explicit TaskStore(std::shared_ptr<db::MySQLPool> pool,
                           std::shared_ptr<RedisPool> redis = nullptr,
                           int task_timeout_sec = 0,
                           int reap_interval_sec = 60);
        ~TaskStore();

        TaskStore(const TaskStore&) = delete;
        TaskStore& operator=(const TaskStore&) = delete;

        bool Ready() const { return ready_; }
        const std::string& Error() const { return error_; }

        // 幂等受理（状态机版）：
        //   1) 第一次 → 插 Pending，attempt=1，会话计数+1（同一事务）
        //   2) 已存在：
        //      Succeeded/Cancelled → Done（返回结果）
        //      Pending/Running     → Processing
        //      Failed 且 attempt < max_attempts → 原子改回 Pending、attempt+1 → Retry
        //      Failed 且 attempt >= max_attempts → Rejected
        ClaimResult TryClaim(uint32_t request_id, uint64_t session_id, uint64_t client_id,
                             int max_attempts = 3);

        // 执行前原子领取：Pending → Running。
        // 返回 false 表示被别人领走或已不在 Pending（防两个线程同时执行）。
        bool MarkRunning(uint32_t request_id);

        void SubmitTerminal(uint32_t request_id, uint64_t session_id, uint64_t client_id,
                            TaskStatus status, const std::string& result, const std::string& error);

        // 查询：先查缓存，未命中查库并回填（空结果也缓存，TTL 更短）
        bool Query(uint32_t request_id, TaskRecord& out);

        std::uint64_t Written() const { return written_.load(); }
        std::uint64_t Dropped() const { return dropped_.load(); }
        std::uint64_t FailedWrites() const { return failed_writes_.load(); }
        std::uint64_t CacheHits() const { return cache_hits_.load(); }
        std::uint64_t CacheMisses() const { return cache_misses_.load(); }
        std::uint64_t Retries() const { return retries_.load(); }

        static constexpr std::size_t kMaxQueue = 4096;

    private:
        void WriterLoop();
        // 把 Pending/Running 超过 task_timeout_sec_ 的任务标 Failed（防孤儿行）
        void ReapStale();
        bool WriteOne(const TaskRecord& rec);
        bool EnsureSchema();
        bool EnsureColumn(const char* table, const char* column, const char* ddl);
        bool ReadRecord(MYSQL* conn, uint32_t request_id, TaskRecord& out);
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
        int task_timeout_sec_{0};
        int reap_interval_sec_{60};

        std::atomic<std::uint64_t> written_{0};
        std::atomic<std::uint64_t> dropped_{0};
        std::atomic<std::uint64_t> failed_writes_{0};
        std::atomic<std::uint64_t> cache_hits_{0};
        std::atomic<std::uint64_t> cache_misses_{0};
        std::atomic<std::uint64_t> retries_{0};
    };
}
