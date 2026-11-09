#pragma once

#include "DB/MySQLPool.h"

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
    // 与 agent_task.status 对应
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
        uint64_t client_id{0};
        TaskStatus status{TaskStatus::Pending};
        std::string result;
        std::string error;
    };

    // 任务结果持久化 + 幂等查询。
    //
    // 为什么需要它：Agent 任务可能跑几十秒。客户端断开后，运行时会把任务取消，
    // 结果就丢了；客户端超时重试又会重复消耗模型。落库之后：
    //   - 断开重连可以按 request_id 查结果（幂等）
    //   - 唯一键 request_id 保证重复写不会产生多行
    //
    // 为什么写是异步的：写库在"任务完成回调"里触发，那是引擎线程。
    // 一次写可能几毫秒，不能让它拖慢任务完成通知和后续调度。
    // 队列满时【丢弃并计数】——DB 是旁路，不能反过来把主链路打挂。
    class TaskStore
    {
    public:
        explicit TaskStore(std::shared_ptr<db::MySQLPool> pool);
        ~TaskStore();

        TaskStore(const TaskStore&) = delete;
        TaskStore& operator=(const TaskStore&) = delete;

        bool Ready() const { return ready_; }
        const std::string& Error() const { return error_; }

        // 异步投递一条终态记录（不阻塞调用线程）
        void SubmitTerminal(uint32_t request_id, uint64_t client_id, TaskStatus status,
                            const std::string& result, const std::string& error);

        // 同步查询（在业务线程调用，不在 IO 线程）
        bool Query(uint32_t request_id, TaskRecord& out);

        std::uint64_t Written() const { return written_.load(); }
        std::uint64_t Dropped() const { return dropped_.load(); }
        std::uint64_t FailedWrites() const { return failed_writes_.load(); }
        std::size_t QueueDepth() const;

        static constexpr std::size_t kMaxQueue = 4096;

    private:
        void WriterLoop();
        bool WriteOne(const TaskRecord& rec);
        bool EnsureSchema();

        std::shared_ptr<db::MySQLPool> pool_;
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
    };
}
