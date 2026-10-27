#pragma once

#include "Scheduler.h"
#include "Worker.h"
#include "CancellationToken.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>


namespace runtime
{
    class RuntimeEngine
    {
    public:
        using Completion = std::function<void (const std::shared_ptr<RuntimeTask>&, WorkerExecutionResult)>;

        explicit RuntimeEngine(Worker::Executor executor, std::size_t workerCount = 2, std::size_t maxPendingTasks = 1024);

        ~RuntimeEngine();

        RuntimeEngine(const RuntimeEngine&) = delete;
        RuntimeEngine& operator=(const RuntimeEngine&) = delete;

        // clientId 用于公平调度与按客户端的队列配额。
        bool submit(Scheduler::ClientId clientId, std::string input, Completion completion);

        // 停止接收新任务, 等待已接收任务执行结束
        void stop();

        std::size_t pendingCount() const;
        std::size_t activeCount() const;

    private:
        void workerLoop();

        Scheduler scheduler_;
        Worker::Executor executor_;
        const std::size_t maxPendingTasks_;

        mutable std::mutex mutex_;
        std::condition_variable cv_;
        std::vector<std::thread> workers_;
        std::unordered_map<TaskId, Completion> completions_;

        std::atomic<TaskId> nextTaskId_{1};
        std::size_t activeCount_{0};
        bool stopping_{false};
        std::once_flag stopOnce_;

        // 当前正在执行的任务的令牌。由 mutex_ 保护
        // worker 取出任务后加入, 执行完成后移除
        std::vector<CancellationToken> activeTokens_;
    };
}