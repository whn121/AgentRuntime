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

        // 关闭模式。GRACEFUL：等待已提交任务执行完；FORCE：立即丢弃所有
        // 尚未执行的任务，通知它们的 completion 回调以 FAILED，然后退出
        enum class ShutdownMode
        {
            GRACEFUL,
            FORCE
        };

        // mode 默认 GRACEFUL，向后兼容
        void stop(ShutdownMode mode = ShutdownMode::GRACEFUL);

        // 取消某个客户端的所有待调度任务 正在执行的任务不受影响
        // 被取消的任务会收到 FAILED 和错误码 "CLIENT_DISCONNECTED"
        void cancelByClient(Scheduler::ClientId clientId);

        std::size_t pendingCount() const;
        std::size_t activeCount() const;

    private:
        void workerLoop();

        // stop() 是否由本引擎的 worker 线程调用。
        // 是的话不能 join 自己，只能发完取消信号后返回，由析构函数负责 join。
        bool isWorkerThread() const;

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

        // FORCE 的排空只做一次；GRACEFUL 之后再调 FORCE 仍可以升级
        bool forceDrained_{false};
        // worker 是否已经 join 过；stop() 可重入，join 只做一次
        bool joined_{false};
        std::mutex joinMutex_;

        // 当前正在执行的任务的令牌。由 mutex_ 保护
        // worker 取出任务后加入, 执行完成后移除
        std::vector<CancellationToken> activeTokens_;
    };
}