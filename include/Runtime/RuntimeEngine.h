#pragma once

#include "Scheduler.h"
#include "Worker.h"
#include "CancellationToken.h"

#include <atomic>
#include <chrono>
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

        // 关闭模式：
        //   GRACEFUL —— **drain + deadline**（业界标准，对标 K8s terminationGracePeriodSeconds）：
        //               先停收新任务，然后**不取消任何在途任务**，等"在途 + 排队"自然完成；
        //               只在超过 drainDeadline 后才取消剩余在途任务、把 pending 标失败。
        //               语义：deadline 内"每个请求都有真实结论"；超时后"每个请求至少有明确结论"。
        //   FORCE    —— 立刻取消在途、pending 全部标失败，秒级退出。
        enum class ShutdownMode
        {
            GRACEFUL,
            FORCE
        };

        // mode 默认 GRACEFUL；drainDeadline 只在 GRACEFUL 下生效。
        // 选了大的 deadline 就要接受"关闭可能等这么久"（这是 drain 语义的代价）。
        void stop(ShutdownMode mode = ShutdownMode::GRACEFUL,
                  std::chrono::milliseconds drainDeadline = std::chrono::seconds(10));

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

        // 把 pending 队列全部摘掉并标失败，返回需要在锁外调用的 completion。
        // 调用方必须持有 mutex_；forceDrained_ 由调用方负责置位。
        std::vector<std::pair<std::shared_ptr<RuntimeTask>, Completion>> drainPendingLocked();

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