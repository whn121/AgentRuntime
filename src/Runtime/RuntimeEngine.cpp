#include "Runtime/RuntimeEngine.h"
#include "Metrics/Metrics.h"
#include "Logger/AsyncLogger.h"

#include <stdexcept>
#include <utility>

namespace runtime
{
    RuntimeEngine::RuntimeEngine(Worker::Executor executor, std::size_t workerCount, std::size_t maxPendingTask)
                                : executor_ (std::move (executor)), maxPendingTasks_ (maxPendingTask)
    {
        if (!executor_)
        {
            throw std::invalid_argument("RuntimeEngine requires an executor");
        }

        if (workerCount == 0)
        {
            throw std::invalid_argument("RuntimeEngine requires at least one worker");
        }

        if (maxPendingTasks_ == 0)
        {
            throw std::invalid_argument("maxPendingTasks must be greater than zero");
        }

        try 
        {
            workers_.reserve(workerCount);

            // 每个 worker 同时最多持有一个在途任务, activeTokens_ 的大小
            // 不会超过 workerCount。预分配之后 push_back 不会再分配内存,
            // 不会抛 bad_alloc, workerLoop 里就不需要额外的 try/catch。
            activeTokens_.reserve(workerCount);
            
            for (std::size_t i = 0; i < workerCount; ++i)
            {
                workers_.emplace_back ([this] {workerLoop();});
            }
        }
        catch (...)
        {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopping_ = true;
            }

            cv_.notify_all();

            for (auto& worker : workers_)
            {
                if (worker.joinable()) worker.join();
            }

            throw;
        }
    }

    RuntimeEngine::~RuntimeEngine()
    {
        stop();
    }

    bool RuntimeEngine::submit (Scheduler::ClientId clientId, std::string input, Completion completion)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (stopping_)
        {
            Metrics::runtime_rejected_stopping.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        if (scheduler_.pendingCount() >= maxPendingTasks_)
        {
            Metrics::runtime_rejected_capacity.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        const TaskId id = nextTaskId_.fetch_add(1, std::memory_order_relaxed);

        // 先登记回调, 在提交任务; 提交失败回滚回调
        auto inserted = completions_.emplace (id, std::move (completion));
        if (!inserted.second)
        {
            return false;
        }

        try
        {
            if (!scheduler_.submit(id, clientId, std::move (input)))
            {
                completions_.erase (id);
                return false;
            }
        }
        catch(...)
        {
            completions_.erase (id);
            return false;
        }
        
        cv_.notify_one();
        return true;
    }

    void RuntimeEngine::workerLoop()
    {
        Worker worker(executor_);

        for (;;)
        {
            std::shared_ptr<RuntimeTask> task;
            Completion completion;

            {
                std::unique_lock<std::mutex> lock(mutex_);

                cv_.wait(lock, [this]
                {
                    return stopping_ || scheduler_.pendingCount() > 0;
                });

                // 停止时不再接收任务, 但会排空已接收的队列
                if (stopping_ && scheduler_.pendingCount() == 0)
                {
                    return;
                }

                task = scheduler_.dispatchNext();
                if (!task) continue;

                auto it = completions_.find (task->id());
                if (it != completions_.end())
                {
                    completion = std::move (it->second);
                    completions_.erase(it);
                }

                ++activeCount_;
                
                // 登记在途任务的令牌, stop() 时靠它取消
                // 不登记的话 stop() 遍历空集合, executor_ 看不到 cancelled()
                activeTokens_.push_back(task->token());
                Metrics::runtime_active.store(activeCount_, std::memory_order_relaxed);
            }

            WorkerExecutionResult result = WorkerExecutionResult::FAILED;

            try
            {
                result = worker.execute(task);
            }
            catch(...)
            {
                // 兜底处理Worker外围的意外异常
                try
                {
                    if (task && !task->isTerminal())
                    {
                        task->fail("UNEXPECTED_RUNTIME_WORKER_EXCEPTION");
                    }
                }
                catch(...)
                {
                    // 不允许异常逃出线程入口。
                }
                
                result = WorkerExecutionResult::FAILED;
            }

            // 不变式守卫：经过 execute 之后，任务必须处于终态。
            // 否则它既不会被再次调度，也永远不会被 eraseTerminal 回收，
            // 会作为孤儿永久留在 Scheduler::tasks_ 里。
            // 这里是所有执行路径的唯一收口，将来谁漏标终态都能被兜住并留下证据。
            if (task && !task->isTerminal())
            {
                LOG_ERROR("RuntimeEngine: task " + std::to_string(task->id())
                          + " left non-terminal after execute (state="
                          + toString(task->state()) + "); forcing FAILED");
                task->fail("TASK_DID_NOT_REACH_TERMINAL");
            }

            if (completion)
            {
                try
                {
                    completion(task, result);
                }
                catch(...)
                {
                    // 回调异常不能终止 Runtime 工作线程。
                }
                
            }

            if (task)
            {
                scheduler_.eraseTerminal(task->id());
            }
            
            {
                std::lock_guard<std::mutex> lock(mutex_);

                // 从活跃令牌集合移除, worker数很小, 线性查找成本很低
                for (auto tokenIt = activeTokens_.begin(); tokenIt != activeTokens_.end(); ++tokenIt)
                {
                    if (*tokenIt == task->token())
                    {
                        activeTokens_.erase(tokenIt);
                        break;
                    }
                }

                --activeCount_;
                Metrics::runtime_active.store(activeCount_, std::memory_order_relaxed);
            }

            cv_.notify_all();
        }
    }

        bool RuntimeEngine::isWorkerThread() const
    {
        const std::thread::id self = std::this_thread::get_id();
        for (const auto& worker : workers_)
        {
            if (worker.get_id() == self) return true;
        }
        return false;
    }

    std::vector<std::pair<std::shared_ptr<RuntimeTask>, RuntimeEngine::Completion>>
    RuntimeEngine::drainPendingLocked()
    {
        std::vector<std::pair<std::shared_ptr<RuntimeTask>, Completion>> failed;

        auto drained = scheduler_.drainAllPending();
        for (auto& task : drained)
        {
            if (!task) continue;

            // PENDING 不能直接 fail，先转 SCHEDULED
            task->schedule();
            task->fail("SERVER_SHUTTING_DOWN");

            auto it = completions_.find(task->id());
            if (it != completions_.end())
            {
                failed.emplace_back(task, std::move(it->second));
                completions_.erase(it);
            }
        }
        return failed;
    }

    void RuntimeEngine::stop(ShutdownMode mode, std::chrono::milliseconds drainDeadline)
    {
        // 在 worker 线程里调 stop()（例如 completion 回调里）不能 join 自己，
        // 也不能"等自己在途任务跑完"——那个在途任务就是当前这个回调，永远等不到。
        // 所以这种情况下只发信号，join 和 drain 都交给别的线程（通常是析构函数）。
        const bool called_from_worker = isWorkerThread();

        // 需要在锁外调用的 completion 回调
        std::vector<std::pair<std::shared_ptr<RuntimeTask>, Completion>> forcedFailures;

        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;

            // GRACEFUL：**先不取消**，让在途任务跑完（drain 语义）
            // FORCE：立刻取消在途 + 排空 pending
            if (mode == ShutdownMode::FORCE)
            {
                for (auto& token : activeTokens_)
                {
                    token.cancel();
                }

                if (!forceDrained_)
                {
                    forceDrained_ = true;
                    forcedFailures = drainPendingLocked();
                }
            }
        }

        // 锁外调用回调，避免 completion 内部拿 mutex_ 造成死锁
        for (auto& [task, cb] : forcedFailures)
        {
            if (cb)
            {
                try { cb(task, WorkerExecutionResult::FAILED); }
                catch (...) {}
            }
        }

        cv_.notify_all();

        // GRACEFUL（drain + deadline）：等"在途 + 排队"全部自然完成，最多等 deadline；
        // 超时才取消剩余在途、把 pending 标失败。
        // 注意：谓词里必须直接读 activeCount_ 成员（activeCount() 会重复锁 mutex_ 而死锁）；
        // scheduler_.pendingCount() 用的是 Scheduler 自己的锁，可以安全调用。
        if (mode == ShutdownMode::GRACEFUL && !called_from_worker)
        {
            std::unique_lock<std::mutex> lock(mutex_);

            const bool drained = cv_.wait_for(lock, drainDeadline, [this]
            {
                return scheduler_.pendingCount() == 0 && activeCount_ == 0;
            });

            if (!drained)
            {
                // 超时兜底：取消在途 + pending 全部标失败（等价于 FORCE 的收尾）
                for (auto& token : activeTokens_)
                {
                    token.cancel();
                }

                if (!forceDrained_)
                {
                    forceDrained_ = true;
                    forcedFailures = drainPendingLocked();
                }

                lock.unlock();

                for (auto& [task, cb] : forcedFailures)
                {
                    if (cb)
                    {
                        try { cb(task, WorkerExecutionResult::FAILED); }
                        catch (...) {}
                    }
                }

                cv_.notify_all();
                lock.lock();
            }
        }

        // 在 worker 线程里调 stop()：只发信号，join 交给析构函数
        if (called_from_worker)
        {
            return;
        }

        // join 只做一次：多个线程同时 stop() 时，后来的直接返回
        std::lock_guard<std::mutex> joinLock(joinMutex_);
        if (joined_) return;

        for (auto& worker : workers_)
        {
            if (worker.joinable()) worker.join();
        }
        joined_ = true;
    }

    void RuntimeEngine::cancelByClient(Scheduler::ClientId clientId)
    {
        // 收集需要在锁外调用的 completion 回调
        std::vector<std::pair<std::shared_ptr<RuntimeTask>, Completion>> cancelled;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            // 停止中时不动，stop() 会处理所有 pending
            if (stopping_)
            {
                return;
            }

            auto drained = scheduler_.drainByClient(clientId);
            for (auto& task : drained)
            {
                if (!task) continue;

                // PENDING 不能直接 fail，先转 SCHEDULED
                task->schedule();
                task->fail("CLIENT_DISCONNECTED");

                auto it = completions_.find(task->id());
                if (it != completions_.end())
                {
                    cancelled.emplace_back(task, std::move(it->second));
                    completions_.erase(it);
                }
            }
        }

        // 锁外调回调，避免 completion 内部拿 mutex_ 造成死锁
        for (auto& [task, cb] : cancelled)
        {
            if (cb)
            {
                try { cb(task, WorkerExecutionResult::FAILED); }
                catch (...) {}
            }
        }
    }

    std::size_t RuntimeEngine::pendingCount() const
    {
        return scheduler_.pendingCount();
    }

    std::size_t RuntimeEngine::activeCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return activeCount_;
    }
}