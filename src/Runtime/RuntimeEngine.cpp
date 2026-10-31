#include "Runtime/RuntimeEngine.h"
#include "Metrics/Metrics.h"

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

    void RuntimeEngine::stop(ShutdownMode mode)
    {
        std::call_once(stopOnce_, [this, mode]
        {
            // 收集需要在锁外调用的commpletion回调
            std::vector<std::pair<std::shared_ptr<RuntimeTask>, Completion>> forcedFailures;

            {
                std::lock_guard<std::mutex> lock(mutex_);
                stopping_ = true;

                // 取消所有在正在执行的任务
                for (auto& token : activeTokens_)
                {
                    token.cancel();
                }

                // FORCE 模式：把 pending 队列里的任务全部取出
                // 标记失败，收集它们的 completion 回调
                if (mode == ShutdownMode::FORCE)
                {
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
                            forcedFailures.emplace_back(task, std::move(it->second));
                            completions_.erase(it);
                        }
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

            for (auto& worker : workers_)
            {
                if (worker.joinable()) worker.join();
            }
        });
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