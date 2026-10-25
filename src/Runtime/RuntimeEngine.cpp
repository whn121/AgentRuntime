#include "Runtime/RuntimeEngine.h"

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

    bool RuntimeEngine::submit (std::string input, Completion completion)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (stopping_ || scheduler_.pendingCount() >= maxPendingTasks_)
        {
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
            if (!scheduler_.submit(id, std::move (input)))
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
                --activeCount_;
            }

            cv_.notify_all();
        }
    }

    void RuntimeEngine::stop()
    {
        std::call_once(stopOnce_, [this]
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