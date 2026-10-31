#include "Runtime/Scheduler.h"
#include "Metrics/Metrics.h"

#include <utility>
#include <vector>
#include <memory>

namespace runtime
{
    bool Scheduler::submit (TaskId id, ClientId clientId, std::string input)
    {
        // 先创建任务, 非法id会由runtimetask构造函数检测冰雹出异常
        auto task = std::make_shared<RuntimeTask> (id, std::move (input));

        std::lock_guard<std::mutex> lock (mutex_);

        // emplace 不会覆盖已有ID
        // 如果id已存在, 直接拒绝重复提交
        const auto inserted = tasks_.emplace(id, task); 
        if (!inserted.second)
        {
            return false;
        }
        
        // 按客户端配额检查, 配额满时回滚任务表, 返回false让上游拒绝
        auto& clientQueue = perClientQueues_[clientId];
        if (clientQueue.size() >= MAX_PENDING_PER_CLIENT)
        {
            tasks_.erase (id);
            Metrics::scheduler_rejected_quota.fetch_add(1, std::memory_order_relaxed);
            // 若该客户端队列位空则顺便map条目, 避免map无限增长
            if (clientQueue.empty())
            {
                perClientQueues_.erase (clientId);
            }
            return false;
        }
        
        try
        {
            clientQueue.push_back (id);

            // 首次出现的客户端加入轮转队列
            if (clientQueue.size() == 1)
            {
                roundRobin_.push_back (clientId);
            }
        }
        catch(...)
        {
            tasks_.erase (id);
            throw;
        }
        
        ++pendingTotal_;
        Metrics::scheduler_pending.store(pendingTotal_, std::memory_order_relaxed);
        return true;
    }

    std::shared_ptr<RuntimeTask> Scheduler::dispatchNext()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 队列为空意味着当前没有等待调度的任务
        while (!roundRobin_.empty())
        {
            const ClientId clientId = roundRobin_.front();
            roundRobin_.pop_front();

            auto it = perClientQueues_.find (clientId);
            if (it == perClientQueues_.end() || it->second.empty())
            {
                // 轮转条目与实际队列不一致时清理该条目, 继续下一个客户端
                if (it != perClientQueues_.end())
                {
                    perClientQueues_.erase (it);
                }
                continue;
            }

            auto& clientQueue = it->second;
            const TaskId id = clientQueue.front();
            clientQueue.pop_front();
            --pendingTotal_;
            Metrics::scheduler_pending.store(pendingTotal_, std::memory_order_relaxed);

            // 该客户端仍有任务，放回轮转队尾，保证其他客户端先被轮到。
            if (!clientQueue.empty())
            {
                roundRobin_.push_back (clientId);
            }
            else
            {
                perClientQueues_.erase (it);
            }

            const auto taskIt = tasks_.find (id);
            if (taskIt == tasks_.end()) continue;

            // 只有 PENDING 任务能进入 SCHEDULED。
            if (!taskIt->second->schedule()) continue;

            return taskIt->second;

        }

        return nullptr;
    }

    std::shared_ptr<RuntimeTask> Scheduler::find (TaskId id) const
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = tasks_.find(id);

        if (it == tasks_.end()) 
        {
            return nullptr;
        }

        return it->second;
    }

    bool Scheduler::eraseTerminal (TaskId id)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        const auto it = tasks_.find (id);
        if (it == tasks_.end())
        {
            return false;
        }

        // 只允许回收已经结束的任务
        if (!it->second->isTerminal())
        {
            return false;
        }

        tasks_.erase(it);
        return true;
    }

    std::vector<std::shared_ptr<RuntimeTask>> Scheduler::drainAllPending()
    {
        std::lock_guard<std::mutex> lock (mutex_);

        std::vector<std::shared_ptr<RuntimeTask>> drained;

        // 遍历所有客户端队列，把任务对象取出来
        for (auto& [clientId, queue] : perClientQueues_)
        {
            for (TaskId id : queue)
            {
                auto it = tasks_.find (id);
                if (it != tasks_.end() && it->second)
                {
                    drained.push_back (it->second);
                    tasks_.erase (it);
                }
            }
        }

        // 清空所有调度状态
        perClientQueues_.clear();
        roundRobin_.clear();
        pendingTotal_ = 0;
        Metrics::scheduler_pending.store (0, std::memory_order_relaxed);

        return drained;
    }

     std::vector<std::shared_ptr<RuntimeTask>> Scheduler::drainByClient(ClientId clientId)
    {
        std::lock_guard<std::mutex> lock (mutex_);

        std::vector<std::shared_ptr<RuntimeTask>> drained;

        auto it = perClientQueues_.find (clientId);
        if (it == perClientQueues_.end())
        {
            return drained;
        }

        const std::size_t count = it->second.size();

        for (TaskId id : it->second)
        {
            auto taskIt = tasks_.find (id);
            if (taskIt != tasks_.end() && taskIt->second)
            {
                drained.push_back (taskIt->second);
                tasks_.erase (taskIt);
            }
        }

        perClientQueues_.erase (it);

        // pendingTotal_ 是 size_t，避免下溢
        pendingTotal_ = (pendingTotal_ >= count) ? (pendingTotal_ - count) : 0;
        Metrics::scheduler_pending.store (pendingTotal_, std::memory_order_relaxed);

        // roundRobin_ 里每个 clientId 最多一个条目
        for (auto rIt = roundRobin_.begin(); rIt != roundRobin_.end(); ++rIt)
        {
            if (*rIt == clientId)
            {
                roundRobin_.erase (rIt);
                break;
            }
        }

        return drained;
    }

    std::size_t Scheduler::taskCount() const 
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return tasks_.size();
    }

    std::size_t Scheduler::pendingCount() const 
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return pendingTotal_;
    }

}