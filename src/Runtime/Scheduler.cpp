#include "Runtime/Scheduler.h"

#include <utility>

namespace runtime
{
    bool Scheduler::submit (TaskId id, std::string input)
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
        
        // 将 ID 加入 FIFO 队列
        // 如果队列扩容时抛出异常，回滚刚刚插入的任务
        // 避免任务表里出现一个永远不会被调度的任务
        try
        {
            pending_queue_.push_back(id);
        }
        catch(...)
        {
            tasks_.erase(id);
            throw;
        }
        
        return true;
    }

    std::shared_ptr<RuntimeTask> Scheduler::dispatchNext()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 队列为空意味着当前没有等待调度的任务
        while (!pending_queue_.empty())
        {
            const TaskId id = pending_queue_.front();
            pending_queue_.pop_front();

            // 根据队列中的id找到唯一的任务对象
            const auto it = tasks_.find (id);

            // 正常情况下队列 ID 必须存在于任务表中
            // 如果内部数据意外不一致，则跳过该 ID，避免反复卡住
            if (it == tasks_.end()) continue;


            // 只有 PENDING 任务可以进入 SCHEDULED
            // 如果状态已被其他代码改变，这个队列项就不能再调度
            if (!it->second->schedule()) continue;

            return it->second;

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

    std::size_t Scheduler::taskCount() const 
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return tasks_.size();
    }

    std::size_t Scheduler::pendingCount() const 
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_queue_.size();
    }

}