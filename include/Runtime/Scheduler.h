#pragma once 

#include "RuntimeTask.h"

#include <cstddef>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>


namespace runtime
{
    class Scheduler
    {
    public:
        Scheduler() = default;

        // 提交一个新任务。
        // 返回 false 表示任务 ID 已存在，不覆盖已有任务。
        bool submit(TaskId id, std::string input);

        // 从 FIFO 队列取出下一个待调度任务。
        // 成功时任务状态从 PENDING 转为 SCHEDULED。
        // 没有可调度任务时返回 nullptr。
        std::shared_ptr<RuntimeTask> dispatchNext();

        // 按 ID 查找任务；找不到时返回 nullptr。
        std::shared_ptr<RuntimeTask> find(TaskId id) const;

        // 仅删除已进入终态（SUCCEEDED / FAILED）的任务。
        // 正在执行或等待调度的任务不会被删除。
        bool eraseTerminal(TaskId id);

        // 当前进程中保存的任务总数，包括已经结束的任务。
        std::size_t taskCount() const;

        // 当前等待调度的队列长度。
        std::size_t pendingCount() const;

    private:
        // 保护tasks_和rending_queue_
        mutable std::mutex mutex_;

        // shared_ptr 让外部持有任务引用时，对象不会立即销毁。
        std::unordered_map<TaskId, std::shared_ptr<RuntimeTask>> tasks_;

        // 队列只存 TaskId，不重复存储任务内容。
        // push_back + pop_front 实现 FIFO。
        std::deque<TaskId> pending_queue_;
    };
}