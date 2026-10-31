#pragma once 

#include "RuntimeTask.h"

#include <cstddef>
#include <deque>
#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>


namespace runtime
{
    class Scheduler
    {
    public:

        // 客户端标识：当前使用连接 fd。fd 会被复用，但复用时旧连接的任务
        // 已进入终态并从队列移除，不会与新连接的任务混排。
        using ClientId = int;    

        Scheduler() = default;

        // clientId 用于公平调度：同一客户端的任务在自己的队列里 FIFO
        // 不同客户端之间轮转取出
        // 返回 false 表示：任务 ID 已存在，或该客户端的待调度队列已满
        bool submit(TaskId id, ClientId clientId, std::string input);

        // 每个客户端最多等待调度的任务数。
        // 超过后 submit 返回 false，调用方（RuntimeEngine::submit）
        // 将失败传播到 backend_main.cpp 的 TASK_SUBMISSION_FAILED。
        static constexpr std::size_t MAX_PENDING_PER_CLIENT = 256;

        // 从 FIFO 队列取出下一个待调度任务。
        // 成功时任务状态从 PENDING 转为 SCHEDULED。
        // 没有可调度任务时返回 nullptr。
        std::shared_ptr<RuntimeTask> dispatchNext();

        // 按 ID 查找任务；找不到时返回 nullptr。
        std::shared_ptr<RuntimeTask> find(TaskId id) const;

        // 仅删除已进入终态（SUCCEEDED / FAILED）的任务。
        // 正在执行或等待调度的任务不会被删除。
        bool eraseTerminal(TaskId id);

        // 把所有还没被调度的任务从队列里取出并返回
        // 被取出的任务在 Scheduler 里不再被跟踪，状态仍然是 PENDING
        // 调用方负责把它们标记为失败并通知 completion
        // FORCE 关闭时用
        std::vector<std::shared_ptr<RuntimeTask>> drainAllPending();

        // 取出某个客户端的所有待调度任务。用于客户端断开时清理。
        // 被取出的任务从 tasks_ 和 perClientQueues_ 中移除。
        std::vector<std::shared_ptr<RuntimeTask>> drainByClient(ClientId clientId);

        // 当前进程中保存的任务总数，包括已经结束的任务。
        std::size_t taskCount() const;

        // 当前等待调度的队列长度。
        std::size_t pendingCount() const;


    private:
        // 保护tasks_和rending_queue_
        mutable std::mutex mutex_;

        // shared_ptr 让外部持有任务引用时，对象不会立即销毁。
        std::unordered_map<TaskId, std::shared_ptr<RuntimeTask>> tasks_;

        // 每个客户端一个 FIFO 队列，只存 TaskId。
        std::unordered_map<ClientId, std::deque<TaskId>> perClientQueues_;

        // 轮转顺序：队头客户端先被取出，取完后若仍有任务则移到队尾。
        std::deque<ClientId> roundRobin_;

        // 总待调度数，用于 pendingCount() 的 O(1) 返回。
        std::size_t pendingTotal_ = 0;

    };
}