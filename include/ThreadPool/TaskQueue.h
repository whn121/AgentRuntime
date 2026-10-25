#pragma once
#include <queue>
#include <mutex>
#include <condition_variable>
#include <cstddef>
#include "Task.h"

// 线程安全的任务队列（阻塞队列）
class TaskQueue 
{
public:
    // 帮助work区分队列满还是线程池正在停止
    enum class PushResult
    {
        Success,
        Stopped,
        Full
    };

    explicit TaskQueue(size_t max_size = 4096) : max_size_ (max_size > 0 ? max_size : 4096) {}
    // 队列停止后拒绝新的任务: 返回false时不移动task
    void push(Task task);
    // 尝试提交任务。
    // 队列停止后返回 false，且不移动 task，调用方可以处理被拒绝的任务。
    PushResult tryPush(Task&& task);
    Task pop();                 // 阻塞直到有任务或停止
    void stop();                // 唤醒所有等待并停止


private:
    std::queue<Task> queue_;
    std::mutex mtx_;
    std::condition_variable cv_;
    bool stop_ = false;

    const size_t max_size_;
};