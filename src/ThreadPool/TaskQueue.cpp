#include "ThreadPool/TaskQueue.h"

void TaskQueue::push(Task task) 
{
    const auto result = tryPush(std::move (task));

    if (result != PushResult::Success && task.onComplete_)
    {
        task.onComplete_();
    }
}


TaskQueue::PushResult TaskQueue::tryPush(Task&& task)
{
    {
        std::lock_guard<std::mutex> lock(mtx_);

        // 停止或队列已满时拒绝任务。
        // 必须在 move 之前检查，保证调用方仍能处理被拒绝的任务。
        if (stop_)
        {
            return PushResult::Stopped;
        }

        if (queue_.size() >= max_size_)
        {
            return PushResult::Full;
        }

        queue_.push(std::move(task));
    }

    // 在释放锁后唤醒一个等待的工作线程。
    cv_.notify_one();
    return PushResult::Success;
}


Task TaskQueue::pop() 
{
    std::unique_lock<std::mutex> lock(mtx_);
    // 等待条件：队列非空或已停止
    cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
    if (stop_ && queue_.empty()) return {};  // 停止且队列空返回空Task
    Task task = std::move(queue_.front());
    queue_.pop();
    return task;
}

void TaskQueue::stop() 
{
    std::lock_guard<std::mutex> lock(mtx_);
    stop_ = true;
    cv_.notify_all();  // 唤醒所有线程以便退出
}