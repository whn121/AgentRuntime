#include "ThreadPool/WorkThreadPool.h"
#include "Logger/AsyncLogger.h"

#include <utility>


WorkThreadPool::WorkThreadPool(int num) : num_(num > 0 ? num : 4) 
{
    try {
        for (int i = 0; i < num_; ++i)
            threads_.emplace_back([this] { worker(); });
    } catch (...) { stop(); throw; }
}

WorkThreadPool::~WorkThreadPool() { stop(); }


TaskQueue::PushResult WorkThreadPool::submit(Task&& task)
{
    const auto result = queue_.tryPush(std::move(task));

    if (result == TaskQueue::PushResult::Success) return result;
    if (result == TaskQueue::PushResult::Full)
    {
        LOG_WARN("WorkThreadPool: rejected task because the queue is full");
    }
    else 
    {
        LOG_WARN("WorkThreadPool: rejected task because the queue is stopped");
    }

    // 拒绝任务由调用方负责后续处理。
    // 不执行 run_，也不调用 onComplete_，避免将拒绝误判为正常完成。
    return result;
}


void WorkThreadPool::worker() 
{
    while (true) 
    {
        Task task = queue_.pop();         // 阻塞获取任务
        
        if (!task.run_)
        {
            // pop() 返回空 Task = 队列空 + 已停止
            // 说明所有任务都处理完了，可以退出
            break;
        }

        // 关键：包 try-catch，防止业务抛异常把工作线程弄死
        try 
        {
            task.run_();
        } 
        catch (const std::exception& e) 
        {
            LOG_ERROR("WorkThreadPool: task.run_ threw: " + std::string(e.what()));
        } 
        catch (...) 
        {
            LOG_ERROR("WorkThreadPool: task.run_ threw unknown exception");
        }    

        // onComplete_ 也要保护
        // 否则 FIFO 链条会断，后续 task 永远不执行
        if (task.onComplete_) 
        {
            try 
            {
                task.onComplete_();
            } 
            catch (const std::exception& e) 
            {
                LOG_ERROR("WorkThreadPool: task.onComplete_ threw: " + std::string(e.what()));
            } 
            catch (...) 
            {
                LOG_ERROR("WorkThreadPool: task.onComplete_ threw unknown exception");
            }
        }
    }
}

void WorkThreadPool::stop()
{
    bool expected = false;
    if (!stop_.compare_exchange_strong (expected, true)) return;

    queue_.stop();  // 唤醒所有等待线程
    
    for (auto& t : threads_) if (t.joinable()) t.join();
}