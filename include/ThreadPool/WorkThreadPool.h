#pragma once
#include "TaskQueue.h"
#include <vector>
#include <thread>
#include <functional>
#include <atomic>

// 业务线程池：执行具体的业务逻辑
class WorkThreadPool 
{
public:
    WorkThreadPool(int num = 4);
    ~WorkThreadPool();
    // 提交任务。
    // Success：任务已转交给队列。
    // Full / Stopped：拒绝任务，不移动 task，也不调用 task.onComplete_()。
    TaskQueue::PushResult submit(Task&& task);
    void stop();
private:
    void worker();            // 工作线程主函数
    int num_;
    std::atomic<bool> stop_{false};
    TaskQueue queue_;
    std::vector<std::thread> threads_;
};