#pragma once

#include "RuntimeTask.h"

#include <functional>
#include <memory>
#include <string>

namespace runtime
{
    // 代表worker执行一次任务后的结果
    enum class WorkerExecutionResult
    {
        REJECTED, // 任务不合法，或任务不能进入 RUNNING。
        SUCCEEDED, // 任务执行成功
        FAILED  // 任务执行器抛出异常，任务被标记为失败
    };

    class Worker
    {
    public:
        // 执行器接收任务输入，返回任务输出。
        // 当前用普通 C++ 函数模拟业务执行，后续可替换成 AI 调用。
        using Executor = std::function <std::string (const std::string&, const CancellationToken&)>;

        explicit Worker (Executor executor);

        // 执行一个已经由Scheduler调用的任务
        WorkerExecutionResult execute (const std::shared_ptr<RuntimeTask>& task);

    private:
        // 保存执行逻辑, 不负责管理任务队列
        Executor executor_;

    };
}