#include "Runtime/Worker.h"

#include <exception>
#include <stdexcept>
#include <utility>


namespace runtime
{
    Worker::Worker(Executor exexutor) : executor_ (std::move (exexutor))
    {
        // 没有执行器时无法执行任务
        // 尽早暴露配置错误,而不是等到任务到来才失败
        if (!executor_)
        {
            throw std::invalid_argument ("Worker requires a valid executor");
        }
    }

    WorkerExecutionResult Worker::execute (const std::shared_ptr<RuntimeTask>& task)
    {
        if (!task)
        {
            return WorkerExecutionResult::REJECTED;
        }

        // 只能执行已经被Scheduler调度的任务
        // start()负责校验状态并完成SCHEDULLED->RUNNING
        if (!task->start())
        {
            return WorkerExecutionResult::REJECTED;
        }

        try
        {
            // 执行具体业务逻辑
            // Worker不关注输入代表啥,也不关心输出如何生产
            std::string result = executor_ (task->input(), task->token());

            // 只有RUNNING状态才能被标记为成功
            if (!task->succeed (std::move (result)))
            {
                // 在当前单线程, 无外部状态修改的模型中
                // 这里正常情况下不应该失败
                // 未来并发, 必须重新设计状态同步
                return WorkerExecutionResult::REJECTED;
            }

            return WorkerExecutionResult::SUCCEEDED;
        }
        catch(const std::exception& e)
        {
            // 将常见c++异常转换成任务失败状态
            task->fail(e.what());
            return WorkerExecutionResult::FAILED;
        }
        catch(...)
        {
            // 防止非std::exception异常直接逃出执行边界
            task->fail("unknown exception");
            return WorkerExecutionResult::FAILED;
        }
        
    }
}