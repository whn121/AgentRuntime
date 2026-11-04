#include "Runtime/Worker.h"

#include <exception>
#include <stdexcept>
#include <utility>


namespace runtime
{
    Worker::Worker(Executor executor) : executor_ (std::move (executor))
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
                // 正常情况下走不到这里：说明状态已被别的路径改过。
                // 若此时仍是 RUNNING（非终态），任务就永远进不了终态，
                // eraseTerminal 会拒绝删除 → 在 tasks_ 里永久残留。
                // 这里兜底标失败，保证它一定能被回收；返回 FAILED 而不是
                // REJECTED，因为任务确实执行过，只是结果无法写入。
                if (!task->isTerminal())
                {
                    task->fail ("TASK_STATE_CONFLICT");
                }
                return WorkerExecutionResult::FAILED;
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