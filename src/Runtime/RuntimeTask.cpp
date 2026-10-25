#include "Runtime/RuntimeTask.h"

#include <stdexcept> // 异常类
#include <utility> // 移动



namespace runtime
{
    const char* toString (TaskState state)
    {
        switch (state)
        {
        case TaskState::PENDING:
            return "PENDING";
        case TaskState::SCHEDULED:
            return "SCHEDULED";
        case TaskState::RUNNING:
            return "RUNNING";
        case TaskState::SUCCEEDED:
            return "SUCCEEDED";
        case TaskState::FAILED:
            return "FAILED";
        default:
            return "UNKNOWN_STATE";
        }
    }

    RuntimeTask::RuntimeTask (TaskId id, std::string input) : id_ (id), input_ (std::move (input))
    {
        // 0为非法id
        if (id_ == 0)
        {
            throw std::invalid_argument ("Task ID must not be zero");
        }
    }

    TaskId RuntimeTask::id() const
    {
        return id_;
    }

    const std::string& RuntimeTask::input() const 
    {
        return input_;
    }

    TaskState RuntimeTask::state() const 
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return state_;
    }


    std::string RuntimeTask::result() const 
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return result_;
    }

    std::string RuntimeTask::error() const 
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return error_;
    }

    bool RuntimeTask::schedule ()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 只有尚未调度的任务才能进入SCHEDULED
        if (state_ != TaskState::PENDING)
        {
            return false;
        }

        state_ = TaskState::SCHEDULED;
        return true;
    }

    bool RuntimeTask::start ()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 不允许任务跳过调度直接开始执行。
        if (state_ != TaskState::SCHEDULED) 
        {
            return false;
        }

        state_ = TaskState::RUNNING;
        return true;
    }

    bool RuntimeTask::succeed (std::string result)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 只有正在执行的任务才能报告成功
        if (state_ != TaskState::RUNNING) 
        {
            return false;
        }

        // 先保存结果，再更新状态。
        // 这里不是数据库事务；若未来需要并发安全或持久化，
        // 还必须设计对应的同步和提交机制。
        result_ = std::move(result);
        error_.clear();
        state_ = TaskState::SUCCEEDED;
        
        return true;
    }

    bool RuntimeTask::fail (std::string error)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 任务可以在调度后或执行中失败。
        if (state_ != TaskState::SCHEDULED && state_ != TaskState::RUNNING) 
        {
            return false;
        }

        error_ = std::move(error);
        result_.clear();
        state_ = TaskState::FAILED;

        return true;
    }

    bool RuntimeTask::isTerminal() const
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 当前版本只有成功和失败属于终态
        return state_ == TaskState::SUCCEEDED || state_ == TaskState::FAILED;
    }
}

