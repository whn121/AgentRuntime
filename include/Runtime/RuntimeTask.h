#pragma once

#include <cstdint>
#include <string>
#include <mutex>
#include "CancellationToken.h"


namespace runtime
{
    // 注意64位整数不等于分布式全局唯一id
    // 后续需要专门设计id生成策略
    using TaskId = std::uint64_t;

    enum class TaskState
    {
        PENDING,    // 已创建，尚未被调度
        SCHEDULED,  // 已分配给执行流程，等待开始运行
        RUNNING,    // 执行中
        SUCCEEDED,  // 成功终态
        FAILED      // 失败终态
    };

    // 将转态转换为可读字符, 便于日志和调试
    const char* toString(TaskState state);

    class RuntimeTask
    {
    public:
        // input按值传入, 通过移动语义保存到成员, 这样可以减少复制
        RuntimeTask(TaskId id, std::string input);

        // 只读取数据
        TaskId id() const;
        const std::string& input() const;

        TaskState state() const;

        // 返回副本避免解锁后继续访问, 避免复制字符串, 也禁止调用方修改成员
        std::string result() const;
        std::string error() const;

        // 状态切换
        bool schedule();
        bool start();
        bool succeed(std::string result);
        bool fail(std::string error);

        // 判断任务是否进入最终成功或失败
        bool isTerminal() const;

        // 任务创建时生成的令牌, 由 RuntimeEngine 在 stop() 时取消。
        const CancellationToken& token() const { return token_; }

    private:
        TaskId id_;
        std::string input_; 

        mutable std::mutex mutex_;

        // 新任务投递默认处于PENDING状态
        TaskState state_ {TaskState::PENDING};

        std::string result_;
        std::string error_;

        CancellationToken token_;

    };
}