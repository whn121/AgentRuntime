#pragma once

#include <atomic>
#include <memory>


namespace runtime
{
    // 任务取消令牌。可以拷贝, 拷贝共享同一个原子标志
    // 生产者调用 cancel(), 消费者(executor_)周期性检查 cancelled()
    // 用 shared_ptr 是为了让令牌在任务和引擎之间共享生命周期
    // 引擎持有副本用于 stop() 时批量取消
    class CancellationToken
    {
    public:
        CancellationToken() : flag_ (std::make_shared<std::atomic<bool>> (false)) {}

        void cancel() const
        {
            flag_->store(true, std::memory_order_relaxed);
        }

        bool cancelled() const
        {
            return flag_->load(std::memory_order_relaxed);
        }

        // 用于在activeTokens_里查找同一个令牌
        bool operator==(const CancellationToken& other) const
        {
            return flag_ == other.flag_;
        }

    private:
        std::shared_ptr<std::atomic<bool>> flag_;
    };

}