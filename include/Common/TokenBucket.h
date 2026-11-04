#pragma once

#include <algorithm>
#include <mutex>

// 令牌桶（token bucket）限流器。
//
// 模型：桶里最多存 capacity 个令牌，按 rate_per_sec 的速度持续补充。
//       每个请求消耗 cost 个令牌；令牌不够就拒绝。
//
// 为什么用令牌桶而不是固定窗口计数：
//   固定窗口在窗口交界处可以放过 2N 个请求（前一窗口末尾 N 个 + 后一窗口开头 N 个），
//   令牌桶按"速率"平滑补充，突发受 capacity 限制，不会出现这种双倍突刺。
//
// 时间由调用方传入（now_sec），不在内部读时钟：
//   这样单元测试可以完全控制时间，不需要 sleep，结果确定。
class TokenBucket
{
public:
    TokenBucket(double capacity, double rate_per_sec)
        : capacity_(capacity > 0.0 ? capacity : 1.0),
          rate_(rate_per_sec > 0.0 ? rate_per_sec : 1.0),
          tokens_(capacity_),
          last_sec_(0.0),
          started_(false) {}

    // 尝试取 cost 个令牌。成功返回 true，否则 false（不扣令牌）。
    bool tryAcquire(double now_sec, double cost = 1.0)
    {
        std::lock_guard<std::mutex> lock(mtx_);

        if (!started_)
        {
            last_sec_ = now_sec;
            started_ = true;
        }

        if (now_sec > last_sec_)
        {
            const double refill = (now_sec - last_sec_) * rate_;
            tokens_ = std::min(capacity_, tokens_ + refill);
            last_sec_ = now_sec;
        }

        if (tokens_ < cost)
        {
            return false;
        }

        tokens_ -= cost;
        return true;
    }

    double tokens() const
    {
        std::lock_guard<std::mutex> lock(mtx_);
        return tokens_;
    }

    double capacity() const { return capacity_; }
    double rate() const { return rate_; }

    // 测试/重连时用：把桶恢复成满的。
    void reset()
    {
        std::lock_guard<std::mutex> lock(mtx_);
        tokens_ = capacity_;
        started_ = false;
    }

private:
    mutable std::mutex mtx_;
    double capacity_;
    double rate_;
    double tokens_;
    double last_sec_;
    bool started_;
};
