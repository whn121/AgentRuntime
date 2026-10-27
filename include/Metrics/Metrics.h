#pragma once

#include <atomic>
#include <cstdint>

// 全局进程级指标, 都是 atomic, 谁都能写, 不加锁
// 只记计数, 不记明细, 要明细去看日志
struct Metrics {
    // ─── 之前就有的 ───
    static std::atomic<uint64_t> total_connections;   // 累计 accept 的连接数
    static std::atomic<uint64_t> active_connections;  // 当前还连着的
    static std::atomic<uint64_t> total_requests;      // 解析成功的业务请求数
    static std::atomic<uint64_t> error_requests;      // 业务错误(协议错误/编码失败/handler抛异常)
    static std::atomic<uint64_t> bytes_read;          // 累计从socket读取的字节数
    static std::atomic<uint64_t> bytes_written;       // 累计写入socket的字节数

    // 之前队列满只能看日志, 日志多了会被采样或者刷掉, 没法统计拒绝率

    // Scheduler 当前积压的任务数(所有客户端加起来)
    // 等于 Scheduler::pendingTotal_, 进队 +1 出队 -1
    // 如果这个数一直涨, 说明背压没生效, 压力全堆在内存里
    static std::atomic<uint64_t> scheduler_pending;

    // 单客户端配额满被拒的次数(MAX_PENDING_PER_CLIENT)
    // 一个连接疯狂提交的时候会涨, 说明这个客户端被隔离了
    static std::atomic<uint64_t> scheduler_rejected_quota;

    // RuntimeEngine 全局队列满被拒的次数(maxPendingTasks_)
    // 和上面那个区分: 那个是单个客户端太多, 这个是所有客户端加起来太多
    static std::atomic<uint64_t> runtime_rejected_capacity;

    // stopping_ 的时候被拒的次数, 单独记
    // 不分开的话没法判断到底是"正在关闭"还是"过载了"
    static std::atomic<uint64_t> runtime_rejected_stopping;

    // 当前正在跑任务的 worker 数, 等于 activeCount_
    // 和 runtime_pending 一起看, 能判断是不是 worker 全被慢任务占住了
    static std::atomic<uint64_t> runtime_active;

    // 单连接 pendingTasks_ 满了直接 close 的次数
    // 客户端在一个连接上发太快, 每条又要排队等 worker, 就会撞这个
    static std::atomic<uint64_t> conn_rejected_pending_full;

    // 全局 TaskQueue submit 返回 Full/Stopped, 走 handleTaskSubmitFailure 的次数
    // 和上面那个区分: 那个是单连接自己的队列满, 这个是全局工作队列满了
    static std::atomic<uint64_t> conn_rejected_submit_failed;

    // 发送侧超水位被 close 的次数
    // 客户端只发不收, 响应堆在 sendQueue_/outBuffer_ 里
    // 超过 MAX_SEND_QUEUE_BYTES 或者 HIGH_WATER_MARK 就断了
    static std::atomic<uint64_t> conn_closed_send_high_water;

    // 历史最高的发送队列字节数(不是当前的, 是峰值)
    // CAS 只增不减, 用来观察离软背压水位还有多远
    // 如果以后要精确的"当前最大值"这个不够, 得另外想办法
    static std::atomic<uint64_t> conn_max_send_queue_bytes;

    // 软背压暂停读取的次数
    static std::atomic<uint64_t> conn_backpressure_paused;

    // 软背压恢复读取的次数
    static std::atomic<uint64_t> conn_backpressure_resumed;
};