#pragma once

#include "Net/Buffer.h"
#include "Common/TokenBucket.h"
#include <unistd.h>
#include <sys/socket.h>
#include "Reactor/Channel.h"
#include <memory>
#include "Reactor/EventLoop.h"
#include "ThreadPool/Task.h"
#include "Protocol/Protocol.h"      // 只依赖抽象协议, 不依赖 HTTP
#include <mutex>
#include <chrono>
#include <queue>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include "ThreadPool/TaskQueue.h"



// 表示一个 TCP 连接, 管理读写缓冲区、协议解析、回调投递
class Connection : public std::enable_shared_from_this<Connection> 
{
public:
    Connection(int afd, std::unique_ptr<Channel> channel, EventLoop* loop,
               std::unique_ptr<Protocol> protocol,
               double rate_capacity = RATE_CAPACITY,
               double rate_refill_per_sec = RATE_REFILL_PER_S);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void read();                                          // 处理读事件
    void write();                                         // 处理写事件
    void sendResponse(const std::any& response);          // 发送响应（内部编码，兼容旧调用）

    // 发送**已编码**的字节。线程安全，可在任意线程调用（业务线程/engine 线程）。
    // 设计意图：把序列化留在业务线程，IO 线程只做字节搬运与 flush，
    // 避免大响应包在 IO 线程上做 encode 阻塞 epoll。
    void sendRaw(std::string data);

    // 每连接准入限流：从令牌桶里取一个令牌。
    // 返回 false 表示本连接当前速率超限，调用方应回 RATE_LIMITED 而不是提交任务。
    // 内部有锁，任意线程可调（实际调用方是业务线程）。
    bool allowRequest();

    Channel* getChannel();                                // 返回内部的 Channel 指针
    void setcloseCallback(std::function<void(int)> close);// 设置连接关闭回调
    void myClose();                                       // 主动关闭连接
    void init();                                          // 初始化回调绑定
    void setCallBack(std::function<TaskQueue::PushResult(Task&&)> callback); // 设置业务处理回调
    void close();// 线程安全的关闭方法, 可以从任意线程调用

    // 将任务加入当前连接的 FIFO 队列 FIFO(按照到达先后顺序执行)
    void enqueueTask(Task task);

    // 当前任务执行完成后, 继续调度下一个任务
    void onTaskComplete();

    // 业务处理回调：接收连接和解析后的消息, 由 Server 层注入
    // 解决work知道协议,解耦不彻底的问题
    using BusinessHandler = std::function<void(std::shared_ptr<Connection>, const std::any&)>;

    void setBusinessHandler(BusinessHandler h) { businessHandler_ = std::move(h); }

    // 客户端断开时调用，用于通知上层取消该连接关联的待调度任务。
    // 保证只调用一次。参数是**逻辑 clientId**（单调递增），不是 fd。
    void setCancelHook(std::function<void(std::uint64_t)> hook) { cancelHook_ = std::move(hook); }

    // 逻辑客户端标识：进程内单调递增，永不复用。
    // 为什么不用 fd：fd 是内核资源、会被复用——连接 A 关了 fd 可能分给 B，
    // 此时"取消 A 的任务"就可能误伤 B。身份不该依赖内核资源的生命周期。
    std::uint64_t clientId() const { return clientId_; }

    // 活跃检测,超时踢人 减少一个额外线程的开销
    void updateActiveTime() { last_active_ = std::chrono::steady_clock::now(); }

    // 返回距离上次活跃经过的秒数
    int64_t idleSeconds() const ;

    //判断状态
    bool isConnected() const
    {
        return state_ == State::Connected;
    }

    void setIdleTimeoutEnabled(bool enabled) { idleTimeoutEnabled_ = enabled; }
    bool idleTimeoutEnabled() const { return idleTimeoutEnabled_; }
    void flushSendQueue(); // 异步发送

    void finishClose();     //真正关闭释放内存

    // 统一认领并执行最终关闭回调。
    // 仅当 closeCallbackPending_ == true 且没有业务任务在运行时才认领，
    // 保证 closeCallBack_ 恰好执行一次。
    // 必须运行在所属 EventLoop 线程（内部会调用 closeCallBack_，
    // 而 closeCallBack_ 会触发 removeConnection，后者有 assertInLoopThread）。
    void finalizeCloseIfPending();

    // 仅在所属 EventLoop 即将退出时调用（loop 线程内）。
    // 强制从 Connected 进入 Closing，并尝试认领最终关闭。
    // 与 close() 的区别：不通过 runInLoop 投递（此时投递必然失败），
    // 直接在 loop 线程内执行。
    void forceCloseOnLoopStop();

    
private:
    int afd_;                                  // socket 文件描述符, finishClose 后置 -1
    const std::uint64_t clientId_;             // 逻辑身份（单调递增），与 fd 生命周期无关
    EventLoop* loop_;                          // 所属的事件循环（该连接所有事件在此 loop 中处理）
    Buffer inBuffer_;                          // 输入缓冲区
    Buffer outBuffer_;                         // 输出缓冲区
    std::unique_ptr<Channel> channel_;         // 对应的 Channel
    std::function<void(int)> closeCallBack_;   // 关闭回调（通知 EventLoop 清理）
    std::unique_ptr<Protocol> protocol_;       // 协议对象（多态）
    std::function<TaskQueue::PushResult(Task&&)> worksumbitcallback_; // 将任务投递给工作线程池的回调
    //防止重复关闭
    enum class State
    {
        Connected,  // 正常工作, 可以进行 IO
        Closing,    // 已经开始关闭, 不再接受新的 IO
        Closed      // 关闭流程已经完成
    };

    std::atomic<State> state_ {State::Connected};

    // 对端已经关闭发送方向, 但本端可能仍需处理已收到的请求并发送响应。
    // 该变量只由所属 EventLoop 线程读写。
    bool peerReadClosed_ = false;
    bool idleTimeoutEnabled_ = true;

    // 已投递但尚未执行的发送回调数量。配合上面,防止任务刚投递到eventloop就关闭,然后看task没任务直接退出
    std::atomic<size_t> pendingSendCallbacks_{0};
    
    void handleClose();      // 统一关闭入口 ,进入关闭状态,禁止新的io操作,移除epoll监听

    // 把已编码的字节放进发送队列并 flush。必须在所属 EventLoop 线程调用。
    void enqueueEncoded(std::string data);

    // 解析缓冲里所有完整帧并投递业务任务。
    // 返回 false 表示连接已进入关闭流程（协议错误），调用方应停止继续读。
    bool drainFrames();

    void notifyCancelOnce();  // 所有关闭路径统一触发一次 cancelHook_

    void handleTaskSubmitFailure(TaskQueue::PushResult result, bool preserveRunningTask = false); // 处理工作线程池拒绝任务：停止调度、取消等待任务并请求关闭

    void maybeFinishAfterPeerEof(); // 专门负责判断 对端发送方向关闭后, 业务任务是否完成,待执行发送回调是否归零, 以及响应缓冲是否排空,都满足进入关闭流程

    BusinessHandler businessHandler_;  //  解决work知道协议,解耦不彻底的问题 是一个函数指针 

    // 活跃检测,超时踢人 减少一个额外线程的开销
    std::chrono::steady_clock::time_point last_active_{std::chrono::steady_clock::now()};

    // 单个连接最多允许排队等待的业务任务数。
    // 不包含当前已经提交给 Worker 执行的任务。
    static constexpr size_t MAX_PENDING_TASKS = 1024;

    // 当前连接等待执行的任务, 严格 FIFO
    std::queue<Task> pendingTasks_;

    // 保护 pendingTasks_ 和 taskRunning_
    std::mutex taskMutex_;

    // true：当前已经有一个任务被送到 Worker 执行
    bool taskRunning_ = false;

    // 任务提交失败或队列溢出后，禁止继续接收和调度任务。
    // 由 taskMutex_ 保护。
    bool taskSchedulingStopped_ = false;

    // 终止关闭已请求，但仍有业务任务执行。
    // 由 taskMutex_ 保护。
    bool closeCallbackPending_ = false;

    // 异步发送队列
    std::mutex sendMutex_;
    std::queue<std::string> sendQueue_;

    // sendQueue_ 当前累计字节数, 由sendMutex_保护
    size_t sendQueueBytes_ = 0;

    // 单连接待发送队列上限
    static constexpr size_t MAX_SEND_QUEUE_BYTES = 64 * 1024 * 1024;
   
    // 写缓冲高水位：超过就断开连接, 防止客户端只读不写把服务器内存吃光
    static constexpr size_t HIGH_WATER_MARK = 64 * 1024 * 1024;  // 64MB

    // 软背压水位：发送队列超过此值时暂停读取，降到低水位后恢复。
    static constexpr size_t SEND_SOFT_HIGH_WATER = 8 * 1024 * 1024;   // 8MB
    static constexpr size_t SEND_SOFT_LOW_WATER  = 2 * 1024 * 1024;   // 2MB

    // 当前是否因软背压暂停了读取（仅 loop 线程读写）。
    bool readPausedForBackpressure_ = false;

    // 根据发送队列水位, 决定暂停还是恢复读事件
    void updateReadInterestForBackpressure();

    // ── 每连接准入限流 ──
    // 容量 200 个令牌（允许的突发量），每秒补充 100 个（稳态 QPS 上限）。
    // 参数放在这里是为了让"限流多少"和"连接实现"在一起，方便改。
    static constexpr double RATE_CAPACITY     = 200.0;
    static constexpr double RATE_REFILL_PER_S = 100.0;

    TokenBucket rateLimiter_;

    // <= 0 表示关闭限流（压测测容量时用）
    double rate_capacity_ = RATE_CAPACITY;

    // 取消回调。close() 里调用一次，通知上层（RuntimeEngine）清理。
    std::function<void(std::uint64_t)> cancelHook_;

    // 保证 cancelHook_ 只调一次。
    std::atomic<bool> cancelHookCalled_{false};
};