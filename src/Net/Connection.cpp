#include "Net/Connection.h"
#include "Logger/AsyncLogger.h"
#include "Metrics/Metrics.h"
#include "Protocol/MyProtocol.h"

#include <sys/uio.h> //解决buffer一次只能读一块, 减少系统调用
#include <utility>
#include <queue>
#include <atomic>


// 逻辑客户端标识：进程内单调递增，永不复用。
// 不能用 fd 当身份——fd 是内核资源，连接关闭后会被复用给新连接，
// 那时"取消旧连接的任务"就可能误伤新连接。
static std::atomic<std::uint64_t> g_next_client_id{1};

// 构造函数：转移 Channel 和 Protocol 的所有权，保存 EventLoop 指针
Connection::Connection(int afd, std::unique_ptr<Channel> channel, EventLoop* loop, std::unique_ptr<Protocol> protocol,
                       double rate_capacity, double rate_refill_per_sec)
    : afd_(afd), clientId_(g_next_client_id.fetch_add(1, std::memory_order_relaxed)), loop_(loop), channel_(std::move(channel)), protocol_(std::move(protocol)),
      rateLimiter_(rate_capacity, rate_refill_per_sec), rate_capacity_(rate_capacity)
{
    Metrics::connection_constructed.fetch_add(1, std::memory_order_relaxed);
}

// 每连接准入限流：业务线程在 submit 之前调用一次
bool Connection::allowRequest()
{
    if (rate_capacity_ <= 0.0)
    {
        return true;   // 限流关闭（压测测容量）
    }

    const double now = std::chrono::duration<double>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return rateLimiter_.tryAcquire(now);
}

Connection::~Connection()
{
    // 如果 finishClose 已经关过，afd_ == -1，避免重复 close
    if (afd_ != -1)
    {
        ::close(afd_);
        afd_ = -1;
    }
    Metrics::connection_destructed.fetch_add(1, std::memory_order_relaxed);
}

// ---------- 读事件处理 ----------
void Connection::read()
{
    // 对端EOF后不再读取, 但允许已收到的请求完成处理
    if(state_ != State::Connected || peerReadClosed_ || readPausedForBackpressure_)
    {
        return;
    }

    static constexpr size_t EXTRA_BUF_SIZE = 65536;
    char extrabuf[EXTRA_BUF_SIZE] = {};

    bool protocol_error = false;

    // 循环读到 EAGAIN：一次事件把内核缓冲区取干净，减少 epoll 唤醒次数。
    // 但每读一段就立刻解析，所以恶意 body_len 不会在解析前把输入缓冲撑大。
    while (true)
    {
        // 保证 Buffer 至少有一些可写空间
        size_t writable = inBuffer_.writableBytes();
        if (writable == 0) {
            inBuffer_.enableWrite(EXTRA_BUF_SIZE);
            writable = inBuffer_.writableBytes();
        }

        struct  iovec vec[2];
        vec[0].iov_base = inBuffer_.beginWrite();
        vec[0].iov_len  = writable;
        vec[1].iov_base = extrabuf;
        vec[1].iov_len  = sizeof(extrabuf);

        ssize_t n = readv(afd_, vec, 2);  // 一次性读两块

        if (n > 0)
        {
            const size_t bytes = static_cast<size_t>(n);

            if (bytes <= writable)
            {
                inBuffer_.moveWritePtr(bytes);
            }
            else
            {
                inBuffer_.moveWritePtr(writable);
                inBuffer_.bufferAppend(extrabuf, bytes - writable);
            }

            updateActiveTime();
            Metrics::bytes_read += bytes;

            // 每读一段先解析：非法长度在这里就被拦下，缓冲不会被撑大
            if (!drainFrames())
            {
                protocol_error = true;
                break;
            }
            continue;   // 还有数据就继续读，直到 EAGAIN
        }

        if (n == 0)
        {
            // 对端关闭写方向：不能立即关连接，缓冲里可能还有完整请求要处理
            peerReadClosed_ = true;

            // 停止读事件关注, 避免半关闭后反复触发读事件
            channel_->disableReading();
            loop_->updateChannelEvent(channel_.get(), channel_->getEvents());
            break;
        }

        // n < 0
        if (errno == EINTR) continue;                                  // 信号中断，重试
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;            // 内核缓冲区空了

        handleClose();
        return;
    }

    if (!protocol_error)
    {
        // 覆盖两种情况：本次一个字节都没读到、但缓冲里还留着上次的半包；
        // 以及读到 EOF 后，把残留的完整帧处理完
        (void)drainFrames();
    }

    // EOF 后检查业务任务与输出是否全部排空。
    maybeFinishAfterPeerEof();
}

// 解析缓冲里所有完整帧并投递业务任务。
// 返回 false 表示连接已进入关闭流程（协议错误），调用方应停止继续读。
bool Connection::drainFrames()
{
    while (true)
    {
        ParseResult res = protocol_->parse(inBuffer_);

        if (res == ParseResult::OK)
        {
            // 连接已经进入关闭流程，不再接受新的业务请求
            if (!isConnected())
            {
                return false;
            }

            // 解析成功，封装任务投递给业务线程池
            Task task;
            task.message_ = protocol_->getMessage();  // 取出解析好的请求消息

            auto msg = task.message_;
            auto handler = businessHandler_;          // 复制一份，避免成员被并发改

            // 用 weak_ptr 捕获：任务排队期间连接可以正常释放，
            // 执行时提升失败就跳过，不会把连接寿命拖到任务执行完
            std::weak_ptr<Connection> weakself = shared_from_this();

            task.run_ = [weakself, msg, handler]()
            {
                auto self = weakself.lock();
                // 对象仍存活, 不代表链接仍允许启动新的业务处理
                if (!self || !self->isConnected()) return;

                if (handler)
                {
                    handler (self, msg);
                }
            };

            task.onComplete_ = [weakself]()
            {
                if (auto self = weakself.lock())
                {
                    self->onTaskComplete();
                }
            };

            enqueueTask(std::move(task));
            protocol_->reset();  // 重置协议状态，准备解析下一个请求
            continue;
        }

        if (res == ParseResult::ERROR)
        {
            // 协议错误：尝试获取协议层预置的错误响应
            auto errResp = protocol_->getErrorResponse();
            if (errResp.has_value())
            {
                sendResponse(errResp.value());   // 发送错误响应给客户端
                protocol_->reset();
            }
            else
            {
                // 协议层没有提供错误响应，直接关闭连接
                handleClose();
            }
            return false;
        }

        // NEED_MORE: 普通情况下保留半包, 等待后续数据
        // 但如果对端已经EOF, 就不可能再收到剩余字节
        if (peerReadClosed_ && inBuffer_.getreadable() > 0)
        {
            LOG_WARN("Connection fd=" + std::to_string(afd_) + " peer EOF with incomplete frame; discarding "
                     + std::to_string(inBuffer_.getreadable()) + " bytes");

            // 丢弃 EOF 时仍不完整的尾帧。
            // 前面已经成功解析的完整请求不受影响。
            inBuffer_.moveReadPtr(inBuffer_.getreadable());
        }

        return true;
    }
}

// ---------- 写事件处理 ----------
void Connection::write()
{
    if(state_ != State::Connected)
    {
        return;
    }

    while (outBuffer_.getreadable() > 0)
    {
         //MSG_NOSIGNAL 用于避免向已经断开的对端发送数据时, 因为 SIGPIPE 导致整个进程意外终止
        ssize_t n = send(afd_, outBuffer_.peek(), outBuffer_.getreadable(), MSG_NOSIGNAL);
        if (n > 0)
        {
            outBuffer_.moveReadPtr(n);   // 发送成功，移动读指针

            updateActiveTime();

            Metrics::bytes_written += n;
        }
        else if (n == 0)
        {
            handleClose();
            return;
        }
        else
        {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break; // 资源暂时不可用，等待下次
            handleClose();
            return;
        }
    }

    // 发送缓冲区已空：取消 EPOLLOUT
    // 关键：先改 Channel 内存状态，再同步给 epoll
    if (outBuffer_.getreadable() == 0)
    {
        channel_->disableWriting();
        loop_->updateChannelEvent(channel_.get(), channel_->getEvents());
    }

    // 发送进度可能把队列从高水位降到低水位, 这时要恢复读取
    // 注意：不管 outBuffer_ 是否排空都要检查, 部分发送也会让水位下降
    updateReadInterestForBackpressure();

    // 如果对端已经EOF, 检查能否完成最终关闭
    if (outBuffer_.getreadable() == 0)
    {
        maybeFinishAfterPeerEof();
    }
}

// 发送业务响应（内部编码）。
// 兼容旧调用点：编码放在**调用方线程**做（协议 encode 已是 const 且无状态），
// 真正的入队/flush 仍然投递到 IO 线程。
void Connection::sendResponse(const std::any& response)
{
    // 关闭期间静默丢掉响应是预期行为。异步任务(RuntimeEngine)的完成回调
    // 可能在连接关了之后才到, 这时候丢掉比强发安全
    // 见 docs/shutdown-semantics.md
    if (!isConnected()) return;

    std::string data;
    try
    {
        data = protocol_->encode(response);
    }
    catch (const std::exception& e)
    {
        LOG_ERROR("Response encoding failed: " + std::string(e.what()));
        // 编码失败要回到 IO 线程关闭连接（handleClose 有线程归属要求）
        std::weak_ptr<Connection> weakself = shared_from_this();
        loop_->runInLoop([weakself]
        {
            if (auto self = weakself.lock()) self->handleClose();
        });
        return;
    }

    sendRaw(std::move(data));
}

// 发送已编码的字节：任意线程可调，入队/flush 投递到 IO 线程执行
void Connection::sendRaw(std::string data)
{
    if (data.empty()) return;
    if (!isConnected()) return;

    // 回调已经提交, 但可能尚未被 EventLoop 执行；
    // 先计数, 避免 EOF 收尾误认为没有待发送工作
    pendingSendCallbacks_.fetch_add(1, std::memory_order_relaxed);

    // runInLoop 的参数构造(lambda 移动 data)和 task_queue_.emplace_back
    // 都可能抛 bad_alloc。任何一步抛异常都要把计数还回去，
    // 否则 maybeFinishAfterPeerEof 会永远被这个计数挡住
    try
    {
        std::weak_ptr<Connection> weakself = shared_from_this();

        const bool accepted = loop_->runInLoop([weakself, data = std::move(data)]() mutable
        {
            auto self = weakself.lock();
            if (!self) return;

            self->pendingSendCallbacks_.fetch_sub(1, std::memory_order_acq_rel);
            self->enqueueEncoded(std::move(data));
        });

        if (!accepted)
        {
            // loop 已停止，回调不会执行，计数必须在这里还回去
            pendingSendCallbacks_.fetch_sub(1, std::memory_order_acq_rel);
        }
    }
    catch (...)
    {
        pendingSendCallbacks_.fetch_sub(1, std::memory_order_acq_rel);
        throw;
    }
}

// 在 IO 线程里：把字节放进发送队列 → flush → 检查 EOF 收尾
void Connection::enqueueEncoded(std::string data)
{
    if (!isConnected()) return;

    bool queueFull = false;

    {
        std::lock_guard<std::mutex> lock (sendMutex_);

        // 使用减法比较, 避免 size 相加时发生整数溢出
        if (data.size() > MAX_SEND_QUEUE_BYTES || sendQueueBytes_ > MAX_SEND_QUEUE_BYTES - data.size())
        {
            queueFull = true;
        }
        else
        {
            sendQueueBytes_ += data.size();
            sendQueue_.push(std::move(data));
        }
    }

    if (queueFull)
    {
        Metrics::conn_closed_send_high_water.fetch_add(1, std::memory_order_relaxed);
        LOG_WARN("Connection fd=" + std::to_string(afd_) + " sendQueue exceeds limit; closing connection");
        handleClose();
        return;
    }

    flushSendQueue();

    // flush 后再次检查：若已排空，可以完成 EOF 收尾；若仍有数据，检查会返回
    maybeFinishAfterPeerEof();
}

Channel* Connection::getChannel()
{
    return channel_.get();
}

void Connection::setcloseCallback(std::function<void(int)> close)
{
    closeCallBack_ = close;
}

void Connection::myClose()
{
    close();
}

// 初始化：将 Channel 的回调绑定到 Connection 的成员函数
void Connection::init()
{
    //不能活的强引用,要不然循环引用了,无法析构计数不会归零
    std::weak_ptr<Connection> weakself = shared_from_this();   // 获得自身的weak_ptr，防止回调中对象被释放

    channel_->setreadCallBack([weakself] { if (auto self = weakself.lock()) { self -> read(); } });      // 读就绪,lock尝试升级为临时强引用,回调结束析构
    channel_->setwriteCallBack([weakself] { if (auto self = weakself.lock()) { self -> write(); } });    // 写就绪,lock尝试升级为临时强引用,回调结束析构
    channel_->setcloseCallBack([weakself] { if (auto self = weakself.lock()) { self -> myClose(); } });  // 挂起/错误,lock尝试升级为临时强引用,回调结束析构
}

void Connection::setCallBack(std::function<TaskQueue::PushResult(Task&&)> callback)
{
    worksumbitcallback_ = std::move(callback);
}

void Connection::close()
{
    {
        std::lock_guard<std::mutex> lock (taskMutex_);
        taskSchedulingStopped_ = true;
        closeCallbackPending_ = true;
    }

    std::weak_ptr<Connection> weakself = shared_from_this();
    
    const bool accepted = loop_->runInLoop([weakself]
    {
        if (auto self = weakself.lock())
        {
            self->handleClose();
        }
    });

    if (!accepted)
    {
        // closeCallbackPending_ 保持 true，由 IoThreadPool::stop() 中的
        // drainClosingConnections() 在 EventLoop 线程认领并执行 closeCallBack_
        // 不得在当前线程直接操作 Channel 或 finishClose()
    }
}

// 返回距离上次活跃经过的秒数
    int64_t Connection::idleSeconds() const
    {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::seconds>(now - last_active_).count();
    }

void Connection::handleClose()
{
    // 首次关闭: 只有一个线程能从Connected进入Closing
    State expected = State::Connected;
    if (!state_.compare_exchange_strong(expected, State::Closing))
    {
        // 如果已经进入 Closing，尝试补做此前未完成的最终关闭。
        // 本函数必须运行在所属 EventLoop 线程。
        if (expected != State::Closing) return;

        finalizeCloseIfPending();
        
        return;
    }

    LOG_INFO("handleClose: fd=" + std::to_string(afd_));

    // 状态已经是 Closing，completion 回调里 sendResponse 会因 isConnected() 返回 false
    // 而静默丢弃。此时触发 cancelHook 是安全的——不会误发 CLIENT_DISCONNECTED 响应。
    notifyCancelOnce();

    // 优雅关闭写端：告诉对端我们不再发数据
    // 注意：shutdown 不关闭 fd，真正的 close 在 finishClose()
    if (afd_ != -1)
    {
        ::shutdown(afd_, SHUT_WR);
    }

    // 通知 EventLoop / Server 清理
    bool deferFinalClose = false;

    // 清理尚未开始执行的业务任务
    {
        std::lock_guard<std::mutex> lock (taskMutex_);
        while (!pendingTasks_.empty())
        {
            pendingTasks_.pop();
        }

        // taskRunning_ 表示已有任务被调用, 或正在执行
        // 这种情况下, 不能立即清理业务资源
        deferFinalClose = taskRunning_;
        closeCallbackPending_ = deferFinalClose;
    }

    // 清空还在sendQueue_中排队的响应
    {
        std::lock_guard<std::mutex> lock(sendMutex_);
        while (!sendQueue_.empty())
        {
            sendQueue_.pop();
        }
        sendQueueBytes_ = 0;
    }

    // 释放两个Buffer中仍未消费的逻辑数据
    // 这里只清空缓冲区内容, 不释放底层vector的容量
    inBuffer_.moveReadPtr(inBuffer_.getreadable());
    outBuffer_.moveReadPtr(outBuffer_.getreadable());

    if (deferFinalClose)
    {
        // 先从epoll移除该链接, 避免他继续接受网络事件
        // 暂时保留connections_映射和fd, 等待业务任务完成
        loop_->removeChannel(channel_.get());

        LOG_INFO("handleClose: defer final close until task completion, fd=" + std::to_string(afd_));

        return;
    }

    // 没有待完成任务，沿用原来的关闭回调。
    // IoThreadPool 回调负责业务清理、removeConnection 和 finishClose。
    if (closeCallBack_)
    {
        closeCallBack_(afd_);
    }
}

void Connection::notifyCancelOnce()
{
    // 所有关闭路径（close / read 错误 / write 错误 / 协议错误 / forceCloseOnLoopStop）
    // 最终都走 handleClose 或 forceCloseOnLoopStop，都调用本函数。
    // 用不可变的 clientId_（逻辑身份），不用 afd_——afd_ 可能已被置 -1，
    // 而且 fd 会被内核复用，不能当身份。
    // CAS 保证 cancelHook_ 恰好一次。
    bool expected = false;
    if (cancelHook_ && cancelHookCalled_.compare_exchange_strong(expected, true))
    {
        cancelHook_(clientId_);
    }
}

void Connection::maybeFinishAfterPeerEof()
{
    // 只处理对端发送方向已关闭、但连接尚未关闭的情况。
    // 本函数由所属 EventLoop 线程调用。
    if (!peerReadClosed_ || state_ != State::Connected)
    {
        return;
    }

    // 条件一：没有正在执行或等待中的业务任务。
    bool tasksDrained = false;

    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        tasksDrained = !taskRunning_ && pendingTasks_.empty();
    }

    if (!tasksDrained)
    {
        return;
    }

    // 条件二：没有等待 EventLoop 执行的发送回调。
    if (pendingSendCallbacks_.load(std::memory_order_acquire) != 0)
    {
        return;
    }

    // 条件三：没有仍在 sendQueue_ 中等待合并的消息。
    bool queuedResponsesEmpty = false;

    {
        std::lock_guard<std::mutex> lock(sendMutex_);
        queuedResponsesEmpty = sendQueue_.empty();
    }

    if (!queuedResponsesEmpty)
    {
        return;
    }

    // 条件四：outBuffer_ 中没有尚未发送的字节。
    if (outBuffer_.getreadable() != 0)
    {
        return;
    }

    // 业务与输出均排空，可以进入最终关闭流程。
    handleClose();
}

void Connection::finishClose()
{
    if(state_ == State::Closed)
    {
        return;
    }


    state_ = State::Closed;

    // 真正关闭 fd，避免 pendingConnections_ 延迟析构期间占着 fd
    if (afd_ != -1)
    {
        ::close(afd_);
        afd_ = -1;
    }

    LOG_INFO("finishClose done");
}

void Connection::finalizeCloseIfPending()
{
    bool needFinalize = false;

    {
        std::lock_guard<std::mutex> lock(taskMutex_);

        // 只有业务任务已结束、且最终关闭仍待处理时才能认领。
        // 认领和清零在同一临界区内完成，保证并发调用只有一个认领者。
        if (closeCallbackPending_ && !taskRunning_)
        {
            closeCallbackPending_ = false;
            needFinalize = true;
        }
    }

    if (needFinalize && closeCallBack_)
    {
        // 该回调由 IoThreadPool 注入，内部会执行
        // 业务清理 + loop->removeConnection(fd)。
        // 因此本函数必须在 EventLoop 线程调用。
        closeCallBack_(afd_);
    }
}
void Connection::forceCloseOnLoopStop()
{
    // 强制进入Closing 若已经是closing/Closed, CAS失败, 继续走认领
    State expected = State::Connected;
    state_.compare_exchange_strong (expected, State::Closing);

    // loop 停止时强制关闭。这条路径不经过 handleClose，需要单独触发 cancelHook。
    // 如果之前已经通过 close() 或错误路径进入过 handleClose，cancelHookCalled_ CAS 会拦住。
    notifyCancelOnce();

    // 标记待最终关闭：即使之前从未调用过 close()，也要让 finalizeCloseIfPending 认领
    {
        std::lock_guard<std::mutex> lock(taskMutex_);
        closeCallbackPending_ = true;
    }

    // 立即尝试认领。若 taskRunning_ == true，closeCallbackPending_ 保持 true，
    // 需要 EventLoop::loop() 之后的二次扫描来覆盖（见 1a-10 的循环调用）。
    finalizeCloseIfPending();
}

void Connection::handleTaskSubmitFailure(TaskQueue::PushResult result, bool preserveRunningTask)
{
    if (result == TaskQueue::PushResult::Success)
    {
        return;
    }

    Metrics::conn_rejected_submit_failed.fetch_add(1, std::memory_order_relaxed);
    // 将等待任务转移到局部队列, 避免持锁销毁任务对象
    std::queue<Task> cancelledTasks;

    {
        std::lock_guard<std::mutex> lock(taskMutex_);

        // 拒绝后不再允许本链接继续调度任务
        taskSchedulingStopped_ = true;

        // 被拒绝的任务没有进入Worker, 因为改调度槽位以结束
        if (!preserveRunningTask)
        {
            taskRunning_ = false;
        }

        // 不调用这些任务的 onComplete_：
        // 它们的完成回调会推进 FIFO，可能造成递归调度。
        cancelledTasks.swap(pendingTasks_);
    }

    if (result == TaskQueue::PushResult::Full)
    {
        LOG_ERROR("Connection fd=" + std::to_string(afd_) + " work queue full; stopping task scheduling");
    }
    else 
    {
        LOG_ERROR("Connection fd=" + std::to_string(afd_) + " work queue stopped; stopping task scheduling");
    }

    // 在锁外销毁被取消的等待任务。
    while (!cancelledTasks.empty()) 
    {
        cancelledTasks.pop();
    }

    // 统一使用现有 EventLoop 关闭流程。
    // 不在这里直接调用 handleClose()，因为当前线程可能不是所属 IO 线程。
    close();
}

void Connection::enqueueTask(Task task)
{
    Task nextTask;
    bool needSubmit = false;
    bool queueFull = false;

    {
        std::lock_guard<std::mutex> lock(taskMutex_);

        if (!isConnected() || !worksumbitcallback_ || taskSchedulingStopped_) return;

        // 队列满时不接收新的任务, 避免单个链接无限积压
        if (pendingTasks_.size() >= MAX_PENDING_TASKS) 
        {
            queueFull = true;
            taskSchedulingStopped_ = true;
        }
        else 
        {
            pendingTasks_.push(std::move(task));

            if (!taskRunning_)
            {
                taskRunning_ = true;

                nextTask = std::move(pendingTasks_.front());
                pendingTasks_.pop();

                needSubmit = true;
            }
        }
    }

    // 不能持有taskMutex_ 调用 handleClose
    // 因为关闭流程会获得taskMutex_
    if (queueFull)
    {
        Metrics::conn_rejected_pending_full.fetch_add(1, std::memory_order_relaxed);
        LOG_WARN("Connection fd=" + std::to_string(afd_) + " pending task queue full; closing connection");

        // 尝试发送一个明确的拥塞错误
        // sendResponse 内部会检查 isConnected()，若已进入 Closing 则静默丢弃
        try
        {
            sendResponse(MyMessage{
                MyType::ERROR_RESP,
                0,
                "SERVER_BUSY_PENDING_QUEUE_FULL"
            });
        }

        catch (...) {}
        close();
        return;
    }

    if (needSubmit && worksumbitcallback_)
    {
        const auto result = worksumbitcallback_ (std::move (nextTask));
        
        if (result != TaskQueue::PushResult::Success)
        {
            handleTaskSubmitFailure(result);
        }
    }
}

void Connection::onTaskComplete()
{
    Task nextTask;
    bool needSubmit = false;
    bool noMoreTasks = false;
    bool needFinalizeClose = false;

    {
        std::lock_guard<std::mutex> lock(taskMutex_);

        //  如果链接已经进入终止关闭流程, 不再调度后续业务任务
        if (state_.load() != State::Connected || taskSchedulingStopped_) 
        {
            while (!pendingTasks_.empty())
            {
                pendingTasks_.pop();
            }

            taskRunning_ = false;
            noMoreTasks = true;

            // 只标记需要投递，不在锁内清零。
            // 清零与认领由 finalizeCloseIfPending() 在 EventLoop 线程原子完成，
            // 避免"投递失败但已清零"导致关闭丢失。
            if (closeCallbackPending_)
            {
                needFinalizeClose = true;
            }
        }

        else if (pendingTasks_.empty())
        {
            // 当前任务是最后一个任务
            taskRunning_ = false;
            noMoreTasks = true;
        }
        else
        {
            // 保持同一链接上的FIFO顺序
            nextTask = std::move(pendingTasks_.front());
            pendingTasks_.pop();

            taskRunning_ = true;
            needSubmit = true;
        }
    }

    // 继续执行下一个业务任务
    if (needSubmit)
    {
        const auto result = worksumbitcallback_ ? worksumbitcallback_ (std::move (nextTask)) : TaskQueue::PushResult::Stopped;

        if (result != TaskQueue::PushResult::Success)
        {
            handleTaskSubmitFailure (result);
        }
        return;
    }

    std::weak_ptr<Connection> weakself = shared_from_this();

    if (needFinalizeClose)
    {
        // 工作线程不能直接清理 EventLoop / Channel。
        // 回到所属 EventLoop，执行原有关闭回调。
        const bool queued = loop_->runInLoop([weakself]
        {
            if (auto self = weakself.lock())
            {
                self->finalizeCloseIfPending();
            }
        });

        if (!queued)
        {
            // 投递失败时 closeCallbackPending_ 保持 true，
            // 由 IoThreadPool::stop() 中的 drainClosingConnections() 在
            // EventLoop 线程兜底认领，不会丢失。
            LOG_ERROR("Connection: failed to schedule finalize close, fd=" + std::to_string(afd_)
                + "; deferred to drainClosingConnections");
        }

        return;
    }
    if (noMoreTasks)
    {
        // 普通任务完成后, 检查对端EOF场景是否可以收尾
        const bool queued = loop_->runInLoop([weakself]
        {
            if (auto self = weakself.lock())
            {
                self->maybeFinishAfterPeerEof();
            }
        });

        if (!queued)
        {
            {
                std::lock_guard<std::mutex> lock(taskMutex_);

                // 保留最终关闭请求, 避免投递失败后丢失关闭状态
                if (state_.load() == State::Closing && !taskRunning_)
                {
                    closeCallbackPending_ = true;
                }
            }

            LOG_ERROR("Connection: failed to schedule final close callback, fd="
                + std::to_string(afd_));
        }
    }
}

void Connection::flushSendQueue()
{
    if (state_ != State::Connected)
    {
        return;
    }

    std::queue<std::string> temp;
    {
        std::lock_guard<std::mutex> lock(sendMutex_);
        temp.swap(sendQueue_);
        sendQueueBytes_ = 0;
    }

    while (!temp.empty())
    {
        auto& msg = temp.front();
        const size_t currentBytes = outBuffer_.getreadable();

        // 在追加数据, 触发缓冲区扩容之前检查高水位
        // 使用减法比较, 避免currentBytes + msg.szie() 溢出
        if (currentBytes > HIGH_WATER_MARK || msg.size() > HIGH_WATER_MARK - currentBytes)
        {
            Metrics::conn_closed_send_high_water.fetch_add(1, std::memory_order_relaxed);
            LOG_WARN("Connection fd=" + std::to_string(afd_) + " outBuffer would exceed high water mark; closing");

            handleClose();
            return;
        }

        outBuffer_.bufferAppend(msg.data(), msg.size());
        temp.pop();
    }

    uint64_t current = static_cast<uint64_t>(outBuffer_.getreadable());
    uint64_t prev = Metrics::conn_max_send_queue_bytes.load(std::memory_order_relaxed);
    while (current > prev && !Metrics::conn_max_send_queue_bytes.compare_exchange_weak(prev, current, 
            std::memory_order_relaxed)) {}

    // 有数据待发 → 启用 EPOLLOUT
    if (outBuffer_.getreadable() > 0)
    {
        channel_->enableWriting();
        loop_->updateChannelEvent(channel_.get(), channel_->getEvents());
    }

    // 追加数据可能把队列推过高水位, 这时暂停读取
    // 放在 enableWriting 之后, 这样 disableReading 时 epoll 里还保留 EPOLLOUT
    updateReadInterestForBackpressure();
}

void Connection::updateReadInterestForBackpressure()
{
    // 只有正常连接才调整读事件, Closing/Closed 阶段不碰 channel
    if (state_ != State::Connected)
    {
        return;
    }

    const size_t pendingBytes = outBuffer_.getreadable();

    // 涨过高水位：暂停读取, 让 TCP 窗口自己收缩
    if (!readPausedForBackpressure_ && pendingBytes >= SEND_SOFT_HIGH_WATER)
    {
        readPausedForBackpressure_ = true;
        channel_->disableReading();
        loop_->updateChannelEvent(channel_.get(), channel_->getEvents());

        Metrics::conn_backpressure_paused.fetch_add(1, std::memory_order_relaxed);
        LOG_INFO("Connection fd=" + std::to_string(afd_)
            + " send queue high, pause reading");
    }
    // 降到低水位：恢复读取
    else if (readPausedForBackpressure_ && pendingBytes <= SEND_SOFT_LOW_WATER)
    {
        readPausedForBackpressure_ = false;
        channel_->enableReading();
        loop_->updateChannelEvent(channel_.get(), channel_->getEvents());

        Metrics::conn_backpressure_resumed.fetch_add(1, std::memory_order_relaxed);
        LOG_INFO("Connection fd=" + std::to_string(afd_)
            + " send queue low, resume reading");
    }
}
