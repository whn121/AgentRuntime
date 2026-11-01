#include "Net/Connection.h"
#include "Protocol/MyProtocol.h"
#include "Reactor/Channel.h"
#include "Reactor/EventLoop.h"
#include "Runtime/CancellationToken.h"
#include "Runtime/RuntimeEngine.h"
#include "ThreadPool/IoThreadPool.h"
#include "ThreadPool/Task.h"
#include "ThreadPool/TaskQueue.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;

int main()
{
    // 1 个 worker 的 RuntimeEngine，executor 每个任务 sleep 100ms
    std::mutex m;
    std::condition_variable cv;
    bool firstStarted = false;

    runtime::RuntimeEngine engine(
        [&](const std::string& input, const runtime::CancellationToken&) -> std::string
        {
            {
                std::lock_guard<std::mutex> lock(m);
                if (!firstStarted) { firstStarted = true; cv.notify_all(); }
            }
            std::this_thread::sleep_for(100ms);
            return input;
        },
        1, 16);

    std::atomic<int> succeeded{0};
    std::atomic<int> cancelled{0};

    {
        IoThreadPool pool(1);

        pool.setCallBack([&](Task&& task) {
            if (task.run_) task.run_();
            if (task.onComplete_) task.onComplete_();
            return TaskQueue::PushResult::Success;
        });

        int connFd = -1;

        pool.setbuildconn([&](int fd, EventLoop* loop) {
            connFd = fd;
            auto conn = std::make_shared<Connection>(
                fd, std::make_unique<Channel>(fd), loop, std::make_unique<MyProtocol>());

            // 关键：连接关闭时通知 RuntimeEngine
            conn->setCancelHook([&](int clientId) {
                engine.cancelByClient(clientId);
            });

            return conn;
        });

        pool.setCloseCallback([](int) {});

        // businessHandler：每个请求都提交到 RuntimeEngine
        pool.setBusinessHandler([&](std::shared_ptr<Connection> conn, const std::any& msg) {
            int fd = conn->getChannel()->getFd();
            (void)msg;
            engine.submit(fd, "task",
                [&](const std::shared_ptr<runtime::RuntimeTask>& t,
                    runtime::WorkerExecutionResult r) {
                    if (r == runtime::WorkerExecutionResult::SUCCEEDED) succeeded.fetch_add(1);
                    else if (t && t->error() == "CLIENT_DISCONNECTED") cancelled.fetch_add(1);
                });
        });

        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 1;
        pool.submit(sv[0]);
        std::this_thread::sleep_for(100ms);

        // 发 6 个 RUNTIME_TASK_REQ 帧
        for (int i = 1; i <= 6; ++i)
        {
            const std::string frame = MyProtocol::encodeMessage(
                MyType::RUNTIME_TASK_REQ, static_cast<uint32_t>(i), "hello");
            (void)!::write(sv[1], frame.data(), frame.size());
        }
        std::this_thread::sleep_for(100ms);

        // 等第一个任务开始执行
        {
            std::unique_lock<std::mutex> lock(m);
            const bool ready = cv.wait_for(lock, 2s, [&] { return firstStarted; });
            assert(ready);
            (void)ready;
        }

        // 断开客户端：走 Connection 的关闭路径
        ::close(sv[1]);
        // 触发 EOF → read() 返回 0 → peerReadClosed_ → maybeFinishAfterPeerEof
        // → handleClose → notifyCancelOnce → cancelHook_ → engine.cancelByClient(connFd)

        // 等关闭路径执行完
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        pool.stop();
    }

    engine.stop(runtime::RuntimeEngine::ShutdownMode::FORCE);

    // 断开时第 1 个任务正在执行，其余在 pending 队列。
    // 但"断开的一瞬间第 2 个任务有没有被 worker 取走"取决于调度时序：
    // 同一份代码实测 20 次里，1 成功 5 取消 与 2 成功 4 取消 都会出现。
    // 所以不能断言固定数字；真正能确定的契约是——
    // 每个任务要么成功、要么被取消，不能丢、不能重复。
    assert(succeeded.load() >= 1);
    assert(cancelled.load() >= 1);
    assert(succeeded.load() + cancelled.load() == 6);

    return 0;
}