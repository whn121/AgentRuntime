#include "Metrics/Metrics.h"
#include "Net/Connection.h"
#include "Protocol/MyProtocol.h"
#include "Reactor/Channel.h"
#include "Reactor/EventLoop.h"
#include "ThreadPool/IoThreadPool.h"
#include "ThreadPool/Task.h"
#include "ThreadPool/TaskQueue.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <fcntl.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

int main()
{
    std::atomic<int> closeCount{0};
    std::atomic<int> fdWasValidWhenClosed{0};

    const uint64_t constructedBefore = Metrics::connection_constructed.load();
    const uint64_t destructedBefore = Metrics::connection_destructed.load();

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 1;
    const int connFd = sv[0];

    {
        IoThreadPool pool(1);

        pool.setCallBack([](Task&& task) {
            if (task.run_) task.run_();
            if (task.onComplete_) task.onComplete_();
            return TaskQueue::PushResult::Success;
        });

        pool.setbuildconn([](int fd, EventLoop* loop) {
            return std::make_shared<Connection>(
                fd, std::make_unique<Channel>(fd), loop, std::make_unique<MyProtocol>());
        });

        // 关键改动：closeCallback 不 close fd，只验证调用时刻 fd 仍然有效。
        // 真正的 close 由 Connection::finishClose() 负责，避免 double close。
        pool.setCloseCallback([&](int fd) {
            closeCount.fetch_add(1);
            if (fd >= 0 && fcntl(fd, F_GETFD) != -1)
            {
                fdWasValidWhenClosed.fetch_add(1);
            }
        });

        pool.setBusinessHandler([](std::shared_ptr<Connection>, const std::any&) {});

        pool.submit(connFd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        pool.stop();
        ::close(sv[1]);
    }
    // pool 已析构，EventLoop 已退出，所有 Connection 应已析构

    // closeCallback 恰好一次，且调用时 fd 仍然有效（说明没有提前 close）
    assert(closeCount.load() == 1);
    assert(fdWasValidWhenClosed.load() == 1);

    // finishClose 之后 fd 应该已经失效
    assert(fcntl(connFd, F_GETFD) == -1);
    assert(errno == EBADF);

    // Connection 构造数 = 析构数，证明没有对象泄漏
    const uint64_t constructed = Metrics::connection_constructed.load() - constructedBefore;
    const uint64_t destructed = Metrics::connection_destructed.load() - destructedBefore;
    assert(constructed == 1);
    assert(destructed == 1);
    assert(constructed == destructed);

    return 0;
}