// tests/test_close_drain.cpp 的结构
#include "Net/Connection.h"
#include "Reactor/EventLoop.h"
#include "Reactor/Channel.h"
#include "Protocol/MyProtocol.h"
#include "ThreadPool/IoThreadPool.h"
#include "ThreadPool/Task.h"
#include "ThreadPool/TaskQueue.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

int main()
{
    std::atomic<int> closeCount{0};
    std::atomic<int> fdClosed{0};

    // 1. 一个 IO 线程的池
    IoThreadPool pool(1);

    // 2. workCallback：成功入队，但从不执行（模拟"任务在途"）
    //    这样 taskRunning_ 永远是 true
    pool.setCallBack([](Task&& task) {
        // 模拟 Worker：执行任务并通知完成，使 taskRunning_ 归零。
        if (task.run_) task.run_();
        if (task.onComplete_) task.onComplete_();
        return TaskQueue::PushResult::Success;
    });

    // 3. 连接工厂 + 关闭回调
    pool.setbuildconn([](int fd, EventLoop* loop) {
        return std::make_shared<Connection>(
            fd, std::make_unique<Channel>(fd), loop, std::make_unique<MyProtocol>());
    });
    pool.setCloseCallback([&](int fd) {
        closeCount.fetch_add(1);
        if (fd >= 0) { ::close(fd); fdClosed.fetch_add(1); }
    });
    pool.setBusinessHandler([](std::shared_ptr<Connection>, const std::any&) {});

    // 4. 通过 socketpair 造一个入站连接
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 1;
    pool.submit(sv[0]);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 5. 发一个合法的 RUNTIME_TASK_REQ 帧，触发 enqueueTask → taskRunning_ == true
    const std::string frame = MyProtocol::encodeMessage(
        MyType::RUNTIME_TASK_REQ, /*id=*/1, /*payload=*/"hello");
    (void)!::write(sv[1], frame.data(), frame.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 6. 触发停服。EventLoop::loop() 退出时应执行 1a-10 的兜底。
    pool.stop();

    // 7. 断言
    assert(closeCount.load() == 1);       // closeCallBack_ 恰好一次
    assert(fdClosed.load() == 1);         // 底层 fd 被关闭一次

    // 清理：sv[1] 由测试关闭
    ::close(sv[1]);
    return 0;
}