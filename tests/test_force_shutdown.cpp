#include "Runtime/RuntimeEngine.h"
#include "Runtime/CancellationToken.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std::chrono_literals;

int main()
{
    std::mutex m;
    std::condition_variable cv;
    bool firstStarted = false;
    bool releaseFirst = false;

    // executor：第一个任务进入后阻塞，后续任务不会被执行
    auto executor = [&](const std::string& input, const runtime::CancellationToken& token) -> std::string
    {
        std::unique_lock<std::mutex> lock(m);
        if (!firstStarted)
        {
            firstStarted = true;
            cv.notify_all();
        }
        // 每 10ms 检查一次令牌。不能用 cv.wait 死等 releaseFirst
        // 因为 stop() 通知的是 RuntimeEngine 内部的 cv_，不是这里的 cv
        // 死等的话 stop() 的 join() 永远等不到 worker 退出
        while (!releaseFirst && !token.cancelled())
        {
            if (cv.wait_for(lock, 10ms, [&] { return releaseFirst || token.cancelled(); }))
            {
                break;
            }
        }

        if (token.cancelled()) throw std::runtime_error("CANCELLED");
        return input;
    };

    runtime::RuntimeEngine engine(executor, 1, 8);

    std::atomic<int> firstDone{0};
    std::atomic<int> pendingFailed{0};
    std::atomic<int> pendingSucceeded{0};

    // 第一个任务：会占住唯一的 worker
    bool ok = engine.submit(1, "block",
        [&](const std::shared_ptr<runtime::RuntimeTask>&,
            runtime::WorkerExecutionResult) {
            firstDone.fetch_add(1);
        });
    assert(ok);

    // 等第一个任务真正进入 executor
    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait_for(lock, 2s, [&] { return firstStarted; });
    }

    // 后续 5 个任务：全部堆在 pending 队列
    for (int i = 2; i <= 6; ++i)
    {
        ok = engine.submit(i, "pending-" + std::to_string(i),
            [&](const std::shared_ptr<runtime::RuntimeTask>& task,
                runtime::WorkerExecutionResult result) {
                if (result == runtime::WorkerExecutionResult::FAILED &&
                    task && task->error() == "SERVER_SHUTTING_DOWN")
                {
                    pendingFailed.fetch_add(1);
                }
                else
                {
                    pendingSucceeded.fetch_add(1);
                }
            });
        assert(ok);
    }

    // FORCE 关闭：5 个 pending 任务应全部失败，first 任务被取消
    engine.stop(runtime::RuntimeEngine::ShutdownMode::FORCE);

    // 断言：5 个 pending 任务全部收到 SERVER_SHUTTING_DOWN
    assert(pendingFailed.load() == 5);
    assert(pendingSucceeded.load() == 0);

    // 断言：第一个任务也走完了（被取消后 executor 返回 throw）
    assert(firstDone.load() == 1);

    return 0;
}