#include "Runtime/RuntimeEngine.h"
#include "Runtime/CancellationToken.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

using namespace std::chrono_literals;

int main()
{
    std::mutex m;
    std::condition_variable cv;
    bool firstStarted = false;

    // executor：每个任务 sleep 100ms。第一个任务开始时通知主线程，
    // 保证 cancelByClient 调用时 pending 队列里还有剩余任务。
    auto executor = [&](const std::string& input, const runtime::CancellationToken&) -> std::string
    {
        {
            std::lock_guard<std::mutex> lock(m);
            if (!firstStarted)
            {
                firstStarted = true;
                cv.notify_all();
            }
        }
        std::this_thread::sleep_for(100ms);
        return input;
    };

    // 1 个 worker，pending 上限 16
    runtime::RuntimeEngine engine(executor, 1, 16);

    std::atomic<int> cancelledCount{0};
    std::atomic<int> succeededCount{0};

    // 提交 6 个任务，clientId=1
    for (int i = 1; i <= 6; ++i)
    {
        const bool ok = engine.submit(1, "task-" + std::to_string(i),
            [&](const std::shared_ptr<runtime::RuntimeTask>& task,
                runtime::WorkerExecutionResult result) {
                if (result == runtime::WorkerExecutionResult::FAILED &&
                    task && task->error() == "CLIENT_DISCONNECTED")
                {
                    cancelledCount.fetch_add(1);
                }
                else if (result == runtime::WorkerExecutionResult::SUCCEEDED)
                {
                    succeededCount.fetch_add(1);
                }
            });
        assert(ok);
    }

    // 等第一个任务开始执行
    {
        std::unique_lock<std::mutex> lock(m);
        const bool ready = cv.wait_for(lock, 2s, [&] { return firstStarted; });
        assert(ready);
        (void)ready;
    }

    // 模拟客户端断开：取消 clientId=1 的所有 pending 任务
    engine.cancelByClient(1);

    // 等所有任务完成
    engine.stop(runtime::RuntimeEngine::ShutdownMode::FORCE);

    // 第一个任务已开始执行，不受 cancelByClient 影响，会成功完成
    assert(succeededCount.load() == 1);
    // 其余 5 个在 pending 队列里，被 cancelByClient 取消
    assert(cancelledCount.load() == 5);

    // 另一个客户端不受影响
    std::atomic<int> otherClientDone{0};
    {
        runtime::RuntimeEngine engine2([](const std::string& input, const runtime::CancellationToken&) {
            return input;
        }, 1, 4);

        engine2.submit(2, "other", [&](const std::shared_ptr<runtime::RuntimeTask>&,
                                        runtime::WorkerExecutionResult result) {
            if (result == runtime::WorkerExecutionResult::SUCCEEDED)
            {
                otherClientDone.fetch_add(1);
            }
        });
        engine2.stop();
    }
    assert(otherClientDone.load() == 1);

    return 0;
}