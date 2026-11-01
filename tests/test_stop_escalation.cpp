// 回归测试：stop() 的幂等性与重入语义。
//
// 旧实现用 std::call_once 包住整个关闭流程，带来三个问题：
//   1. 第一次 stop(GRACEFUL) 之后，再调 stop(FORCE) 是空操作，无法升级；
//   2. 在 worker 线程里调 stop()（例如 completion 回调里）会 join 自己，
//      抛 std::system_error 后被 catch(...) 吞掉，关闭流程悄悄中断；
//   3. 关闭标志与 join 绑在一起，重复调用只能靠 call_once 兜底。
//
// 这里只断言对外可观察的契约：
//   - stop() 可以重复调用，不崩不挂；
//   - FORCE 排空 pending 后，每个 completion 恰好回调一次（重复 stop 不会翻倍）；
//   - 在 completion 回调里调 stop() 不死锁，引擎仍能正常析构。
//
// 注意：本文件不用 assert，避免 Release 构建下 -DNDEBUG 把断言全部编译掉。
#include "Runtime/RuntimeEngine.h"
#include "Runtime/CancellationToken.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std::chrono_literals;

static int g_failures = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::printf("CHECK failed: %s at line %d\n", #cond, __LINE__); \
            ++g_failures;                                                  \
        }                                                                  \
    } while (0)

// 1) 没有任务时反复 stop()，不应崩或挂住
static void TestRepeatedStopIsSafe()
{
    runtime::RuntimeEngine engine(
        [](const std::string& input, const runtime::CancellationToken&) { return input; },
        2, 8);

    engine.stop();
    engine.stop();
    engine.stop(runtime::RuntimeEngine::ShutdownMode::FORCE);
    engine.stop();
    CHECK(true);   // 活到这里就说明没崩
}

// 2) FORCE 排空 pending，重复调用不会让 completion 回调翻倍
static void TestForceDrainsExactlyOnce()
{
    std::mutex m;
    std::condition_variable cv;
    bool firstStarted = false;

    auto executor = [&](const std::string&, const runtime::CancellationToken& token) -> std::string
    {
        {
            std::unique_lock<std::mutex> lock(m);
            if (!firstStarted)
            {
                firstStarted = true;
                cv.notify_all();
            }
        }
        // 每 10ms 看一次取消令牌，保证 stop() 的 join 能退出
        for (int i = 0; i < 500; ++i)
        {
            if (token.cancelled()) throw std::runtime_error("CANCELLED");
            std::this_thread::sleep_for(2ms);
        }
        return "done";
    };

    runtime::RuntimeEngine engine(executor, 1, 16);

    std::atomic<int> pendingCallbacks{0};
    std::atomic<int> totalCallbacks{0};

    CHECK(engine.submit(1, "block", [&](const std::shared_ptr<runtime::RuntimeTask>&,
                                        runtime::WorkerExecutionResult) {
        totalCallbacks.fetch_add(1);
    }));

    {
        std::unique_lock<std::mutex> lock(m);
        cv.wait_for(lock, 2s, [&] { return firstStarted; });
    }

    for (int i = 2; i <= 5; ++i)
    {
        CHECK(engine.submit(i, "pending-" + std::to_string(i),
            [&](const std::shared_ptr<runtime::RuntimeTask>& task,
                runtime::WorkerExecutionResult result) {
                totalCallbacks.fetch_add(1);
                if (result == runtime::WorkerExecutionResult::FAILED && task &&
                    task->error() == "SERVER_SHUTTING_DOWN")
                {
                    pendingCallbacks.fetch_add(1);
                }
            }));
    }

    engine.stop(runtime::RuntimeEngine::ShutdownMode::FORCE);
    engine.stop(runtime::RuntimeEngine::ShutdownMode::FORCE);
    engine.stop();

    CHECK(pendingCallbacks.load() == 4);   // 4 个 pending 各一次
    CHECK(totalCallbacks.load() == 5);     // 加上第一个正在执行的任务，共 5 次，不能翻倍
}

// 3) 在 completion 回调里（worker 线程）调 stop()：不能 join 自己
static void TestStopFromCompletionCallback()
{
    std::atomic<int> completed{0};
    std::atomic<bool> stopReturned{false};

    runtime::RuntimeEngine* enginePtr = nullptr;

    runtime::RuntimeEngine engine(
        [](const std::string& input, const runtime::CancellationToken&) { return input; },
        1, 8);

    enginePtr = &engine;

    CHECK(engine.submit(1, "hello", [&](const std::shared_ptr<runtime::RuntimeTask>&,
                                        runtime::WorkerExecutionResult) {
        enginePtr->stop();          // 在 worker 线程里关闭
        stopReturned.store(true);
        completed.fetch_add(1);
    }));

    // 等回调执行完；如果 stop() 死锁，这里会超时
    for (int i = 0; i < 500 && !stopReturned.load(); ++i)
    {
        std::this_thread::sleep_for(2ms);
    }

    CHECK(stopReturned.load());
    CHECK(completed.load() == 1);

    // 析构时再 join 一次，必须正常返回
    engine.stop();
    CHECK(true);
}

int main()
{
    TestRepeatedStopIsSafe();
    TestForceDrainsExactlyOnce();
    TestStopFromCompletionCallback();

    if (g_failures == 0)
    {
        std::printf("All stop-escalation tests passed.\n");
        return 0;
    }
    std::printf("%d checks failed.\n", g_failures);
    return 1;
}
