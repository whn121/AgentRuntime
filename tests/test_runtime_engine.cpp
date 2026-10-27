#include "Runtime/RuntimeEngine.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

int main()
{
    // Test 1: successful execution and result delivery.
    {
        runtime::RuntimeEngine engine(
            [](const std::string& input, const runtime::CancellationToken&) {
                return "processed: " + input;
            },
            2, 16);

        std::mutex mutex;
        std::condition_variable cv;
        bool completed = false;
        bool correct = false;

        if (!engine.submit(1, "hello",
            [&](const std::shared_ptr<runtime::RuntimeTask>& task,
                runtime::WorkerExecutionResult result) {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    completed = true;
                    correct = task &&
                        result == runtime::WorkerExecutionResult::SUCCEEDED &&
                        task->state() == runtime::TaskState::SUCCEEDED &&
                        task->result() == "processed: hello";
                }
                cv.notify_one();
            })) {
            std::cerr << "FAIL: normal task submission\n";
            return 1;
        }

        {
            std::unique_lock<std::mutex> lock(mutex);
            if (!cv.wait_for(lock, 2s, [&] { return completed; }) || !correct) {
                std::cerr << "FAIL: normal task completion/result\n";
                return 1;
            }
        }

        engine.stop();
        engine.stop();

        if (engine.pendingCount() != 0 || engine.activeCount() != 0) {
            std::cerr << "FAIL: engine did not drain after stop\n";
            return 1;
        }
    }

    // Test 2: executor exceptions become failed tasks.
    {
        runtime::RuntimeEngine engine(
            [](const std::string&, const runtime::CancellationToken&) -> std::string {
                throw std::runtime_error("simulated failure");
            },
            1, 4);

        std::mutex mutex;
        std::condition_variable cv;
        bool completed = false;
        bool correct = false;

        if (!engine.submit(1, "fail",
            [&](const std::shared_ptr<runtime::RuntimeTask>& task,
                runtime::WorkerExecutionResult result) {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    completed = true;
                    correct = task &&
                        result == runtime::WorkerExecutionResult::FAILED &&
                        task->state() == runtime::TaskState::FAILED &&
                        task->error() == "simulated failure";
                }
                cv.notify_one();
            })) {
            std::cerr << "FAIL: failure task submission\n";
            return 1;
        }

        {
            std::unique_lock<std::mutex> lock(mutex);
            if (!cv.wait_for(lock, 2s, [&] { return completed; }) || !correct) {
                std::cerr << "FAIL: executor exception handling\n";
                return 1;
            }
        }

        engine.stop();
    }

    // Test 3: pending queue capacity is enforced deterministically.
    // 三层上限同时存在：全局 maxPendingTasks=1、单客户端 256、Worker=1。
    // 用 clientId=1 提交全部三个任务，验证全局上限先触发。
    {
        std::mutex mutex;
        std::condition_variable cv;
        bool executorStarted = false;
        bool releaseExecutor = false;

        runtime::RuntimeEngine engine(
            [&](const std::string& input, const runtime::CancellationToken&) {
                std::unique_lock<std::mutex> lock(mutex);
                executorStarted = true;
                cv.notify_all();
                cv.wait(lock, [&] { return releaseExecutor; });
                return input;
            },
            1, 1);

        if (!engine.submit(1, "running", [](const auto&, auto) {})) {
            std::cerr << "FAIL: first task submission\n";
            return 1;
        }

        {
            std::unique_lock<std::mutex> lock(mutex);
            if (!cv.wait_for(lock, 2s, [&] { return executorStarted; })) {
                std::cerr << "FAIL: executor did not start\n";
                return 1;
            }
        }

        // The only worker is blocked; one task can now occupy the pending queue.
        if (!engine.submit(1, "queued", [](const auto&, auto) {})) {
            std::cerr << "FAIL: task should fit in pending queue\n";
            {
                std::lock_guard<std::mutex> lock(mutex);
                releaseExecutor = true;
            }
            cv.notify_all();
            engine.stop();
            return 1;
        }

        if (engine.submit(1, "overflow", [](const auto&, auto) {})) {
            std::cerr << "FAIL: queue accepted task beyond capacity\n";
            {
                std::lock_guard<std::mutex> lock(mutex);
                releaseExecutor = true;
            }
            cv.notify_all();
            engine.stop();
            return 1;
        }

        {
            std::lock_guard<std::mutex> lock(mutex);
            releaseExecutor = true;
        }
        cv.notify_all();

        engine.stop();

        if (engine.pendingCount() != 0 || engine.activeCount() != 0) {
            std::cerr << "FAIL: queue/worker did not drain\n";
            return 1;
        }
    }

    // Test 4: every accepted task completes when stop drains the engine.
    // taskCount=20 < 256，单客户端也不会触发配额；使用分散 clientId
    // 是为了让测试不隐含"单客户端独占"的假设。
    {
        constexpr int taskCount = 20;
        std::atomic<int> completed{0};

        runtime::RuntimeEngine engine(
            [](const std::string& input, const runtime::CancellationToken&) {
                std::this_thread::sleep_for(2ms);
                return input;
            },
            3, taskCount);

        for (int i = 0; i < taskCount; ++i) {
            const int clientId = 1 + (i % 4);
            if (!engine.submit(clientId, std::to_string(i),
                [&](const auto&, auto) {
                    completed.fetch_add(1, std::memory_order_relaxed);
                })) {
                std::cerr << "FAIL: accepted-task submission " << i << '\n';
                engine.stop();
                return 1;
            }
        }

        engine.stop();

        if (completed.load(std::memory_order_relaxed) != taskCount ||
            engine.pendingCount() != 0 ||
            engine.activeCount() != 0) {
            std::cerr << "FAIL: stop did not drain all accepted tasks; completed="
                      << completed.load() << '\n';
            return 1;
        }

        if (engine.submit(1, "after-stop", [](const auto&, auto) {})) {
            std::cerr << "FAIL: engine accepted task after stop\n";
            return 1;
        }
    }

    std::cout << "PASS: RuntimeEngine tests\n";
    return 0;
}