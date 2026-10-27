
#include "Runtime/RuntimeTask.h"
#include "Runtime/Worker.h"
#include "Runtime/CancellationToken.h"

#include <atomic>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

int main() {
    using namespace runtime;

    // 统计 Executor 实际执行次数
    std::atomic<int> executorCalls{0};

    Worker worker([&](const std::string& input, const runtime::CancellationToken&) {
        executorCalls.fetch_add(1, std::memory_order_relaxed);
        return "processed: " + input;
    });

    auto task = std::make_shared<RuntimeTask>(1, "test-input");

    if (!task->schedule()) {
        std::cerr << "FAIL: schedule task\n";
        return 1;
    }

    constexpr int threadCount = 16;

    std::atomic<bool> startFlag{false};
    std::atomic<int> successCount{0};
    std::atomic<int> rejectedCount{0};

    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (int i = 0; i < threadCount; ++i) {
        threads.emplace_back([&]() {
            while (!startFlag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            const auto result = worker.execute(task);

            if (result == WorkerExecutionResult::SUCCEEDED) {
                successCount.fetch_add(1, std::memory_order_relaxed);
            } else if (result == WorkerExecutionResult::REJECTED) {
                rejectedCount.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    startFlag.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    if (successCount.load() != 1) {
        std::cerr << "FAIL: success count = "
                  << successCount.load() << '\n';
        return 1;
    }

    if (rejectedCount.load() != threadCount - 1) {
        std::cerr << "FAIL: rejected count = "
                  << rejectedCount.load() << '\n';
        return 1;
    }

    if (executorCalls.load() != 1) {
        std::cerr << "FAIL: executor calls = "
                  << executorCalls.load() << '\n';
        return 1;
    }

    if (task->state() != TaskState::SUCCEEDED) {
        std::cerr << "FAIL: task did not succeed\n";
        return 1;
    }

    if (task->result() != "processed: test-input") {
        std::cerr << "FAIL: unexpected task result\n";
        return 1;
    }

    std::cout << "PASS: concurrent Worker execution test\n";
    return 0;
}
