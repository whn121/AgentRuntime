
#include "Runtime/RuntimeTask.h"

#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

int main() {
    using namespace runtime;

    RuntimeTask task(1, "concurrent-test");

    // 先将任务推进到 SCHEDULED 状态
    if (!task.schedule()) {
        std::cerr << "FAIL: schedule task\n";
        return 1;
    }

    constexpr int threadCount = 16;

    // 控制线程尽可能同时尝试启动任务
    std::atomic<bool> startFlag{false};

    // 统计成功启动的线程数量
    std::atomic<int> successCount{0};

    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (int i = 0; i < threadCount; ++i) {
        threads.emplace_back([&]() {
            // 等待主线程发出开始信号
            while (!startFlag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }

            if (task.start()) {
                successCount.fetch_add(
                    1, std::memory_order_relaxed);
            }
        });
    }

    // 释放所有等待中的线程
    startFlag.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    // 只能有一个线程成功完成状态转换
    if (successCount.load() != 1) {
        std::cerr << "FAIL: success count = "
                  << successCount.load() << '\n';
        return 1;
    }

    if (task.state() != TaskState::RUNNING) {
        std::cerr << "FAIL: unexpected task state\n";
        return 1;
    }

    std::cout << "PASS: concurrent task start test\n";
    return 0;
}
