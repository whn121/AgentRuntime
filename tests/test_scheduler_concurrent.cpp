
#include "Runtime/Scheduler.h"

#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

int main() {
    using namespace runtime;

    Scheduler scheduler;

    constexpr int threadCount = 4;
    constexpr int tasksPerThread = 1000;
    constexpr int totalTasks = threadCount * tasksPerThread;

    // 阶段 1：多个线程同时提交任务。
    std::atomic<int> submitSuccess{0};
    std::vector<std::thread> submitters;

    for (int t = 0; t < threadCount; ++t) {
        submitters.emplace_back([&, t]() {
            for (int i = 0; i < tasksPerThread; ++i) {
                const TaskId id =
                    static_cast<TaskId>(t * tasksPerThread + i + 1);

                if (scheduler.submit(
                        id, "input-" + std::to_string(id))) {
                    submitSuccess.fetch_add(
                        1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& thread : submitters) {
        thread.join();
    }

    if (submitSuccess.load() != totalTasks ||
        scheduler.taskCount() != totalTasks ||
        scheduler.pendingCount() != totalTasks) {
        std::cerr << "FAIL: concurrent submission\n";
        return 1;
    }

    // 阶段 2：多个线程同时领取任务。
    std::atomic<int> dispatchSuccess{0};
    std::atomic<int> invalidState{0};
    std::vector<std::thread> dispatchers;

    for (int t = 0; t < threadCount; ++t) {
        dispatchers.emplace_back([&]() {
            while (true) {
                auto task = scheduler.dispatchNext();

                if (!task) {
                    break;
                }

                // 每次领取到的任务都必须已进入 SCHEDULED。
                if (task->state() != TaskState::SCHEDULED) {
                    invalidState.fetch_add(
                        1, std::memory_order_relaxed);
                }

                dispatchSuccess.fetch_add(
                    1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& thread : dispatchers) {
        thread.join();
    }

    if (dispatchSuccess.load() != totalTasks ||
        invalidState.load() != 0 ||
        scheduler.pendingCount() != 0) {
        std::cerr << "FAIL: concurrent dispatch\n";
        return 1;
    }

    std::cout << "PASS: concurrent Scheduler tests\n";
    return 0;
}

