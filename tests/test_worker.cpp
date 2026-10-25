
#include "Runtime/RuntimeTask.h"
#include "Runtime/Worker.h"

#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

int main() {
    using namespace runtime;

    // 测试 1：正常执行。
    Worker worker([](const std::string& input) {
        return "processed: " + input;
    });

    auto task = std::make_shared<RuntimeTask>(1, "hello");

    // Worker 要求任务已经被 Scheduler 调度。
    if (!task->schedule()) {
        std::cerr << "FAIL: task scheduling\n";
        return 1;
    }

    const auto result = worker.execute(task);

    if (result != WorkerExecutionResult::SUCCEEDED ||
        task->state() != TaskState::SUCCEEDED ||
        task->result() != "processed: hello" ||
        !task->error().empty()) {
        std::cerr << "FAIL: successful execution\n";
        return 1;
    }

    // 测试 2：执行器抛出异常。
    Worker failingWorker([](const std::string&) -> std::string {
        throw std::runtime_error("simulated failure");
    });

    auto failedTask =
        std::make_shared<RuntimeTask>(2, "fail");

    if (!failedTask->schedule()) {
        std::cerr << "FAIL: failed task scheduling\n";
        return 1;
    }

    const auto failedResult = failingWorker.execute(failedTask);

    if (failedResult != WorkerExecutionResult::FAILED ||
        failedTask->state() != TaskState::FAILED ||
        failedTask->error() != "simulated failure" ||
        !failedTask->result().empty()) {
        std::cerr << "FAIL: exception handling\n";
        return 1;
    }

    // 测试 3：空任务应该被拒绝。
    if (worker.execute(nullptr) !=
        WorkerExecutionResult::REJECTED) {
        std::cerr << "FAIL: null task rejection\n";
        return 1;
    }

    // 测试 4：未调度的任务不能直接执行。
    auto unscheduledTask =
        std::make_shared<RuntimeTask>(3, "not scheduled");

    if (worker.execute(unscheduledTask) !=
            WorkerExecutionResult::REJECTED ||
        unscheduledTask->state() != TaskState::PENDING) {
        std::cerr << "FAIL: unscheduled task rejection\n";
        return 1;
    }

    // 测试 5：终态任务不能重复执行。
    if (worker.execute(task) !=
            WorkerExecutionResult::REJECTED ||
        task->state() != TaskState::SUCCEEDED ||
        task->result() != "processed: hello") {
        std::cerr << "FAIL: duplicate execution rejection\n";
        return 1;
    }

    std::cout << "PASS: Worker execution tests\n";
    return 0;
}
