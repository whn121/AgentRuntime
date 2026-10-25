#include "Runtime/RuntimeTask.h"

#include <cassert>
#include <iostream>
#include <stdexcept>

int main() {
    using runtime::RuntimeTask;
    using runtime::TaskState;

    // 测试一：构造后的初始状态和输入。
    RuntimeTask task(1, "Analyze server logs");

    assert(task.id() == 1);
    assert(task.state() == TaskState::PENDING);
    assert(task.input() == "Analyze server logs");
    assert(!task.isTerminal());

    // 测试二：不能跳过调度直接运行。
    assert(!task.start());
    assert(task.state() == TaskState::PENDING);

    // 测试三：正常执行路径。
    assert(task.schedule());
    assert(task.state() == TaskState::SCHEDULED);

    assert(task.start());
    assert(task.state() == TaskState::RUNNING);

    assert(task.succeed("Analysis completed"));
    assert(task.state() == TaskState::SUCCEEDED);
    assert(task.result() == "Analysis completed");
    assert(task.error().empty());
    assert(task.isTerminal());

    // 测试四：成功后不允许再次转换状态。
    assert(!task.schedule());
    assert(!task.start());
    assert(!task.fail("late failure"));
    assert(task.state() == TaskState::SUCCEEDED);

    // 测试五：失败路径。
    RuntimeTask failed_task(2, "Analyze broken logs");

    assert(failed_task.schedule());
    assert(failed_task.start());
    assert(failed_task.fail("Model execution failed"));

    assert(failed_task.state() == TaskState::FAILED);
    assert(failed_task.error() == "Model execution failed");
    assert(failed_task.result().empty());
    assert(failed_task.isTerminal());

    // 测试六：非法任务 ID 必须被拒绝。
    bool threw = false;

    try {
        RuntimeTask invalid_task(0, "invalid");
    } catch (const std::invalid_argument&) {
        threw = true;
    }

    assert(threw);

    std::cout << "PASS: RuntimeTask state machine tests\n";
    return 0;
}
