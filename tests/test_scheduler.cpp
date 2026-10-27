#include "Runtime/Scheduler.h"

#include <cassert>
#include <iostream>

int main() {
    using runtime::Scheduler;
    using runtime::TaskState;

    Scheduler scheduler;

    // 本测试验证同一 clientId 下的 FIFO 与 ID 唯一性。
    constexpr int kClient = 1;
    (void)kClient;

    // 测试一：初始状态。
    assert(scheduler.taskCount() == 0);
    assert(scheduler.pendingCount() == 0);

    // 测试二：提交任务。
    assert(scheduler.submit(1, kClient, "Task A"));
    assert(scheduler.submit(2, kClient, "Task B"));
    assert(scheduler.submit(3, kClient, "Task C"));

    assert(scheduler.taskCount() == 3);
    assert(scheduler.pendingCount() == 3);

    // 测试三：重复 ID 必须被拒绝，不能覆盖原任务。
    assert(!scheduler.submit(1, kClient, "Duplicate Task"));

    assert(scheduler.taskCount() == 3);
    assert(scheduler.find(1)->input() == "Task A");

    // 测试四：按 FIFO 顺序调度。
    auto first = scheduler.dispatchNext();

    assert(first != nullptr);
    assert(first->id() == 1);
    assert(first->state() == TaskState::SCHEDULED);

    auto second = scheduler.dispatchNext();

    assert(second != nullptr);
    assert(second->id() == 2);
    assert(second->state() == TaskState::SCHEDULED);

    auto third = scheduler.dispatchNext();

    assert(third != nullptr);
    assert(third->id() == 3);
    assert(third->state() == TaskState::SCHEDULED);

    assert(scheduler.pendingCount() == 0);

    // 测试五：队列为空时返回 nullptr。
    assert(scheduler.dispatchNext() == nullptr);

    // 测试六：已调度的任务仍可按 ID 查询。
    auto found = scheduler.find(2);

    assert(found != nullptr);
    assert(found->input() == "Task B");
    assert(found->state() == TaskState::SCHEDULED);

    // 测试七：不存在的 ID 返回 nullptr。
    assert(scheduler.find(999) == nullptr);

    std::cout << "PASS: Scheduler tests\n";
    return 0;
}