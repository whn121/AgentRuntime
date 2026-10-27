#include "Runtime/Scheduler.h"
#include <cassert>
#include <vector>

using namespace runtime;

int main()
{
    Scheduler scheduler;

    // 客户端 A 提交 4 个任务
    for (int i = 0; i < 4; ++i)
    {
        assert(scheduler.submit(100 + i, /*clientId=*/1, "a"));
    }

    // 客户端 B 提交 1 个任务
    assert(scheduler.submit(200, /*clientId=*/2, "b"));

    // 断言 1：轮转调度下，B 的任务应在前 2 次 dispatchNext 内被取出
    std::vector<TaskId> order;
    for (int i = 0; i < 5; ++i)
    {
        auto task = scheduler.dispatchNext();
        if (!task) break;
        order.push_back(task->id());
    }

    assert(order.size() == 5);
    bool bFoundEarly = (order[0] == 200 || order[1] == 200);
    assert(bFoundEarly);
    (void)bFoundEarly;

    // 断言 2：每个客户端配额生效
    // 先清空 A 的旧队列（已全部被取出），重新提交
    // 注意：上面 5 次 dispatchNext 已把 A 的 4 个和 B 的 1 个全部取出
    // pendingTotal 应为 0
    assert(scheduler.pendingCount() == 0);

    // 重新提交 A 的 MAX_PENDING_PER_CLIENT + 1 个任务
    for (std::size_t i = 0; i < Scheduler::MAX_PENDING_PER_CLIENT; ++i)
    {
        assert(scheduler.submit(300 + i, /*clientId=*/1, "a"));
    }
    // 第 MAX_PENDING_PER_CLIENT + 1 个应被拒绝
    assert(!scheduler.submit(999, /*clientId=*/1, "a"));

    // 断言 3：客户端 B 仍能提交（A 的配额满不影响 B）
    assert(scheduler.submit(888, /*clientId=*/2, "b"));

    return 0;
}