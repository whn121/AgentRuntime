#include "Runtime/RuntimeEngine.h"
#include "Runtime/CancellationToken.h"

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

using namespace std::chrono_literals;

int main()
{
    std::mutex m;
    std::condition_variable cv;
    bool started = false;

    // 模拟 LlamaClient::complete 的协作式行为：
    // 每 50ms 检查一次令牌, 取消后返回。和 CURLOPT_XFERINFOFUNCTION 的
    // 周期性回调语义一致。
    auto executor = [&](const std::string&, const runtime::CancellationToken& token) -> std::string
    {
        std::unique_lock<std::mutex> lock(m);
        started = true;
        cv.notify_all();

        while (!token.cancelled())
        {
            if (cv.wait_for(lock, 50ms, [&] { return token.cancelled(); }))
            {
                break;
            }
        }

        throw std::runtime_error("CANCELLED");
    };

    runtime::RuntimeEngine engine(executor, 1, 4);

    std::atomic<int> completed{0};
    const bool submitted = engine.submit(1, "long-task",
        [&](const std::shared_ptr<runtime::RuntimeTask>&,
            runtime::WorkerExecutionResult) {
            completed.fetch_add(1, std::memory_order_relaxed);
        });
    assert(submitted);
    (void)submitted;

    {
        std::unique_lock<std::mutex> lock(m);
        // 不能用 assert 包住 wait_for：Release 下 NDEBUG 会整句删掉，
        // 主线程不等待 executor 启动就 stop()，此时 activeTokens_ 还是空的，
        // 取消不传播，join() 卡死。
        const bool executorReady = cv.wait_for(lock, 2s, [&] { return started; });
        assert(executorReady);
        (void)executorReady;
    }

    const auto begin = std::chrono::steady_clock::now();
    engine.stop();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - begin);

    // 协作式取消应在 200ms 内生效。如果 stop() 等了 CURLOPT_TIMEOUT_MS
    // 级别的时长(120s), 说明取消没有传播到 executor_。
    assert(elapsed < 200ms);
    assert(completed.load() == 1);

    return 0;
}