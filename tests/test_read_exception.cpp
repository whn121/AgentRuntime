#include "Net/Connection.h"
#include "Protocol/Protocol.h"
#include "Reactor/Channel.h"
#include "Reactor/EventLoop.h"
#include "ThreadPool/IoThreadPool.h"
#include "ThreadPool/Task.h"
#include "ThreadPool/TaskQueue.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <optional>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

// parse 永远抛异常的 Protocol
class ThrowingProtocol : public Protocol
{
public:
    ParseResult parse(Buffer&) override
    {
        throw std::runtime_error("simulated parse failure");
    }
    std::string encode(const std::any&) override { return {}; }
    std::any getMessage() override { return {}; }
    void reset() override {}
    std::optional<std::any> getErrorResponse() override { return std::nullopt; }
};

int main()
{
    std::atomic<int> closeCount{0};

    {
        IoThreadPool pool(1);

        pool.setCallBack([](Task&&) { return TaskQueue::PushResult::Success; });

        pool.setbuildconn([](int fd, EventLoop* loop) {
            return std::make_shared<Connection>(
                fd,
                std::make_unique<Channel>(fd),
                loop,
                std::make_unique<ThrowingProtocol>());
        });

        pool.setCloseCallback([&](int fd) {
            closeCount.fetch_add(1);
            if (fd >= 0) ::close(fd);
        });

        pool.setBusinessHandler([](std::shared_ptr<Connection>, const std::any&) {});

        int sv[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 1;
        pool.submit(sv[0]);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // 发一帧数据触发 read() → parse() 抛异常
        const char dummy[] = "x";
        (void)!::write(sv[1], dummy, 1);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        pool.stop();

        ::close(sv[1]);
    }

    // parse 抛异常 → read 抛异常 → loop catch → close → closeCallback 一次
    assert(closeCount.load() == 1);
    return 0;
}