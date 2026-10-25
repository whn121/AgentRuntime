#include "ThreadPool/IoThreadPool.h"
#include "Net/Connection.h"
#include "Logger/AsyncLogger.h"
#include <future>


IoThreadPool::IoThreadPool(int num) : num_(num > 0 ? num : 8)
{
    // 第二步: 创造所以Eventloop
    for (int i = 0; i < num_; ++i)
    {
        loops_.emplace_back(std::make_unique<EventLoop>());
    }
    //第二步: 启动所有工作线程
    try {
        for (int i = 0; i < num_; ++i)
            threads_.emplace_back([this, i] { worker(i); });
    } catch (...) { stop(); throw; }

    // 所有子 EventLoop 都记录了实际运行线程后，构造函数才返回。
    for (auto& loop : loops_)
    {
        loop->waitUntilStarted();
    }
}

IoThreadPool::~IoThreadPool() { stop(); }

void IoThreadPool::submit(int afd)
{
    if (stop_ || !buildconn_) { ::close(afd); return; }
    int idx = static_cast<int>(next_++ % static_cast<size_t>(num_));                     // 轮询选择EventLoop
    EventLoop* loop = loops_[idx].get();

    // 把“创建连接并注册”整个任务投递到子线程
    if (!loop->runInLoop([this, loop, afd, workCallback = workCallback_]
    {
        if (!loop->acceptsConnections()) { ::close(afd); return; }
        // 连接工厂已解耦到上层（buildconn_），此处不再直接创建协议对象

        if (buildconn_)
        {
            auto conn = buildconn_ (afd, loop);

            conn->init();
            conn->setcloseCallback([this, loop](int fd) { if (closeCallback_) closeCallback_(fd); loop->removeConnection(fd); });;
            conn->setCallBack(workCallback);

            // 注入业务处理器
            // 说明：Worker 线程执行 task.run_() 时，会调用这个 handler
            // handler 负责把 any 里的 MyMessage 取出来，交给上层业务
            conn->setBusinessHandler (businessHandler_);

            loop->updateChannel(conn->getChannel(), EPOLLIN);
            loop->updateConnection(conn);

        }
        else ::close(afd);
    })) ::close(afd);
}

void IoThreadPool::worker(int idx)
{
    loops_[idx]->loop();   // 启动事件循环（阻塞）
}

void IoThreadPool::closeAllConnections()
{
    std::vector<std::future<void>> futures;
    futures.reserve(loops_.size());

    // 向每个 EventLoop 投递关闭请求。
    for (auto& loop : loops_)
    {
        auto done = std::make_shared<std::promise<void>>();
        futures.emplace_back(done->get_future());

        loop->closeAllConnections([done]()
        {
            done->set_value();
        });
    }

    // 等待所有 EventLoop 都已执行关闭请求。
    // 此时部分连接仍可能有业务任务正在运行。
    for (auto& future : futures)
    {
        future.wait();
    }
}

void IoThreadPool::waitForPendingCallbacks()
{
    std::vector<std::future<void>> futures;
    futures.reserve(loops_.size());

    // 在每个 EventLoop 中投递一个屏障任务。
    for (auto& loop : loops_)
    {
        auto done = std::make_shared<std::promise<void>>();
        futures.emplace_back(done->get_future());

        if (!loop->runInLoop([done]() { done->set_value(); })) done->set_value();
    }

    // 所有屏障都完成后，调用前已排队的回调已处理。
    for (auto& future : futures)
    {
        future.wait();
    }
}


void IoThreadPool::stop()
{
    bool expected = false;
    if (!stop_.compare_exchange_strong (expected, true)) return; //比较并交换如果与1相当与2交换返回true,不相等吧原子赋值给1
    for (auto& loop : loops_) loop->stop();
    for (auto& t : threads_) if (t.joinable()) t.join();
}

void IoThreadPool::setCallBack(std::function<void(Task)> cb) { workCallback_ = cb; }
std::function<void(Task)> IoThreadPool::getCallback() { return workCallback_; }


void IoThreadPool::setbuildconn (std::function<std::shared_ptr<Connection> (int afd, EventLoop* loop)> bconn)
{
    buildconn_ = bconn;
}

void IoThreadPool::setBusinessHandler(BusinessHandler handler)
{
    businessHandler_ = std::move(handler);
}

void IoThreadPool::setCloseCallback(std::function<void(int)> cb)
{
    closeCallback_ = std::move(cb);
}