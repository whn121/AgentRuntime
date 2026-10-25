#pragma once

#include "Reactor/Acceptor.h"
#include "Reactor/EventLoop.h"
#include "ThreadPool/IoThreadPool.h"
#include "ThreadPool/WorkThreadPool.h"
#include "Net/Socket.h"

#include <memory>
#include <functional>
#include <atomic>


// 服务器主类，组装所有组件
class Server 
{
public:
    Server();
    ~Server();
    bool start();   // 启动服务
    void stop();    // 停止服务
    void setcreator (std::function<std::shared_ptr<Connection> (int afd, EventLoop* loop)>); //设置对调为了解耦传给io

    void setBusinessHandler(IoThreadPool::BusinessHandler handler);
    void setCloseCallback(std::function<void(int)> cb);

    // 要主动暴露主线程网关拿不到不行
    EventLoop* getMainLoop() { return &mainloop_; } // 获得主线程

    void setBeforeIoShutdownCallback(std::function<void()> cb);

    // 将 Gateway 出站连接产生的业务任务提交到现有工作线程池
    void submitWork(Task task);

private:
    Socket listen_socket_;               // 监听套接字
    EventLoop mainloop_;                 // 主事件循环
    std::unique_ptr<Acceptor> acceptor_; // 接受器
    IoThreadPool iopool_;                // IO线程池
    WorkThreadPool workpool_;            // 业务线程池
    std::function<std::shared_ptr<Connection> (int afd, EventLoop* loop)> creator_;
    IoThreadPool::BusinessHandler businessHandler_;
    std::function<void(int)> closeCallback_;
    // 防止 Server::stop() 被重复执行
    std::atomic<bool> stopStarted_{false};
    std::atomic<bool> started_{false};

    std::function<void()> beforeIoShutdownCallback_;
};