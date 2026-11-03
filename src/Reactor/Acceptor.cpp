#include "Reactor/Acceptor.h"
#include <fcntl.h>       // fcntl / open / O_CLOEXEC
#include <unistd.h>      // close
#include <netinet/tcp.h> //关闭Nagle算法(防止大量小包等待数据满了再发)
#include <cstring>       // strerror
#include "Logger/AsyncLogger.h"


// 构造函数：保存监听 fd、事件循环，创建 Channel，并预留一个 fd
Acceptor::Acceptor(int fd, EventLoop* loop)
    : fd_(fd), loop_(loop), channel_(std::make_unique<Channel>(fd))
{
    // 预留 fd 用于 EMFILE 时腾名额（见 handleAccept）
    reserve_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
}

Acceptor::~Acceptor()
{
    if (reserve_fd_ >= 0)
    {
        ::close(reserve_fd_);
        reserve_fd_ = -1;
    }
}

// 开始监听：将 handleAccept 注册为读事件回调，并加入 epoll
void Acceptor::listen() 
{
    channel_->setreadCallBack([this] { handleAccept(); });  // 绑定回调
    loop_->updateChannel(channel_.get(), EPOLLIN);           // 注册到 epoll，监听读事件
}

void Acceptor::stop()
{
    if (!channel_)
    {
        return;
    }

    // 必须在所属Eventloop线程中注销Channel
    loop_->assertInLoopThread();
    loop_->removeChannel(channel_.get());

    // 注销成功或失败后, 都不再让Acceptor持有Channel
    channel_.reset();
}

// 设置新连接到来时的外部回调（用于将新连接分发给 IO 线程池）
void Acceptor::setCallBack(std::function<void(int)> cb) 
{
    newConnectionCallback_ = cb;
}

// 处理新连接：accept 获得 fd，设为非阻塞，然后通过回调分发
//
// 这里是【水平触发】，一次通知 accept 到 EAGAIN 为止：
//  - EAGAIN          队列空，正常退出
//  - EINTR           被信号打断，重试
//  - EMFILE/ENFILE   fd 耗尽，必须消费掉一个连接，否则会 busy loop
//  - 其他错误        记录并退出（交给上层判断是否停服）
void Acceptor::handleAccept() 
{
    while (true) 
    {
        sockaddr_in addr{};                    // 客户端地址结构
        socklen_t len = sizeof(addr);          // 每轮重置：accept4 可能改写它

        int afd = accept4(fd_, (sockaddr*)&addr, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (afd < 0) 
        {
            if (errno == EINTR) 
            {
                continue;                      // 被信号中断，重试
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) 
            {
                break;                         // 内核没有更多连接了
            }

            if (errno == EMFILE || errno == ENFILE)
            {
                // fd 耗尽：先关掉预留 fd 腾出一个名额，accept 一个连接后立刻关掉，
                // 再把预留 fd 补回来。必须消费掉一个，否则 listen fd 仍然可读，
                // LT 会立刻再次触发，形成 100% CPU 的 busy loop。
                if (reserve_fd_ >= 0)
                {
                    ::close(reserve_fd_);
                    reserve_fd_ = -1;
                }

                int dropped = accept4(fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                if (dropped >= 0) ::close(dropped);

                reserve_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);

                LOG_ERROR("accept: fd exhausted (EMFILE/ENFILE), dropped one connection");
                continue;
            }

            // ECONNABORTED / EPROTO：连接在 accept 之前被对端重置，
            // 丢弃这一个继续取下一个，不算致命错误。
            if (errno == ECONNABORTED || errno == EPROTO)
            {
                continue;
            }

            LOG_ERROR(std::string("accept4 failed: ") + strerror(errno));
            break;
        }

        int one = 1;
        if (setsockopt(afd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
        {
            LOG_WARN("setsockopt TCP_NODELAY failed");
            // 不 return，TCP_NODELAY 不是必须
        }

        if (newConnectionCallback_) 
        {
            newConnectionCallback_(afd);
        }
        else ::close(afd);
    }
}
