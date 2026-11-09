#include "Reactor/Acceptor.h"
#include <fcntl.h>       // fcntl / open / O_CLOEXEC
#include <unistd.h>      // close
#include <netinet/tcp.h> //关闭Nagle算法(防止大量小包等待数据满了再发)
#include <sys/timerfd.h> // EMFILE 非阻塞退避
#include <cstring>       // strerror
#include "Logger/AsyncLogger.h"


// 构造函数：保存监听 fd、事件循环，创建 Channel，并预留一个 fd
Acceptor::Acceptor(int fd, EventLoop* loop)
    : fd_(fd), loop_(loop), channel_(std::make_unique<Channel>(fd))
{
    // 预留 fd 用于 EMFILE 时腾名额（见 handleAccept）
    reserve_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);

    // 预创建退避定时器：fd 耗尽时再创建会失败，所以必须现在就建好；
    // 平时不上弦（it_value = 0），不产生任何事件。
    backoff_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (backoff_fd_ >= 0)
    {
        backoff_channel_ = std::make_unique<Channel>(backoff_fd_);
        backoff_channel_->setreadCallBack([this] { HandleBackoff(); });
        loop_->updateChannel(backoff_channel_.get(), EPOLLIN);
    }
    else
    {
        LOG_WARN(std::string("timerfd_create for EMFILE backoff failed: ") + strerror(errno)
                 + "; fallback = resume reading immediately");
    }
}

Acceptor::~Acceptor()
{
    if (backoff_channel_)
    {
        loop_->removeChannel(backoff_channel_.get());
        backoff_channel_.reset();
    }
    if (backoff_fd_ >= 0)
    {
        ::close(backoff_fd_);
        backoff_fd_ = -1;
    }
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
    channel_.reset();

    // 退避定时器也要摘掉，避免停服后它仍挂在 epoll 上
    if (backoff_channel_)
    {
        loop_->removeChannel(backoff_channel_.get());
        backoff_channel_.reset();
    }
}

// ── EMFILE 处理：批量丢弃 ──
// 每次腾挪只能消费一个连接；积压多时要循环，但必须有上限，
// 否则等于把 busy loop 换成"有上限的 busy loop"。
int Acceptor::DropBurstedConnections()
{
    int dropped = 0;

    while (dropped < kMaxDropPerRound)
    {
        // 腾出一个名额：关预留 → accept 一个 → 立刻关掉 → 补回预留
        if (reserve_fd_ >= 0)
        {
            ::close(reserve_fd_);
            reserve_fd_ = -1;
        }

        const int victim = accept4(fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (victim >= 0)
        {
            ::close(victim);   // 不服务这个连接，只把它从队列里消费掉
        }

        reserve_fd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        ++dropped;

        if (victim < 0)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;          // 队列空了
            if (errno != EMFILE && errno != ENFILE) break;               // 其他错误，交给外层
        }
    }

    return dropped;
}

// ── EMFILE 处理：非阻塞退避 ──
void Acceptor::ArmBackoff()
{
    if (backoff_fd_ < 0)
    {
        // 没有定时器就立刻恢复读，宁可多消耗点 CPU，也不能永久不接受连接
        channel_->enableReading();
        loop_->updateChannelEvent(channel_.get(), channel_->getEvents());
        return;
    }

    // 摘掉 listen 的 EPOLLIN：退避期间不再被"有新连接"唤醒
    channel_->disableReading();
    loop_->updateChannelEvent(channel_.get(), channel_->getEvents());

    itimerspec its{};
    its.it_value.tv_sec = 0;
    its.it_value.tv_nsec = static_cast<long>(kBackoffMs) * 1000L * 1000L;
    its.it_interval.tv_sec = 0;    // ★ 一次性：到点触发一次，之后自动停
    its.it_interval.tv_nsec = 0;

    if (::timerfd_settime(backoff_fd_, 0, &its, nullptr) < 0)
    {
        LOG_WARN(std::string("EMFILE backoff timerfd_settime failed: ") + strerror(errno)
                 + "; resume reading immediately");
        channel_->enableReading();
        loop_->updateChannelEvent(channel_.get(), channel_->getEvents());
        return;
    }

    LOG_WARN("accept: fd exhausted; backing off " + std::to_string(kBackoffMs) + "ms");
}

void Acceptor::HandleBackoff()
{
    // 必须读掉到期次数，否则 timerfd 仍然可读 → LT 会一直触发（又是一个 busy loop）
    uint64_t expirations = 0;
    const ssize_t n = ::read(backoff_fd_, &expirations, sizeof(expirations));
    if (n != static_cast<ssize_t>(sizeof(expirations)))
    {
        LOG_WARN("EMFILE backoff timerfd read failed");
    }

    channel_->enableReading();
    loop_->updateChannelEvent(channel_.get(), channel_->getEvents());
    LOG_INFO("accept: backoff finished; resume accepting");
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
                // 批量丢弃 + 必要时非阻塞退避（见 Acceptor.h 的说明）。
                // 不在这里 sleep：那会把主 loop 停住。
                const int dropped = DropBurstedConnections();
                if (dropped >= kMaxDropPerRound)
                {
                    ArmBackoff();   // 还打不住 → 摘 EPOLLIN + 一次性 timerfd 退避
                }
                return;             // 立刻回到 epoll_wait
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
