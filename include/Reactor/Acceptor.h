#pragma once
#include "EventLoop.h"
#include <memory>
#include <functional>
#include <netinet/in.h>   // sockaddr_in
#include <sys/socket.h>   // accept4

class Acceptor 
{
public:
    Acceptor(int fd, EventLoop* loop);
    ~Acceptor();
    void listen();
    void stop();
    void setCallBack(std::function<void(int)> cb);
private:
    int fd_;
    EventLoop* loop_;
    std::unique_ptr<Channel> channel_;
    std::function<void(int)> newConnectionCallback_;   // 新连接回调

    // 预留 fd：accept 遇到 EMFILE/ENFILE 时先关它腾出名额，
    // 消费掉一个连接后立刻补回来。不加这个，LT 下 listen fd 一直可读，
    // accept 一直失败，会变成 busy loop。
    int reserve_fd_ = -1;

    // ── EMFILE 退避 ──
    // 预留 fd 只解决"不泄漏 + 不 100% 空转"，但只要 accept 队列还有连接，
    // LT 仍会立刻重触发。策略是"批量丢弃 + 非阻塞退避"：
    //   一轮最多丢 kMaxDropPerRound 个（快速消化积压）；
    //   还打不住才摘掉 listen 的 EPOLLIN，用一次性 timerfd 退避 kBackoffMs，
    //   到点再恢复读。
    // 关键：timerfd 必须在构造时创建——fd 耗尽时连 timerfd_create 都会失败。
    // 平时不上弦（it_value=0），占用 1 个 fd、0 CPU。
    static constexpr int kMaxDropPerRound = 64;
    static constexpr int kBackoffMs = 100;

    int backoff_fd_ = -1;
    std::unique_ptr<Channel> backoff_channel_;

    int DropBurstedConnections();   // 返回本轮丢弃数量
    void ArmBackoff();
    void HandleBackoff();

    void handleAccept();
};
