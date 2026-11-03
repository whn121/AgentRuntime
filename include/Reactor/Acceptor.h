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

    void handleAccept();
};
