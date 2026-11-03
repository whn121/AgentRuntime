#pragma once

#include <sys/socket.h>
#include <netinet/in.h> 
#include <unistd.h>
#include <string>
#include <fcntl.h> //操作fd
#include <arpa/inet.h> // inet_pton 把字符串变成网络二进制ip


enum class IP { ipv4, ipv6 };
enum class Proto { tcp, udp };

// fd 的 RAII 封装：构造置 -1，析构自动 close，避免 fd 泄漏。
// 目前只用到 TCP；UDP 分支保留在 m_init 里但未被调用。
class Socket 
{
public:
    Socket();
    ~Socket();
    Socket(const Socket&) = delete;
    Socket& operator=(const Socket&) = delete;

    bool m_init(IP ip, Proto proto);                    // 创建socket（非阻塞 + CLOEXEC）
    bool m_bind(uint16_t port = 8080, uint32_t addr = INADDR_ANY); // 绑定
    bool m_listen(int backlog = 128);                   // 监听

    // 注意：accept 不在这个类里做。Acceptor 直接用 accept4，
    // 以便在创建 fd 的同时原子地设置 NONBLOCK / CLOEXEC。
    void setReuseAddr(bool on);                         // 设置地址重用（必须在 bind 前调用）
    int getFd() const { return fd_; };                  // 获取文件描述符
    void close(); // 主动关闭 socket；重复调用安全

    // 主动连接：网关需要向 Backend 建立出站连接
    bool m_connect(const std::string& host, uint16_t port);   // 主动连接

    // 交出 fd 所有权：release 之后本对象不再负责 close，避免与接手方重复关闭
    int release(); 

private:
    int fd_ = -1;
    std::string ip_;       // "ipv4" 或 "ipv6"
    sockaddr_in addr;      // 地址结构

    // 服务主动连接
    bool parseAddr(const std::string& host, uint16_t port);   // 解析地址

};
