#pragma once

#include <string>
#include <cstdint>

// 全局配置单例，支持从命令行读取参数，避免硬编码端口和线程数
struct Config 
{
    uint16_t port = 8080;        // 监听端口，默认 8080
    int io_threads = 8;          // I/O 线程池大小（子 Reactor 数量）
    int work_threads = 4;        // 业务线程池大小

    // RuntimeEngine 的执行线程数。这两个线程会阻塞在 HTTP 调用上，
    // 因此它是"并发在途模型调用"的上限，也是压测中最常见的瓶颈点。
    int engine_workers = 2;

    // 每连接令牌桶：容量（突发）与补充速率。
    // rate_limit_per_sec == 0 表示关闭限流（压测测容量时用）。
    //
    // 注意：这个桶挂在"连接"上。经过网关后，后端看到的是一条复用连接，
    // 承载的却是所有客户端的流量——限流维度在网关架构下会失真。
    int rate_limit_burst = 200;
    int rate_limit_per_sec = 100;

    // 后端自身的每连接限流，**默认 0（关闭）**。
    // 后端看到的是网关的复用连接，按连接限流会把整片流量误判成单客户端超速，
    // 所以客户端准入限流在网关做。只有在"后端被直连部署"的特殊场景，
    // 才用 --backend-rate-limit=N 显式打开（N 同时作为突发容量和补充速率）。
    int backend_rate_limit_per_sec = 0;

    // Backend 注册到 Redis 时使用的地址。单机默认 127.0.0.1
    // 跨机器部署时填本机对外的 IP
    std::string bind_host = "127.0.0.1";

    // Redis 地址 Gateway 和 Backend 共用
    std::string redis_host = "127.0.0.1";
    uint16_t redis_port = 6379;

    // 获取单例
    static Config& instance();

    // 解析命令行参数，支持 --port, --io, --work, --help
    void parseArgs(int argc, char* argv[]);
};