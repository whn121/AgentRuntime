#pragma once

#include <string>
#include <cstdint>

// 全局配置单例，支持从命令行读取参数，避免硬编码端口和线程数
struct Config 
{
    uint16_t port = 8080;        // 监听端口，默认 8080
    int io_threads = 8;          // I/O 线程池大小（子 Reactor 数量）
    int work_threads = 4;        // 业务线程池大小

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