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

    // 全局令牌桶（网关进程级），0 = 关闭。
    // 每连接桶挡不住"多开连接"：1000 条连接 × 100/s 就是 10 万/s。
    // 全局桶封总量；代价是不公平（一个客户端能吃光全局额度）。
    // 真公平要按身份（API key）限流，而协议里还没有身份字段。
    int global_rate_limit_per_sec = 0;
    int global_rate_burst = 0;   // 0 表示与 global_rate_limit_per_sec 相同

    // 客户端准入限流只在网关做。

    // Backend 注册到 Redis 时使用的地址。单机默认 127.0.0.1
    // 跨机器部署时填本机对外的 IP
    std::string bind_host = "127.0.0.1";

    // Redis 地址 Gateway 和 Backend 共用
    std::string redis_host = "127.0.0.1";
    uint16_t redis_port = 6379;

    // 任务持久化（MySQL）。mysql_host 为空表示关闭。
    // 默认关闭：DB 是旁路能力，不该成为启动的前提条件。
    std::string mysql_host;
    uint16_t mysql_port = 3306;
    std::string mysql_user = "agent";
    std::string mysql_password;
    std::string mysql_db = "agent_runtime";

    // 查询令牌的 HMAC 密钥。为空 = 不启用鉴权（向后兼容）。
    // 启用后：提交返回 token，查询必须带 token，防止"知道 request_id 就能查别人结果"。
    std::string query_secret;

    // 提交前去重：同一 request_id 只受理一次（一次 INSERT 占位，业务线程完成）
    bool db_dedup = false;

    // 停服模式：graceful（drain + deadline，默认）| force（全丢）
    // shutdown_deadline_ms 是 graceful 模式下"等任务自然完成"的最长时间。
    std::string shutdown_mode = "graceful";
    int shutdown_deadline_ms = 10000;

    // 获取单例
    static Config& instance();

    // 解析命令行参数，支持 --port, --io, --work, --help
    void parseArgs(int argc, char* argv[]);
};