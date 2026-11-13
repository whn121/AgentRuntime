#include "Common/Config.h"
#include <cstdlib>
#include <charconv>
#include <stdexcept>
#include <string_view>
#include <getopt.h>     // GNU 长选项解析
#include <iostream>

Config& Config::instance() 
{
    static Config cfg;   // C++11 保证线程安全的局部静态变量
    return cfg;
}

void Config::parseArgs(int argc, char* argv[]) 
{
    // 定义长选项
    static struct option long_options[] =
    {
        {"port",       required_argument, 0, 'p'},
        {"io",         required_argument, 0, 'i'},
        {"work",       required_argument, 0, 'w'},
        {"engine-workers", required_argument, 0, 'E'},
        {"rate-burst",   required_argument, 0, 'B'},
        {"rate-limit",   required_argument, 0, 'L'},
        {"global-rate-limit", required_argument, 0, 'G'},
        {"global-rate-burst", required_argument, 0, 'T'},
        {"shutdown-mode", required_argument, 0, 'S'},
        {"shutdown-deadline-ms", required_argument, 0, 'D'},
        {"bind-host",  required_argument, 0, 'b'},
        {"redis-host", required_argument, 0, 'r'},
        {"redis-port", required_argument, 0, 'R'},
        {"mysql-host", required_argument, 0, 'H'},
        {"mysql-port", required_argument, 0, 'P'},
        {"mysql-user", required_argument, 0, 'U'},
        {"mysql-password", required_argument, 0, 'W'},
        {"mysql-db", required_argument, 0, 'N'},
        {"query-secret", required_argument, 0, 'K'},
        {"db-dedup", no_argument, 0, 'J'},
        {"help",       no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    

    auto number = [](const char* text, int maximum) 
    {
        std::string_view value(text);
        int result = 0;
        auto parsed = std::from_chars(value.data(), value.data() + value.size(), result);
        if (value.empty() || parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size()
            || result < 1 || result > maximum)
            throw std::invalid_argument("invalid numeric argument: " + std::string(value));
        return result;
    };
    opterr = 0;
    optind = 1;
    int opt;
    // 循环解析参数，直到返回 -1
    while ((opt = getopt_long(argc, argv, "p:i:w:E:B:L:G:T:S:D:b:r:R:H:P:U:W:N:K:Jh", long_options, nullptr)) != -1)
    {
        switch (opt) 
        {
            case 'p': port = static_cast<uint16_t>(number(optarg, 65535)); break;
            case 'i': io_threads = number(optarg, 1024); break;
            case 'w': work_threads = number(optarg, 1024); break;
            case 'E': engine_workers = number(optarg, 1024); break;
            case 'B': rate_limit_burst = number(optarg, 1000000); break;
            case 'L': rate_limit_per_sec = (std::atoi(optarg) == 0) ? 0 : number(optarg, 1000000); break;
            case 'G': global_rate_limit_per_sec = (std::atoi(optarg) == 0) ? 0 : number(optarg, 1000000); break;
            case 'T': global_rate_burst = number(optarg, 1000000); break;
            case 'S':
                shutdown_mode = optarg;
                if (shutdown_mode != "graceful" && shutdown_mode != "force")
                    throw std::invalid_argument("invalid --shutdown-mode (graceful|force)");
                break;
            case 'D': shutdown_deadline_ms = number(optarg, 3600000); break;
            case 'b': bind_host = optarg; break;
            case 'r': redis_host = optarg; break;
            case 'R': redis_port = static_cast<uint16_t>(number(optarg, 65535)); break;
            case 'H': mysql_host = optarg; break;
            case 'P': mysql_port = static_cast<uint16_t>(number(optarg, 65535)); break;
            case 'U': mysql_user = optarg; break;
            case 'W': mysql_password = optarg; break;
            case 'N': mysql_db = optarg; break;
            case 'K': query_secret = optarg; break;
            case 'J': db_dedup = true; break;
            case 'h':
                std::cout
                    << "Usage: AgentRuntime [options]\n"
                    << "  --port=N          listen port (default 8080)\n"
                    << "  --io=N            IO threads (default 8)\n"
                    << "  --work=N          worker threads (default 4)\n"
                    << "  --engine-workers=N RuntimeEngine threads (default 2)\n"
                    << "  --rate-burst=N    客户端令牌桶容量 (gateway, default 200)\n"
                    << "  --rate-limit=N    客户端限流速率/s (gateway, 0=off, default 100)\n"
                    << "  --global-rate-limit=N 网关全局限流/s (0=off)\n"
                    << "  --global-rate-burst=N 全局桶容量 (0=与速率相同)\n"
                    << "  --shutdown-mode=M 停服模式 graceful|force (default graceful)\n"
                    << "  --shutdown-deadline-ms=N  graceful 模式最长等待 (default 10000)\n"
                    << "  --bind-host=HOST  address registered to Redis (default 127.0.0.1)\n"
                    << "  --redis-host=HOST Redis host (default 127.0.0.1)\n"
                    << "  --redis-port=N    Redis port (default 6379)\n"
                    << "  --mysql-host=H    MySQL host; empty = 任务持久化关闭 (default 关闭)\n"
                    << "  --mysql-port=N    MySQL port (default 3306)\n"
                    << "  --mysql-user=U    MySQL user (default agent)\n"
                    << "  --mysql-password=P MySQL password (default 空)\n"
                    << "  --mysql-db=DB     MySQL database (default agent_runtime)\n"
                    << "  --query-secret=S  查询令牌 HMAC 密钥；空=不鉴权 (default 空)\n"
                    << "  --db-dedup        提交前去重：同一 request_id 只受理一次\n";
                exit(0);
            default:
                throw std::invalid_argument("unknown option or missing argument");
        }
    }
    if (optind != argc) throw std::invalid_argument("unexpected positional argument");
}