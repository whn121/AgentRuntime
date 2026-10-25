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
        {"port",     required_argument, 0, 'p'},
        {"io",       required_argument, 0, 'i'},
        {"work",     required_argument, 0, 'w'},
        {"help",     no_argument,       0, 'h'},
        {0, 0, 0, 0}                     // 结束标记
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
    while ((opt = getopt_long(argc, argv, "p:i:w:h", long_options, nullptr)) != -1) 
    {
        switch (opt) 
        {
            case 'p': port = static_cast<uint16_t>(number(optarg, 65535)); break;
            case 'i': io_threads = number(optarg, 1024); break;
            case 'w': work_threads = number(optarg, 1024); break;
            case 'h':
                std::cout << "Usage: EchoNetGateway [--port=8080] [--io=8] [--work=4]\n";
                exit(0);
            default:
                throw std::invalid_argument("unknown option or missing argument");
        }
    }
    if (optind != argc) throw std::invalid_argument("unexpected positional argument");
}