#pragma once

#include <string>

namespace runtime
{
    class LlamaClient
    {
    public:
        explicit LlamaClient(std::string endpoint = "http://127.0.0.1:8081/v1/chat/completions", long timeoutMs = 120000);

        std::string complete(const std::string& prompt) const;
        
    private:
        std::string endpoint_;
        long timeoutMs_;
    };
}

