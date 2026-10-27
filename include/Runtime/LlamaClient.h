#pragma once

#include "CancellationToken.h"
#include <string>


namespace runtime
{
    class LlamaClient
    {
    public:
        explicit LlamaClient(std::string endpoint = "http://127.0.0.1:8081/v1/chat/completions", long timeoutMs = 120000);

        // token 为空时行为与原来一致。非空时通过 CURLOPT_XFERINFOFUNCTION
        // 周期性检查, 取消后 curl_easy_perform 返回 CURLE_ABORTED_BY_CALLBACK。
        std::string complete(const std::string& prompt, const CancellationToken& token) const;
        
    private:
        std::string endpoint_;
        long timeoutMs_;
    };
}

