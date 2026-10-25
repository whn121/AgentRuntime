#include "Runtime/LlamaClient.h"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

namespace
{
    using json = nlohmann::json;

    void initializeCurl()
    {
        static std::once_flag flag;
        static CURLcode result = CURLE_OK;

        std::call_once(flag, []
        {
            result = curl_global_init (CURL_GLOBAL_DEFAULT);
        });

        if (result != CURLE_OK)
        {
            throw std::runtime_error("CURL_GLOBAL_INIT_FAILED");
        }
    }

    size_t writeResponse (char* data, size_t size, size_t count, void* userData)
    {
        if (size != 0 && count > static_cast<size_t>(-1) / size)
        {
            return 0;
        }

        const size_t bytes = size * count;
        auto* response = static_cast<std::string*> (userData);

        // 防止异常响应体无限占用内存
        constexpr size_t maxResponseBytes = 8 * 1024 * 1024;

        if (bytes > maxResponseBytes - response->size())
        {
            return 0;
        }

        response->append (data, bytes);
        return bytes;
    }
}

namespace runtime
{
    LlamaClient::LlamaClient(std::string endpoint, long timeoutMS) : endpoint_ (endpoint), timeoutMs_ (timeoutMS)
    {
        if (endpoint_.empty() || timeoutMs_ <= 0)
        {
            throw std::invalid_argument("Invalid LlamaClient configuration");
        }
    }

    std::string LlamaClient::complete (const std::string& prompt) const
    {
        initializeCurl();

        CURL* raw = curl_easy_init();
        if (!raw)
        {
            throw std::runtime_error("CURL_EASY_INIT_FAILED");
        }

        struct CurlHandle
        {
            CURL* handle;
            ~CurlHandle () { curl_easy_cleanup (handle); }
        } curlHandle{raw};
        
        json request = 
        {
            {"messages", json::array(
                {
                    {
                        {"role", "system"},
                        {"content", "你是一个精准,简洁的中文AI助手"}
                    },
                    {
                        {"role", "user"},
                        {"content", prompt}
                    }
                })
            },
            {"temperature", 0.7},
            {"max_tokens", 512},
            {"stream", false}
        };

        const std::string requestBody = request.dump();
        std::string responseBody;

        struct curl_slist* rawHeaders = nullptr;
        rawHeaders = curl_slist_append (rawHeaders, "Content-Type: application/json");

        if (!rawHeaders)
        {
            throw std::runtime_error ("CURL_HEADER_ALLOCATION_FAILED");
        }

        struct  HeaderList
        {
            curl_slist* headers;
            ~HeaderList() { curl_slist_free_all (headers); }
        } headers {rawHeaders};
        
        curl_easy_setopt(raw, CURLOPT_URL, endpoint_.c_str());
        curl_easy_setopt(raw, CURLOPT_HTTPHEADER, rawHeaders);
        curl_easy_setopt(raw, CURLOPT_POST, 1L);
        curl_easy_setopt(raw, CURLOPT_POSTFIELDS, requestBody.data());
        curl_easy_setopt(raw, CURLOPT_POSTFIELDSIZE, static_cast<long>(requestBody.size()));
        curl_easy_setopt(raw, CURLOPT_WRITEFUNCTION, writeResponse);
        curl_easy_setopt(raw, CURLOPT_WRITEDATA, &responseBody);
        curl_easy_setopt(raw, CURLOPT_CONNECTTIMEOUT_MS, 3000L);
        curl_easy_setopt(raw, CURLOPT_TIMEOUT_MS, timeoutMs_);
        curl_easy_setopt(raw, CURLOPT_NOSIGNAL, 1L);

        const CURLcode result = curl_easy_perform (raw);

        if (result != CURLE_OK)
        {
            throw std::runtime_error(std::string("LLAMA_HTTP_REQUEST_FAILED: ") + curl_easy_strerror(result));
        }

        long statusCode = 0;
        curl_easy_getinfo (raw, CURLINFO_RESPONSE_CODE, &statusCode);

        if (statusCode < 200 || statusCode >= 300)
        {
            throw std::runtime_error("LLAMA_HTTP_STATUS_" + std::to_string(statusCode) + ": " + responseBody.substr(0, 1024));
        }

        try
        {
            const json response = json::parse (responseBody);
            const auto& content = response.at ("choices").at(0).at("message").at("content");
            
            if (!content.is_string() || content.get<std::string>().empty())
            {
                throw std::runtime_error("LLAMA_EMPTY_RESPONSE");
            }

            return content.get<std::string>();
        }
        catch(const json::exception& error)
        {
            throw std::runtime_error(std::string("LLAMA_INVALID_RESPONSE: ") + error.what());
        }
        
    }
}