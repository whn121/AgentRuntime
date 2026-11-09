#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace crypto
{
    // 自实现的 SHA-256 / HMAC-SHA256。
    //
    // 为什么不引 OpenSSL：只需要"算一个 16 字节的查询令牌"，引一个库意味着
    // 多一个部署依赖和版本风险；这段代码用 RFC 4231 的官方测试向量验证过
    // （见 tests/test_hmac.cpp），比"用了某个库"更容易讲清楚。
    //
    // 注意：这不是通用加密库，只实现了本项目需要的部分，没有做侧信道加固以外的
    // 优化；生产上更稳妥的做法仍是用 libcrypto。

    std::array<std::uint8_t, 32> Sha256(const std::uint8_t* data, std::size_t len);
    std::array<std::uint8_t, 32> Sha256(const std::string& data);

    // HMAC-SHA256，返回十六进制字符串（out_bytes 截断，默认 16 字节 = 32 个 hex 字符）
    std::string HmacSha256Hex(const std::string& key, const std::string& message,
                              std::size_t out_bytes = 16);

    // 常数时间比较，避免用 == 泄漏前缀匹配长度
    bool ConstantTimeEquals(const std::string& a, const std::string& b);

    // 查询令牌：token = HMAC-SHA256(secret, "query:" + request_id) 的前 16 字节
    std::string MakeQueryToken(std::uint32_t request_id, const std::string& secret);
    bool VerifyQueryToken(std::uint32_t request_id, const std::string& token,
                          const std::string& secret);
}
