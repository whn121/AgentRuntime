#include "Crypto/HmacSha256.h"

#include <cstring>

namespace crypto
{
    namespace
    {
        constexpr std::uint32_t K[64] = {
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
            0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
            0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
            0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
            0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
            0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
            0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2};

        inline std::uint32_t Rotr(std::uint32_t x, int n) { return (x >> n) | (x << (32 - n)); }

        void Compress(std::uint32_t state[8], const std::uint8_t block[64])
        {
            std::uint32_t w[64];
            for (int i = 0; i < 16; ++i)
            {
                w[i] = (static_cast<std::uint32_t>(block[i * 4]) << 24) |
                       (static_cast<std::uint32_t>(block[i * 4 + 1]) << 16) |
                       (static_cast<std::uint32_t>(block[i * 4 + 2]) << 8) |
                       (static_cast<std::uint32_t>(block[i * 4 + 3]));
            }
            for (int i = 16; i < 64; ++i)
            {
                const std::uint32_t s0 = Rotr(w[i - 15], 7) ^ Rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
                const std::uint32_t s1 = Rotr(w[i - 2], 17) ^ Rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
                w[i] = w[i - 16] + s0 + w[i - 7] + s1;
            }

            std::uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
            std::uint32_t e = state[4], f = state[5], g = state[6], h = state[7];

            for (int i = 0; i < 64; ++i)
            {
                const std::uint32_t s1 = Rotr(e, 6) ^ Rotr(e, 11) ^ Rotr(e, 25);
                const std::uint32_t ch = (e & f) ^ (~e & g);
                const std::uint32_t t1 = h + s1 + ch + K[i] + w[i];
                const std::uint32_t s0 = Rotr(a, 2) ^ Rotr(a, 13) ^ Rotr(a, 22);
                const std::uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
                const std::uint32_t t2 = s0 + maj;

                h = g; g = f; f = e; e = d + t1;
                d = c; c = b; b = a; a = t1 + t2;
            }

            state[0] += a; state[1] += b; state[2] += c; state[3] += d;
            state[4] += e; state[5] += f; state[6] += g; state[7] += h;
        }
    }

    std::array<std::uint8_t, 32> Sha256(const std::uint8_t* data, std::size_t len)
    {
        std::uint32_t state[8] = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                  0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};

        const std::size_t full_blocks = len / 64;
        for (std::size_t i = 0; i < full_blocks; ++i)
        {
            Compress(state, data + i * 64);
        }

        // 尾部：剩余字节 + 0x80 + 零填充 + 64 位长度
        std::uint8_t tail[128] = {0};
        const std::size_t rem = len % 64;
        if (rem > 0) std::memcpy(tail, data + full_blocks * 64, rem);
        tail[rem] = 0x80;

        const std::size_t tail_len = (rem + 9 <= 64) ? 64 : 128;
        const std::uint64_t bit_len = static_cast<std::uint64_t>(len) * 8;
        for (int i = 0; i < 8; ++i)
        {
            tail[tail_len - 1 - i] = static_cast<std::uint8_t>((bit_len >> (8 * i)) & 0xFF);
        }

        for (std::size_t off = 0; off < tail_len; off += 64)
        {
            Compress(state, tail + off);
        }

        std::array<std::uint8_t, 32> out{};
        for (int i = 0; i < 8; ++i)
        {
            out[i * 4] = static_cast<std::uint8_t>((state[i] >> 24) & 0xFF);
            out[i * 4 + 1] = static_cast<std::uint8_t>((state[i] >> 16) & 0xFF);
            out[i * 4 + 2] = static_cast<std::uint8_t>((state[i] >> 8) & 0xFF);
            out[i * 4 + 3] = static_cast<std::uint8_t>(state[i] & 0xFF);
        }
        return out;
    }

    std::array<std::uint8_t, 32> Sha256(const std::string& data)
    {
        return Sha256(reinterpret_cast<const std::uint8_t*>(data.data()), data.size());
    }

    std::string HmacSha256Hex(const std::string& key, const std::string& message,
                              std::size_t out_bytes)
    {
        std::uint8_t block_key[64] = {0};
        if (key.size() > 64)
        {
            const auto hashed = Sha256(key);
            std::memcpy(block_key, hashed.data(), hashed.size());
        }
        else
        {
            std::memcpy(block_key, key.data(), key.size());
        }

        std::uint8_t ipad[64], opad[64];
        for (int i = 0; i < 64; ++i)
        {
            ipad[i] = static_cast<std::uint8_t>(block_key[i] ^ 0x36);
            opad[i] = static_cast<std::uint8_t>(block_key[i] ^ 0x5c);
        }

        std::string inner(reinterpret_cast<const char*>(ipad), 64);
        inner += message;
        const auto inner_hash = Sha256(inner);

        std::string outer(reinterpret_cast<const char*>(opad), 64);
        outer.append(reinterpret_cast<const char*>(inner_hash.data()), inner_hash.size());
        const auto digest = Sha256(outer);

        static const char* kHex = "0123456789abcdef";
        const std::size_t bytes = (out_bytes == 0 || out_bytes > 32) ? 32 : out_bytes;
        std::string hex;
        hex.reserve(bytes * 2);
        for (std::size_t i = 0; i < bytes; ++i)
        {
            hex.push_back(kHex[digest[i] >> 4]);
            hex.push_back(kHex[digest[i] & 0x0F]);
        }
        return hex;
    }

    bool ConstantTimeEquals(const std::string& a, const std::string& b)
    {
        if (a.size() != b.size()) return false;
        unsigned char diff = 0;
        for (std::size_t i = 0; i < a.size(); ++i)
        {
            diff |= static_cast<unsigned char>(a[i] ^ b[i]);
        }
        return diff == 0;
    }

    std::string MakeQueryToken(std::uint32_t request_id, const std::string& secret)
    {
        // 前缀 "query:" 把令牌的用途固定住，避免和别的 HMAC 用途混用
        return HmacSha256Hex(secret, "query:" + std::to_string(request_id), 16);
    }

    bool VerifyQueryToken(std::uint32_t request_id, const std::string& token,
                          const std::string& secret)
    {
        if (secret.empty() || token.empty()) return false;
        return ConstantTimeEquals(token, MakeQueryToken(request_id, secret));
    }
}
