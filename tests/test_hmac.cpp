// HMAC-SHA256 / 查询令牌的单元测试（纯逻辑，不依赖网络与数据库）。
//
// 为什么值得单独测：这是手写的密码学原语，写错了不会崩，只会"验签永远失败"或
// "永远通过"——两种都很难在生产里发现。所以用 RFC 4231 的官方测试向量钉死。
//
// 覆盖：
//   1. SHA-256 标准向量（空串、abc）
//   2. HMAC-SHA256 RFC 4231 用例 1/2
//   3. 查询令牌：同 id 同 secret 稳定、换 id / 换 secret 必变
//   4. 常数时间比较：长度不同直接 false、内容不同 false
//   5. 截断长度：默认 32 个 hex 字符（16 字节）

#include "Crypto/HmacSha256.h"

#include <cstdio>
#include <cstring>
#include <string>

static int g_failures = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::printf("CHECK failed: %s at line %d\n", #cond, __LINE__); \
            ++g_failures;                                                  \
        }                                                                  \
    } while (0)

static std::string ToHex(const std::array<std::uint8_t, 32>& digest)
{
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(64);
    for (std::uint8_t b : digest)
    {
        out.push_back(kHex[b >> 4]);
        out.push_back(kHex[b & 0x0F]);
    }
    return out;
}

int main()
{
    using namespace crypto;

    // ── 1. SHA-256 标准向量 ──
    CHECK(ToHex(Sha256("")) ==
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    CHECK(ToHex(Sha256("abc")) ==
          "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    CHECK(ToHex(Sha256("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")) ==
          "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");

    // ── 2. RFC 4231 用例 1：key = 0x0b × 20，data = "Hi There" ──
    {
        std::string key(20, '\x0b');
        CHECK(HmacSha256Hex(key, "Hi There", 32) ==
              "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");
    }

    // ── 3. RFC 4231 用例 2：key = "Jefe" ──
    CHECK(HmacSha256Hex("Jefe", "what do ya want for nothing?", 32) ==
          "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");

    // ── 4. 查询令牌的性质 ──
    const std::string secret = "test-secret";
    const std::string t1 = MakeQueryToken(4242, secret);
    const std::string t2 = MakeQueryToken(4242, secret);
    CHECK(t1 == t2);                              // 同一输入稳定
    CHECK(t1.size() == 32);                       // 截断到 16 字节
    CHECK(t1 != MakeQueryToken(4243, secret));    // 换 id 必变
    CHECK(t1 != MakeQueryToken(4242, "other"));   // 换 secret 必变

    CHECK(VerifyQueryToken(4242, t1, secret));
    CHECK(!VerifyQueryToken(4243, t1, secret));
    CHECK(!VerifyQueryToken(4242, t1, "other"));
    CHECK(!VerifyQueryToken(4242, "", secret));      // 空 token 拒绝
    CHECK(!VerifyQueryToken(4242, t1, ""));          // 空 secret 拒绝

    // 篡改一个字符必须失败
    {
        std::string tampered = t1;
        tampered[0] = (tampered[0] == 'a') ? 'b' : 'a';
        CHECK(!VerifyQueryToken(4242, tampered, secret));
    }

    // ── 5. 常数时间比较的边界 ──
    CHECK(ConstantTimeEquals("abc", "abc"));
    CHECK(!ConstantTimeEquals("abc", "abd"));
    CHECK(!ConstantTimeEquals("abc", "ab"));      // 长度不同直接 false
    CHECK(ConstantTimeEquals("", ""));

    if (g_failures == 0)
    {
        std::printf("All hmac tests passed.\n");
        return 0;
    }
    std::printf("%d checks failed.\n", g_failures);
    return 1;
}
