// tests/test_protocol.cpp —— 协议解析的纯逻辑单测（不开 socket、不起线程）
//
// 为什么单独测协议层：
//   半包/粘包是整个网络层最容易出边界 bug 的地方，而它不需要网络就能测。
//   核心断言是"NEED_MORE 时一个字节都不消费"——这条性质靠 code review 看不出来，
//   只能靠断言守住。

#include "Protocol/MyProtocol.h"
#include "Net/Buffer.h"

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace {

int g_failures = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::printf("CHECK failed: %s (line %d)\n", #cond, __LINE__);  \
            ++g_failures;                                                  \
        }                                                                  \
    } while (0)

// 手工拼一个帧头（4B 长度 + 2B 类型 + 4B id），用于构造非法长度
std::string MakeHeader(uint32_t body_len, uint16_t type = 0x04, uint32_t id = 1)
{
    std::string h;
    h.resize(10);
    h[0] = static_cast<char>((body_len >> 24) & 0xFF);
    h[1] = static_cast<char>((body_len >> 16) & 0xFF);
    h[2] = static_cast<char>((body_len >> 8) & 0xFF);
    h[3] = static_cast<char>(body_len & 0xFF);
    h[4] = static_cast<char>((type >> 8) & 0xFF);
    h[5] = static_cast<char>(type & 0xFF);
    h[6] = static_cast<char>((id >> 24) & 0xFF);
    h[7] = static_cast<char>((id >> 16) & 0xFF);
    h[8] = static_cast<char>((id >> 8) & 0xFF);
    h[9] = static_cast<char>(id & 0xFF);
    return h;
}

void Append(Buffer& buf, const std::string& s)
{
    buf.bufferAppend(s.data(), s.size());
}

// 1. 空缓冲：连长度字段都不够 → NEED_MORE，且不消费
void TestEmptyBuffer()
{
    MyProtocol proto;
    Buffer buf;

    CHECK(proto.parse(buf) == ParseResult::NEED_MORE);
    CHECK(buf.getreadable() == 0);
}

// 2. 只有 3 字节：不够长度字段 → NEED_MORE，且不消费
void TestPartialLengthField()
{
    MyProtocol proto;
    Buffer buf;

    const std::string frame = MyProtocol::encodeMessage(MyType::RUNTIME_TASK_REQ, 7, "hi");
    Append(buf, frame.substr(0, 3));

    CHECK(proto.parse(buf) == ParseResult::NEED_MORE);
    CHECK(buf.getreadable() == 3);      // ★ 一个字节都没被吃掉
}

// 3. 完整帧 + 半帧：先 OK 一次，再 NEED_MORE，剩下半帧留在缓冲里
void TestFullFrameThenHalf()
{
    MyProtocol proto;
    Buffer buf;

    const std::string a = MyProtocol::encodeMessage(MyType::RUNTIME_TASK_REQ, 11, "first");
    const std::string b = MyProtocol::encodeMessage(MyType::RUNTIME_TASK_REQ, 12, "second");

    Append(buf, a);
    Append(buf, b.substr(0, b.size() / 2));

    CHECK(proto.parse(buf) == ParseResult::OK);
    proto.reset();

    const size_t remain = buf.getreadable();
    CHECK(remain == b.size() / 2);      // 正好剩下半帧

    // ★ 核心断言：NEED_MORE 不能消费任何字节
    CHECK(proto.parse(buf) == ParseResult::NEED_MORE);
    CHECK(buf.getreadable() == remain);
}

// 4. 三个完整帧：连续三次 OK
void TestThreeFrames()
{
    MyProtocol proto;
    Buffer buf;

    Append(buf, MyProtocol::encodeMessage(MyType::RUNTIME_TASK_REQ, 1, "aaa"));
    Append(buf, MyProtocol::encodeMessage(MyType::HEARTBEAT_REQ, 2, "bbb"));
    Append(buf, MyProtocol::encodeMessage(MyType::HEARTBEAT_REQ, 3, "ccc"));

    for (int i = 0; i < 3; ++i)
    {
        CHECK(proto.parse(buf) == ParseResult::OK);
        proto.reset();
    }
    CHECK(proto.parse(buf) == ParseResult::NEED_MORE);
    CHECK(buf.getreadable() == 0);
}

// 5. body_len 超过上限（16MB+1）→ ERROR，且不需要 payload 到齐
void TestBodyTooLarge()
{
    MyProtocol proto;
    Buffer buf;

    Append(buf, MakeHeader(MyProtocol::MAX_BODY_SIZE + 1));
    CHECK(proto.parse(buf) == ParseResult::ERROR);
}

// 6. body_len 小于 type(2)+id(4) → ERROR
void TestBodyTooSmall()
{
    MyProtocol proto;
    Buffer buf;

    Append(buf, MakeHeader(5));
    CHECK(proto.parse(buf) == ParseResult::ERROR);
}

// 7. 一次塞 4 个帧，逐个消费，最后 NEED_MORE（粘包场景）
void TestManyFramesInOneBuffer()
{
    MyProtocol proto;
    Buffer buf;

    int total = 0;
    for (uint32_t id = 100; id < 104; ++id)
    {
        const std::string f = MyProtocol::encodeMessage(MyType::RUNTIME_TASK_REQ, id, "payload");
        total += static_cast<int>(f.size());
        Append(buf, f);
    }

    int parsed = 0;
    while (proto.parse(buf) == ParseResult::OK)
    {
        proto.reset();
        ++parsed;
    }
    CHECK(parsed == 4);
    CHECK(buf.getreadable() == 0);
}

} // namespace

int main()
{
    TestEmptyBuffer();
    TestPartialLengthField();
    TestFullFrameThenHalf();
    TestThreeFrames();
    TestBodyTooLarge();
    TestBodyTooSmall();
    TestManyFramesInOneBuffer();

    if (g_failures == 0)
    {
        std::printf("All MyProtocol tests passed.\n");
        return 0;
    }
    std::printf("%d checks failed.\n", g_failures);
    return 1;
}
