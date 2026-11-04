#include "Common/TokenBucket.h"

#include <chrono>
#include <cstdio>

// 令牌桶单元测试：全部用注入的时间，不 sleep。
static int g_failures = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        if (!(cond)) {                                                     \
            std::printf("CHECK failed: %s at line %d\n", #cond, __LINE__); \
            ++g_failures;                                                  \
        }                                                                  \
    } while (0)

// 容量 3、每秒补 1：验证"突发上限"和"按速率补充"
static void TestBurstThenRefill()
{
    TokenBucket bucket(3.0, 1.0);

    // 一开始是满的：连续 3 个通过，第 4 个被拒
    CHECK(bucket.tryAcquire(0.0));
    CHECK(bucket.tryAcquire(0.0));
    CHECK(bucket.tryAcquire(0.0));
    CHECK(!bucket.tryAcquire(0.0));

    // 0.5 秒只补 0.5 个，还不够 1 个
    CHECK(!bucket.tryAcquire(0.5));

    // 到 1.0 秒累计补 1 个，刚好够一次
    CHECK(bucket.tryAcquire(1.0));
    CHECK(!bucket.tryAcquire(1.0));

    // 空闲很久后补满，但被容量截断在 3
    CHECK(bucket.tryAcquire(100.0));
    CHECK(bucket.tryAcquire(100.0));
    CHECK(bucket.tryAcquire(100.0));
    CHECK(!bucket.tryAcquire(100.0));

    CHECK(bucket.capacity() == 3.0);
    CHECK(bucket.rate() == 1.0);
}

// 时间回退（例如时钟被调整）不能让令牌凭空增加
static void TestTimeGoesBackwards()
{
    TokenBucket bucket(2.0, 10.0);

    CHECK(bucket.tryAcquire(10.0));
    CHECK(bucket.tryAcquire(10.0));
    CHECK(!bucket.tryAcquire(10.0));

    // 时间回退：不补充，仍然拒绝
    CHECK(!bucket.tryAcquire(5.0));
}

// 一次消耗多个令牌
static void TestCostGreaterThanOne()
{
    TokenBucket bucket(5.0, 1.0);

    CHECK(bucket.tryAcquire(0.0, 2.0));    // 5 -> 3
    CHECK(bucket.tryAcquire(0.0, 3.0));    // 3 -> 0
    CHECK(!bucket.tryAcquire(0.0, 1.0));   // 不够
    CHECK(bucket.tokens() == 0.0);
}

static void TestReset()
{
    TokenBucket bucket(2.0, 1.0);

    CHECK(bucket.tryAcquire(0.0));
    CHECK(bucket.tryAcquire(0.0));
    CHECK(!bucket.tryAcquire(0.0));

    bucket.reset();
    CHECK(bucket.tokens() == 2.0);
    CHECK(bucket.tryAcquire(0.0));
}

int main()
{
    TestBurstThenRefill();
    TestTimeGoesBackwards();
    TestCostGreaterThanOne();
    TestReset();

    if (g_failures == 0)
    {
        std::printf("All TokenBucket tests passed.\n");
        return 0;
    }
    std::printf("%d checks failed.\n", g_failures);
    return 1;
}
