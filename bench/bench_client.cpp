// bench/bench_client.cpp —— AgentRuntime 压测客户端（无第三方依赖）
//
// 两种模式：
//   closed_loop : 每连接 "发一个请求 → 等响应" 的闭环，测吞吐与延迟分位
//   blast       : 只发不收，用于触发服务端发送侧背压（8MB/2MB/64MB 水位）
//
// 用法：
//   ./bench_client --host 127.0.0.1 --port 8080 --conns 200 --seconds 30 \
//                  --payload 32 --mode closed_loop [--csv out.csv]
//
// 输出：
//   total / ok / rate_limited / failed / qps
//   lat_ms p50 / p95 / p99 / max

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr uint16_t kTypeTaskReq = 0x04;
constexpr uint16_t kTypeTaskResp = 0x05;
constexpr uint16_t kTypeTaskFailed = 0x06;
constexpr uint16_t kTypeErrorResp = 0x03;

struct Args
{
    std::string host = "127.0.0.1";
    int port = 8080;
    int conns = 50;
    int seconds = 10;
    int payload = 32;
    int warmup_ms = 300;
    std::string mode = "closed_loop";
    std::string csv;
};

struct Stats
{
    std::vector<double> lat_ms;
    long ok = 0;
    long rate_limited = 0;
    long failed = 0;
    long other = 0;
    long bytes_sent = 0;
};

bool send_all(int fd, const char* p, std::size_t n)
{
    std::size_t sent = 0;
    while (sent < n)
    {
        const ssize_t r = ::send(fd, p + sent, n - sent, MSG_NOSIGNAL);
        if (r > 0) { sent += static_cast<std::size_t>(r); continue; }
        if (r < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

bool recv_all(int fd, char* p, std::size_t n)
{
    std::size_t got = 0;
    while (got < n)
    {
        const ssize_t r = ::recv(fd, p + got, n - got, 0);
        if (r > 0) { got += static_cast<std::size_t>(r); continue; }
        if (r < 0 && errno == EINTR) continue;
        return false;
    }
    return true;
}

std::string make_frame(uint16_t type, uint32_t id, int payload_len)
{
    const uint32_t body_len = static_cast<uint32_t>(6 + payload_len);
    std::string frame;
    frame.resize(4 + body_len);

    frame[0] = static_cast<char>((body_len >> 24) & 0xFF);
    frame[1] = static_cast<char>((body_len >> 16) & 0xFF);
    frame[2] = static_cast<char>((body_len >> 8) & 0xFF);
    frame[3] = static_cast<char>(body_len & 0xFF);

    frame[4] = static_cast<char>((type >> 8) & 0xFF);
    frame[5] = static_cast<char>(type & 0xFF);

    frame[6] = static_cast<char>((id >> 24) & 0xFF);
    frame[7] = static_cast<char>((id >> 16) & 0xFF);
    frame[8] = static_cast<char>((id >> 8) & 0xFF);
    frame[9] = static_cast<char>(id & 0xFF);

    for (int i = 0; i < payload_len; ++i)
    {
        frame[10 + i] = static_cast<char>('a' + (i % 26));
    }
    return frame;
}

double now_ms()
{
    using namespace std::chrono;
    static const steady_clock::time_point t0 = steady_clock::now();
    return duration<double, std::milli>(steady_clock::now() - t0).count();
}

int connect_to(const std::string& host, int port)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) { ::close(fd); return -1; }

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) { ::close(fd); return -1; }

    const int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

// 闭环：发一个、等一个
void run_closed_loop(const Args& args, int conn_index, Stats& st)
{
    const int fd = connect_to(args.host, args.port);
    if (fd < 0) { ++st.failed; return; }

    const double deadline = now_ms() + static_cast<double>(args.seconds) * 1000.0;
    uint32_t id = static_cast<uint32_t>(conn_index) * 1000000u + 1u;

    std::string frame;
    while (now_ms() < deadline)
    {
        frame = make_frame(kTypeTaskReq, id, args.payload);
        const double t0 = now_ms();
        if (!send_all(fd, frame.data(), frame.size())) { ++st.failed; break; }
        st.bytes_sent += static_cast<long>(frame.size());

        char hdr[4];
        if (!recv_all(fd, hdr, 4)) { ++st.failed; break; }
        const uint32_t body_len =
            (static_cast<uint32_t>(static_cast<unsigned char>(hdr[0])) << 24) |
            (static_cast<uint32_t>(static_cast<unsigned char>(hdr[1])) << 16) |
            (static_cast<uint32_t>(static_cast<unsigned char>(hdr[2])) << 8) |
            static_cast<uint32_t>(static_cast<unsigned char>(hdr[3]));
        if (body_len < 6 || body_len > 16u * 1024u * 1024u) { ++st.failed; break; }

        std::string body;
        body.resize(body_len);
        if (!recv_all(fd, body.data(), body_len)) { ++st.failed; break; }

        const double t1 = now_ms();
        st.lat_ms.push_back(t1 - t0);
        st.bytes_sent += 0;

        const uint16_t type = static_cast<uint16_t>(
            (static_cast<unsigned char>(body[0]) << 8) | static_cast<unsigned char>(body[1]));
        const std::string payload = body.substr(6);

        if (type == kTypeTaskResp) ++st.ok;
        else if (payload == "RATE_LIMITED") ++st.rate_limited;
        else if (type == kTypeTaskFailed) ++st.failed;
        else if (type == kTypeErrorResp) ++st.other;
        else ++st.other;

        ++id;
    }
    ::close(fd);
}

// 只发不收：触发服务端发送侧背压
void run_blast(const Args& args, int conn_index, Stats& st)
{
    const int fd = connect_to(args.host, args.port);
    if (fd < 0) { ++st.failed; return; }

    const double deadline = now_ms() + static_cast<double>(args.seconds) * 1000.0;
    uint32_t id = static_cast<uint32_t>(conn_index) * 1000000u + 1u;

    while (now_ms() < deadline)
    {
        const std::string frame = make_frame(kTypeTaskReq, id, args.payload);
        if (!send_all(fd, frame.data(), frame.size())) { ++st.failed; break; }
        st.bytes_sent += static_cast<long>(frame.size());
        ++st.ok;
        ++id;
    }
    // 不读、不关，让服务端把响应堆起来
    ::close(fd);
}

void print_usage()
{
    std::printf(
        "usage: bench_client [--host H] [--port P] [--conns N] [--seconds S]\n"
        "                    [--payload B] [--mode closed_loop|blast] [--csv path]\n");
}

} // namespace

int main(int argc, char** argv)
{
    Args args;
    for (int i = 1; i < argc; ++i)
    {
        const std::string k = argv[i];
        auto next = [&](const char* what) -> std::string {
            if (i + 1 >= argc) { std::fprintf(stderr, "missing value for %s\n", what); std::exit(2); }
            return argv[++i];
        };
        if (k == "--host") args.host = next("--host");
        else if (k == "--port") args.port = std::atoi(next("--port").c_str());
        else if (k == "--conns") args.conns = std::atoi(next("--conns").c_str());
        else if (k == "--seconds") args.seconds = std::atoi(next("--seconds").c_str());
        else if (k == "--payload") args.payload = std::atoi(next("--payload").c_str());
        else if (k == "--warmup-ms") args.warmup_ms = std::atoi(next("--warmup-ms").c_str());
        else if (k == "--mode") args.mode = next("--mode");
        else if (k == "--csv") args.csv = next("--csv");
        else { print_usage(); return 2; }
    }

    if (args.conns <= 0 || args.seconds <= 0 || args.payload < 0) { print_usage(); return 2; }
    if (args.mode != "closed_loop" && args.mode != "blast") { print_usage(); return 2; }

    std::vector<Stats> per_thread(static_cast<std::size_t>(args.conns));
    std::vector<std::thread> threads;
    threads.reserve(static_cast<std::size_t>(args.conns));

    const double t_start = now_ms();

    for (int i = 0; i < args.conns; ++i)
    {
        threads.emplace_back([&, i]() {
            if (args.mode == "blast") run_blast(args, i, per_thread[static_cast<std::size_t>(i)]);
            else run_closed_loop(args, i, per_thread[static_cast<std::size_t>(i)]);
        });
    }
    for (auto& t : threads) t.join();

    const double elapsed_s = (now_ms() - t_start) / 1000.0;

    Stats total;
    for (const auto& s : per_thread)
    {
        total.ok += s.ok;
        total.rate_limited += s.rate_limited;
        total.failed += s.failed;
        total.other += s.other;
        total.bytes_sent += s.bytes_sent;
        total.lat_ms.insert(total.lat_ms.end(), s.lat_ms.begin(), s.lat_ms.end());
    }

    std::sort(total.lat_ms.begin(), total.lat_ms.end());
    auto pct = [&](double p) -> double {
        if (total.lat_ms.empty()) return 0.0;
        std::size_t idx = static_cast<std::size_t>(p * static_cast<double>(total.lat_ms.size()));
        if (idx >= total.lat_ms.size()) idx = total.lat_ms.size() - 1;
        return total.lat_ms[idx];
    };

    const long responses = total.ok + total.rate_limited + total.failed + total.other;
    const double qps = elapsed_s > 0 ? static_cast<double>(responses) / elapsed_s : 0.0;

    std::printf("mode=%s conns=%d seconds=%d payload=%d\n",
                args.mode.c_str(), args.conns, args.seconds, args.payload);
    std::printf("total=%ld ok=%ld rate_limited=%ld failed=%ld other=%ld qps=%.1f\n",
                responses, total.ok, total.rate_limited, total.failed, total.other, qps);
    if (!total.lat_ms.empty())
    {
        std::printf("lat_ms p50=%.2f p95=%.2f p99=%.2f max=%.2f\n",
                    pct(0.50), pct(0.95), pct(0.99), total.lat_ms.back());
    }
    std::printf("sent_bytes=%ld elapsed_s=%.2f\n", total.bytes_sent, elapsed_s);

    if (!args.csv.empty())
    {
        if (FILE* f = std::fopen(args.csv.c_str(), "w"))
        {
            std::fprintf(f, "mode,conns,seconds,payload,total,ok,rate_limited,failed,other,qps,p50,p95,p99,max\n");
            std::fprintf(f, "%s,%d,%d,%d,%ld,%ld,%ld,%ld,%ld,%.1f,%.2f,%.2f,%.2f,%.2f\n",
                         args.mode.c_str(), args.conns, args.seconds, args.payload,
                         responses, total.ok, total.rate_limited, total.failed, total.other,
                         qps, pct(0.50), pct(0.95), pct(0.99),
                         total.lat_ms.empty() ? 0.0 : total.lat_ms.back());
            std::fclose(f);
        }
    }
    return 0;
}
