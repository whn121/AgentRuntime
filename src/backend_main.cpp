#include "Common/Config.h"
#include "DB/RedisPool.h"
#include "Net/Connection.h"
#include "Protocol/MyProtocol.h"
#include "Reactor/Channel.h"
#include "Server/Server.h"
#include "Runtime/RuntimeEngine.h"
#include "Logger/AsyncLogger.h"
#include "Runtime/LlamaClient.h"
#include "Metrics/Metrics.h"

#include <any>
#include <memory>
#include <string>
#include <any>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

namespace 
{
    // 地址值必须保持 IPv4:port；所有权令牌放在服务发现命名空间之外。
    class BackendRegistration 
    {
    public:
        explicit BackendRegistration(uint16_t port)
            : pool_(Config::instance().redis_host, Config::instance().redis_port, 1),
            key_("services:backend_service:node-" + std::to_string(port)),
            owner_key_("backend:owners:node-" + std::to_string(port)),
            address_(Config::instance().bind_host + ":" + std::to_string(port))
        {
            std::ifstream uuid("/proc/sys/kernel/random/uuid");
            if (!(uuid >> token_) || token_.empty())
                throw std::runtime_error("Cannot generate backend registration ownership token");
        }

        void start()
        {
            if (!renew()) throw std::runtime_error("Backend Redis registration failed: " + key_);
            registered_ = true;
            std::cerr << "[Backend] registered " << key_ << "=" << address_ << " (TTL 10s, renewal 3s)\n";
            thread_ = std::thread([this] 
            {
                std::unique_lock<std::mutex> lock(mutex_);
                while (!cv_.wait_for(lock, std::chrono::seconds(3), [this] { return stop_; })) 
                {
                    lock.unlock();
                    try 
                    {
                        if (!renew())
                            std::cerr << "[Backend] lease renewal failed; retrying in 3s (TTL bounds stale registration)\n";
                    } 
                    catch (const std::exception& e) 
                    {
                        std::cerr << "[Backend] lease renewal exception: " << e.what() << "; retrying in 3s\n";
                    }
                    lock.lock();
                }
            });
        }

        ~BackendRegistration()
        {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                stop_ = true;
            }
            cv_.notify_all();
            if (thread_.joinable()) thread_.join();
            if (!registered_) return;
            // 必须先停止续租，再以原子比较删除保护后来启动的同名实例。
            try 
            {
                const char* script =
                    "if redis.call('GET',KEYS[2]) ~= ARGV[1] then return 0 end "
                    "if redis.call('GET',KEYS[1]) == ARGV[2] then redis.call('DEL',KEYS[1]) end "
                    "redis.call('DEL',KEYS[2]); return 1";
                const auto result = execute(script);
                if (result == 1) std::cerr << "[Backend] registration removed\n";
                else if (result == 0) std::cerr << "[Backend] registration ownership changed; not deleting\n";
            } 
            catch (const std::exception& e) 
            {
                std::cerr << "[Backend] registration cleanup failed: " << e.what() << '\n';
            }
        }

    private:
        long long execute(const char* script)
        {
            // getConnection() 返回的 shared_ptr 自动归还连接，不手动 release。
            auto conn = pool_.getConnection();
            if (!conn) 
            {
                std::cerr << "[Backend] Redis connection unavailable\n";
                return -1;
            }
            std::unique_ptr<redisReply, decltype(&freeReplyObject)> reply(
                static_cast<redisReply*>(redisCommand(conn.get(), "EVAL %s 2 %s %s %s %s",
                script, key_.c_str(), owner_key_.c_str(), token_.c_str(), address_.c_str())),
                freeReplyObject);
            if (!reply || reply->type != REDIS_REPLY_INTEGER) 
            {
                std::cerr << "[Backend] Redis registration command failed: " << (reply && reply->str ? reply->str : conn->errstr) << '\n';
                return -1;
            }
            return reply->integer;
        }

        bool renew()
        {
            // Lua 原子校验所有权并用 SET EX 写入，两个键具有同样的有效期。
            // Redis 重启或租约过期后可重注册，但绝不覆盖其他实例的租约。
            const char* script =
                "local owner=redis.call('GET',KEYS[2]); "
                "if owner and owner ~= ARGV[1] then return 0 end "
                "local addr=redis.call('GET',KEYS[1]); "
                "if not owner and addr then return 0 end "
                "if owner and addr and addr ~= ARGV[2] then return 0 end "
                "redis.call('SET',KEYS[1],ARGV[2],'EX',10); "
                "redis.call('SET',KEYS[2],ARGV[1],'EX',10); return 1";
            const auto result = execute(script);
            if (result == 0) std::cerr << "[Backend] registration ownership conflict: " << key_ << '\n';
            return result == 1;
        }

        RedisPool pool_;
        std::string key_, owner_key_, address_, token_;
        std::mutex mutex_;
        std::condition_variable cv_;
        bool stop_{false};
        bool registered_{false};
        std::thread thread_;
    };
}

int main(int argc, char* argv[]) try
{
    auto& config = Config::instance();
    config.port = 9001;
    config.parseArgs(argc, argv);

    auto llamaClient = std::make_shared<runtime::LlamaClient>();

    runtime::RuntimeEngine runtimeEngine([llamaClient]
        (const std::string& input, const runtime::CancellationToken& token) -> std::string
    {
        return llamaClient->complete(input, token);
    },
    static_cast<std::size_t>(config.engine_workers),   // --engine-workers，压测时用来定位瓶颈
    1024     // 最多排队等待的任务数
    );
    Server server;
        server.setcreator([&runtimeEngine](int fd, EventLoop* loop) 
    {
        // 后端**不限流**（容量传 0 → allowRequest() 恒 true）：
        // 它看到的连接是网关的复用连接，一条连接承载所有客户端流量，
        // 按连接限流会把整片流量误判成单客户端超速。
        // 客户端准入限流只在网关按真实客户端连接做（见 gateway_main.cpp）。
        auto conn = std::make_shared<Connection>(fd, std::make_unique<Channel>(fd), loop,
            std::make_unique<MyProtocol>(),
            0.0,   // rate_capacity：0 = 关闭
            0.0);  // rate_refill_per_sec

        // 连接关闭时通知 RuntimeEngine 取消该 clientId 的待调度任务。
        // clientId 是连接自己的逻辑标识（单调递增），不是 fd——fd 会被内核复用。
        conn->setCancelHook([&runtimeEngine](std::uint64_t clientId)
        {
            runtimeEngine.cancelByClient(clientId);
        });

        return conn;
    });

    server.setBeforeIoShutdownCallback([&runtimeEngine, &config]() 
    {
        // 停服模式由 --shutdown-mode 决定：
        //   graceful（默认）= drain + deadline：先不取消，等任务自然完成；
        //                    超过 --shutdown-deadline-ms 才取消剩余、标失败。
        //   force          = 立刻取消 + 丢弃 pending，秒级退出。
        const auto mode = (config.shutdown_mode == "force")
            ? runtime::RuntimeEngine::ShutdownMode::FORCE
            : runtime::RuntimeEngine::ShutdownMode::GRACEFUL;

        runtimeEngine.stop(mode, std::chrono::milliseconds(config.shutdown_deadline_ms));
    });

    server.setBusinessHandler([&runtimeEngine](std::shared_ptr<Connection> conn, const std::any& message) 
    {
        try
        {
            const auto request = std::any_cast<MyMessage>(message);
            
            if (request.type_ == MyType::HEARTBEAT_REQ)
            {
                // 序列化在业务线程完成，IO 线程只负责搬字节（见 Connection::sendRaw）
                conn->sendRaw(MyProtocol::encodeMessage(
                    MyType::HEARTBEAT_RESP, request.id_, request.payload_));
                return;
            }

            // 第一阶段只处理明确约定的Runtime业务请求
            if (request.type_ != MyType::RUNTIME_TASK_REQ)
            {
                conn->sendRaw(MyProtocol::encodeMessage(
                    MyType::ERROR_RESP, request.id_, "UNSUPPORTED_MESSAGE_TYPE"));
                return;
            }

            
            // 将任务提交给长期运行的 RuntimeEngine，异步执行。
            // 保存连接和客户端请求 ID，以便任务完成后返回原请求的响应。
            const auto requestId = request.id_;
            const auto input = request.payload_;

            // 用逻辑 clientId（不是 fd）：fd 会被内核复用，不能当身份。
            const auto clientId = conn->clientId();

            // ★ 这里刻意【不做】限流。
            // 后端看到的连接是网关的复用连接（一条连接承载所有客户端流量），
            // 按连接限流会把整片流量误判成"单客户端超速"——压测实测：50 客户端
            // 10 秒只有 630 个成功（= 3 条后端连接 × 210）。
            // 客户端准入限流只在网关做（见 gateway_main.cpp），维度才对。
            const bool accepted = runtimeEngine.submit(clientId, input, [conn, requestId]
                (const std::shared_ptr<runtime::RuntimeTask>& task, runtime::WorkerExecutionResult result)
            {
                // 关闭期间连接可能已经进了 Closing, sendResponse 会静默丢掉
                // 这是预期的, 不是 bug。见 docs/shutdown-semantics.md
                // 别在这里加"关闭时也回最后一帧"的逻辑, 会把停服时序搞乱
                if (task && result == runtime::WorkerExecutionResult::SUCCEEDED)
                {
                    // 这个 lambda 运行在 RuntimeEngine 的 worker 线程里：
                    // encode 在这里做，IO 线程只收字节
                    conn->sendRaw(MyProtocol::encodeMessage(
                        MyType::RUNTIME_TASK_RESP, requestId, task->result()));
                }
                else
                {
                    const std::string error = task && !task->error().empty() ? task->error() : "TASK_EXECUTION_FAILED";

                    conn->sendRaw(MyProtocol::encodeMessage(
                        MyType::RUNTIME_TASK_FAILED, requestId, error));
                }
            });

            if (!accepted)
            {
                conn->sendRaw(MyProtocol::encodeMessage(
                    MyType::RUNTIME_TASK_FAILED, requestId, "TASK_SUBMISSION_FAILED"));
            }
        }
        catch (const std::bad_any_cast&)
        {
            LOG_ERROR("[Backend] invalid message type");
        }
        catch (const std::exception& e)
        {
            LOG_ERROR("[Backend] runtime request failed: " + std::string(e.what()));
        }
        catch (...)
        {
            LOG_ERROR("[Backend] unknown runtime request failure");
        }
        
    });

    bool started = false;
    {
        BackendRegistration registration(config.port);
        registration.start();
        // 使用 Server 内置 SignalHandler；仅主线程在 start 返回后停止 Server。
        started = server.start();
    } // 停止并 join 续租线程，然后尽力注销；Server 和 RedisPool 仍然有效。
    server.stop();

    // 退出时打印一行指标快照：压测脚本用它断言"连接构造数 == 析构数"（无泄漏）。
    // 现在没有 /metrics 端点，日志是唯一的外部可观测出口。
    // 同时写 stderr：异步日志是缓冲落盘的，压测脚本按 stdout/stderr 抓更可靠。
    LOG_INFO("[Backend] metrics: " + Metrics::Summary());
    std::cerr << "[Backend] metrics: " << Metrics::Summary() << '\n';

    if (!started) std::cerr << "[Backend] server start failed\n";
    return started ? 0 : 1;
}
catch (const std::exception& e)
{
    std::cerr << "[Backend] startup/runtime failure: " << e.what() << '\n';
    return 1;
}