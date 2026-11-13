#include "Runtime/TaskStore.h"

#include "Logger/AsyncLogger.h"

#include <cstdlib>
#include <cstring>
#include <utility>

namespace store
{
    namespace
    {
        // 两张表：任务 + 会话计数。session 的 task_count 用来演示"同一事务里的两个写"。
        const char* kCreateTask =
            "CREATE TABLE IF NOT EXISTS agent_task ("
            "  request_id BIGINT UNSIGNED NOT NULL,"
            "  session_id BIGINT UNSIGNED NOT NULL DEFAULT 0,"
            "  client_id  BIGINT UNSIGNED NOT NULL,"
            "  status     TINYINT NOT NULL,"
            "  result     MEDIUMTEXT NULL,"
            "  error      VARCHAR(128) NULL,"
            "  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
            "  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),"
            "  PRIMARY KEY (request_id),"
            "  KEY idx_client_time (client_id, created_at)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        const char* kCreateSession =
            "CREATE TABLE IF NOT EXISTS agent_session ("
            "  session_id BIGINT UNSIGNED NOT NULL,"
            "  client_id  BIGINT UNSIGNED NOT NULL,"
            "  task_count INT NOT NULL DEFAULT 0,"
            "  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),"
            "  PRIMARY KEY (session_id),"
            "  KEY idx_client_time (client_id, updated_at)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        std::string Escape(MYSQL* conn, const std::string& in)
        {
            std::string out(in.size() * 2 + 1, '\0');
            const unsigned long n = ::mysql_real_escape_string(
                conn, out.data(), in.data(), static_cast<unsigned long>(in.size()));
            out.resize(n);
            return out;
        }

        std::string CacheKey(uint32_t request_id)
        {
            return "agent_task:" + std::to_string(request_id);
        }

        // 缓存值格式：status \x1f error \x1f result
        constexpr char kSep = '\x1f';
    }

    TaskStore::TaskStore(std::shared_ptr<db::MySQLPool> pool,
                         std::shared_ptr<RedisPool> redis)
        : pool_(std::move(pool)), redis_(std::move(redis))
    {
        if (!pool_ || !pool_->Valid())
        {
            error_ = "mysql pool unavailable";
            return;
        }
        if (!EnsureSchema()) return;

        ready_ = true;
        writer_ = std::thread([this] { WriterLoop(); });
    }

    TaskStore::~TaskStore()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        if (writer_.joinable()) writer_.join();
    }

    bool TaskStore::EnsureSchema()
    {
        auto conn = pool_->Acquire();
        if (!conn)
        {
            error_ = "no mysql connection for schema";
            return false;
        }
        if (::mysql_query(conn.get(), kCreateTask) != 0 ||
            ::mysql_query(conn.get(), kCreateSession) != 0)
        {
            error_ = std::string(::mysql_error(conn.get()));
            return false;
        }
        return true;
    }

    bool TaskStore::TryClaim(uint32_t request_id, uint64_t session_id, uint64_t client_id)
    {
        if (!ready_) return false;

        auto conn = pool_->Acquire();
        if (!conn) return false;

        // 一个事务里做两件事：占位任务 + 会话计数 +1。
        // 要么都成功，要么都回滚——避免"任务存在但计数没加"。
        if (::mysql_query(conn.get(), "START TRANSACTION") != 0) return false;

        const std::string insert_task =
            "INSERT INTO agent_task (request_id, session_id, client_id, status) VALUES (" +
            std::to_string(request_id) + "," + std::to_string(session_id) + "," +
            std::to_string(client_id) + ",0)";

        if (::mysql_query(conn.get(), insert_task.c_str()) != 0)
        {
            ::mysql_query(conn.get(), "ROLLBACK");
            return false;
        }

        // 唯一键冲突 → 重复提交，回滚并返回 false
        if (::mysql_affected_rows(conn.get()) == 0)
        {
            ::mysql_query(conn.get(), "ROLLBACK");
            return false;
        }

        const std::string upsert_session =
            "INSERT INTO agent_session (session_id, client_id, task_count) VALUES (" +
            std::to_string(session_id) + "," + std::to_string(client_id) + ",1) "
            "ON DUPLICATE KEY UPDATE task_count = task_count + 1";

        if (::mysql_query(conn.get(), upsert_session.c_str()) != 0)
        {
            ::mysql_query(conn.get(), "ROLLBACK");
            return false;
        }

        return ::mysql_query(conn.get(), "COMMIT") == 0;
    }

    void TaskStore::SubmitTerminal(uint32_t request_id, uint64_t session_id, uint64_t client_id,
                                   TaskStatus status, const std::string& result,
                                   const std::string& error)
    {
        if (!ready_) return;

        TaskRecord rec{request_id, session_id, client_id, status, result, error};
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_) return;
            if (queue_.size() >= kMaxQueue)
            {
                dropped_.fetch_add(1);
                return;
            }
            queue_.push_back(std::move(rec));
        }
        cv_.notify_one();
    }

    void TaskStore::WriterLoop()
    {
        for (;;)
        {
            TaskRecord rec;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return stop_ || !queue_.empty(); });
                if (queue_.empty())
                {
                    if (stop_) break;
                    continue;
                }
                rec = std::move(queue_.front());
                queue_.pop_front();
            }

            if (WriteOne(rec)) written_.fetch_add(1);
            else failed_writes_.fetch_add(1);
        }
    }

    bool TaskStore::WriteOne(const TaskRecord& rec)
    {
        auto conn = pool_->Acquire();
        if (!conn) return false;

        const std::string result = Escape(conn.get(), rec.result);
        const std::string error = Escape(conn.get(), rec.error);

        const std::string sql =
            "INSERT INTO agent_task (request_id, session_id, client_id, status, result, error) VALUES (" +
            std::to_string(rec.request_id) + "," + std::to_string(rec.session_id) + "," +
            std::to_string(rec.client_id) + "," + std::to_string(static_cast<int>(rec.status)) +
            ",'" + result + "','" + error + "') "
            "ON DUPLICATE KEY UPDATE session_id=VALUES(session_id), client_id=VALUES(client_id), "
            "status=VALUES(status), result=VALUES(result), error=VALUES(error)";

        if (::mysql_query(conn.get(), sql.c_str()) != 0)
        {
            LOG_WARN(std::string("[TaskStore] write failed: ") + ::mysql_error(conn.get()));
            return false;
        }

        // 先更库、再删缓存（cache-aside）。删除放在写成功之后，
        // 保证不会出现"缓存已删、库还没写"导致读旧值的窗口。
        CacheInvalidate(rec.request_id);
        return true;
    }

    void TaskStore::CacheInvalidate(uint32_t request_id)
    {
        if (!redis_) return;
        auto conn = redis_->getConnection();
        if (!conn) return;
        const std::string key = CacheKey(request_id);
        ::redisCommand(conn.get(), "DEL %s", key.c_str());
    }

    bool TaskStore::Query(uint32_t request_id, TaskRecord& out)
    {
        if (!ready_) return false;

        // ① 先查缓存
        if (redis_)
        {
            auto conn = redis_->getConnection();
            if (conn)
            {
                const std::string key = CacheKey(request_id);
                redisReply* reply = static_cast<redisReply*>(
                    ::redisCommand(conn.get(), "GET %s", key.c_str()));
                std::unique_ptr<redisReply, decltype(&freeReplyObject)> guard(reply, freeReplyObject);
                if (reply != nullptr && reply->type == REDIS_REPLY_STRING)
                {
                    const std::string val(reply->str, static_cast<std::size_t>(reply->len));
                    cache_hits_.fetch_add(1);
                    if (val == "MISS")
                    {
                        return false;   // 空结果缓存：确实不存在
                    }
                    const auto p1 = val.find(kSep);
                    const auto p2 = (p1 == std::string::npos) ? std::string::npos : val.find(kSep, p1 + 1);
                    if (p1 != std::string::npos && p2 != std::string::npos)
                    {
                        out.request_id = request_id;
                        out.status = static_cast<TaskStatus>(std::atoi(val.substr(0, p1).c_str()));
                        out.error = val.substr(p1 + 1, p2 - p1 - 1);
                        out.result = val.substr(p2 + 1);
                        return true;
                    }
                }
            }
        }

        // ② 缓存未命中，查库
        cache_misses_.fetch_add(1);
        auto conn = pool_->Acquire();
        if (!conn) return false;

        const std::string sql =
            "SELECT request_id, session_id, client_id, status, IFNULL(result,''), IFNULL(error,'') "
            "FROM agent_task WHERE request_id=" + std::to_string(request_id) + " LIMIT 1";

        if (::mysql_query(conn.get(), sql.c_str()) != 0) return false;

        MYSQL_RES* res = ::mysql_store_result(conn.get());
        if (res == nullptr) return false;

        MYSQL_ROW row = ::mysql_fetch_row(res);
        if (row == nullptr)
        {
            ::mysql_free_result(res);
            // 空结果也缓存（防穿透），TTL 更短
            if (redis_)
            {
                auto rc = redis_->getConnection();
                if (rc)
                {
                    const std::string key = CacheKey(request_id);
                    ::redisCommand(rc.get(), "SET %s MISS EX 10", key.c_str());
                }
            }
            return false;
        }

        out.request_id = static_cast<uint32_t>(std::strtoul(row[0] ? row[0] : "0", nullptr, 10));
        out.session_id = std::strtoull(row[1] ? row[1] : "0", nullptr, 10);
        out.client_id = std::strtoull(row[2] ? row[2] : "0", nullptr, 10);
        out.status = static_cast<TaskStatus>(std::atoi(row[3] ? row[3] : "0"));
        out.result = row[4] ? row[4] : "";
        out.error = row[5] ? row[5] : "";
        ::mysql_free_result(res);

        // ③ 回填缓存（TTL 60s）
        if (redis_)
        {
            auto rc = redis_->getConnection();
            if (rc)
            {
                const std::string key = CacheKey(request_id);
                const std::string val = std::to_string(static_cast<int>(out.status)) +
                                        std::string(1, kSep) + out.error +
                                        std::string(1, kSep) + out.result;
                ::redisCommand(rc.get(), "SET %s %b EX 60", key.c_str(), val.data(), val.size());
            }
        }
        return true;
    }
}
