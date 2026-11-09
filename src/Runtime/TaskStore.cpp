#include "Runtime/TaskStore.h"

#include "Logger/AsyncLogger.h"

#include <cstdlib>
#include <utility>

namespace store
{
    namespace
    {
        // 表结构见 sql/schema.sql（同一份 DDL，便于人工对照）
        const char* kCreateTable =
            "CREATE TABLE IF NOT EXISTS agent_task ("
            "  request_id BIGINT UNSIGNED NOT NULL,"
            "  client_id  BIGINT UNSIGNED NOT NULL,"
            "  status     TINYINT NOT NULL,"
            "  result     MEDIUMTEXT NULL,"
            "  error      VARCHAR(128) NULL,"
            "  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
            "  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),"
            "  PRIMARY KEY (request_id),"
            "  KEY idx_client_time (client_id, created_at)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        std::string Escape(MYSQL* conn, const std::string& in)
        {
            std::string out(in.size() * 2 + 1, '\0');
            const unsigned long n = ::mysql_real_escape_string(
                conn, out.data(), in.data(), static_cast<unsigned long>(in.size()));
            out.resize(n);
            return out;
        }
    }

    TaskStore::TaskStore(std::shared_ptr<db::MySQLPool> pool)
        : pool_(std::move(pool))
    {
        if (!pool_ || !pool_->Valid())
        {
            error_ = "mysql pool unavailable";
            return;
        }
        if (!EnsureSchema())
        {
            return;
        }
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
        if (::mysql_query(conn.get(), kCreateTable) != 0)
        {
            error_ = std::string(::mysql_error(conn.get()));
            return false;
        }
        return true;
    }

    void TaskStore::SubmitTerminal(uint32_t request_id, uint64_t client_id, TaskStatus status,
                                   const std::string& result, const std::string& error)
    {
        if (!ready_) return;

        TaskRecord rec{request_id, client_id, status, result, error};
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_) return;
            if (queue_.size() >= kMaxQueue)
            {
                // DB 是旁路：宁可丢这一条，也不能让引擎线程等
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

        // 幂等写：request_id 是唯一键，重复终态只更新，不产生第二行。
        const std::string sql =
            "INSERT INTO agent_task (request_id, client_id, status, result, error) VALUES (" +
            std::to_string(rec.request_id) + "," +
            std::to_string(rec.client_id) + "," +
            std::to_string(static_cast<int>(rec.status)) + ",'" + result + "','" + error + "') "
            "ON DUPLICATE KEY UPDATE client_id=VALUES(client_id), status=VALUES(status), "
            "result=VALUES(result), error=VALUES(error)";

        if (::mysql_query(conn.get(), sql.c_str()) != 0)
        {
            LOG_WARN(std::string("[TaskStore] write failed: ") + ::mysql_error(conn.get()));
            return false;
        }
        return true;
    }

    bool TaskStore::Query(uint32_t request_id, TaskRecord& out)
    {
        if (!ready_) return false;

        auto conn = pool_->Acquire();
        if (!conn) return false;

        const std::string sql =
            "SELECT request_id, client_id, status, IFNULL(result,''), IFNULL(error,'') "
            "FROM agent_task WHERE request_id=" + std::to_string(request_id) + " LIMIT 1";

        if (::mysql_query(conn.get(), sql.c_str()) != 0) return false;

        MYSQL_RES* res = ::mysql_store_result(conn.get());
        if (res == nullptr) return false;

        MYSQL_ROW row = ::mysql_fetch_row(res);
        if (row == nullptr)
        {
            ::mysql_free_result(res);
            return false;
        }

        out.request_id = static_cast<uint32_t>(std::strtoul(row[0] ? row[0] : "0", nullptr, 10));
        out.client_id = std::strtoull(row[1] ? row[1] : "0", nullptr, 10);
        out.status = static_cast<TaskStatus>(std::atoi(row[2] ? row[2] : "0"));
        out.result = row[3] ? row[3] : "";
        out.error = row[4] ? row[4] : "";

        ::mysql_free_result(res);
        return true;
    }

    std::size_t TaskStore::QueueDepth() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
}
