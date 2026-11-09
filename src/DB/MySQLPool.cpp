#include "DB/MySQLPool.h"

#include <utility>

namespace db
{
    MySQLPool::MySQLPool(std::string host, unsigned port, std::string user,
                         std::string password, std::string database, int size)
        : host_(std::move(host)), user_(std::move(user)), password_(std::move(password)),
          database_(std::move(database)), port_(port), size_(size > 0 ? size : 1)
    {
        for (int i = 0; i < size_; ++i)
        {
            MYSQL* conn = Connect();
            if (conn == nullptr) break;
            free_.push_back(conn);
        }
        valid_ = !free_.empty();
    }

    MySQLPool::~MySQLPool()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (MYSQL* conn : free_)
        {
            ::mysql_close(conn);
        }
        free_.clear();
    }

    MYSQL* MySQLPool::Connect()
    {
        MYSQL* conn = ::mysql_init(nullptr);
        if (conn == nullptr)
        {
            error_ = "mysql_init failed";
            return nullptr;
        }

        ::mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8mb4");

        // 连接超时要短：DB 不可用时不应该拖住启动流程。
        const unsigned int connect_timeout = 2;
        ::mysql_options(conn, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);

        if (::mysql_real_connect(conn, host_.c_str(), user_.c_str(), password_.c_str(),
                                 database_.c_str(), port_, nullptr, 0) == nullptr)
        {
            error_ = std::string(::mysql_error(conn));
            ::mysql_close(conn);
            return nullptr;
        }
        return conn;
    }

    std::shared_ptr<MYSQL> MySQLPool::Acquire()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (free_.empty()) return nullptr;

        MYSQL* conn = free_.back();
        free_.pop_back();
        return std::shared_ptr<MYSQL>(conn, [this](MYSQL* c) { Return(c); });
    }

    void MySQLPool::Return(MYSQL* conn)
    {
        if (conn == nullptr) return;
        std::lock_guard<std::mutex> lock(mutex_);
        free_.push_back(conn);
    }

    bool MySQLPool::Exec(const std::string& sql)
    {
        auto conn = Acquire();
        if (!conn) return false;
        return ::mysql_query(conn.get(), sql.c_str()) == 0;
    }

    bool MySQLPool::QueryScalar(const std::string& sql, std::string& value)
    {
        auto conn = Acquire();
        if (!conn) return false;

        if (::mysql_query(conn.get(), sql.c_str()) != 0) return false;

        MYSQL_RES* res = ::mysql_store_result(conn.get());
        if (res == nullptr) return false;

        MYSQL_ROW row = ::mysql_fetch_row(res);
        if (row == nullptr)
        {
            ::mysql_free_result(res);
            return false;
        }

        value = (row[0] != nullptr) ? row[0] : "";
        ::mysql_free_result(res);
        return true;
    }
}
