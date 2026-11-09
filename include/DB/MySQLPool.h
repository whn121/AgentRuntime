#pragma once

#include <mysql/mysql.h>

#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace db
{
    // 极简 MySQL 连接池：固定 N 条连接，借/还，不扩容。
    //
    // 为什么不做动态扩容：DB 只用在"落终态 + 幂等查询"，QPS 远低于主链路，
    // 固定小池足够；扩容要配健康检查、超时重建，复杂度与收益不成比例。
    class MySQLPool
    {
    public:
        MySQLPool(std::string host, unsigned port, std::string user,
                  std::string password, std::string database, int size);
        ~MySQLPool();

        MySQLPool(const MySQLPool&) = delete;
        MySQLPool& operator=(const MySQLPool&) = delete;

        bool Valid() const { return valid_; }
        const std::string& Error() const { return error_; }

        // 借出一条连接；shared_ptr 析构时自动归还。
        // 池耗尽时返回 nullptr（调用方应记日志并降级，不要阻塞主链路）。
        std::shared_ptr<MYSQL> Acquire();

        bool Exec(const std::string& sql);

        // 取第一行第一列（NULL 视为空串）；无结果行返回 false
        bool QueryScalar(const std::string& sql, std::string& value);

    private:
        void Return(MYSQL* conn);
        MYSQL* Connect();

        std::string host_, user_, password_, database_;
        unsigned port_;
        int size_;
        bool valid_{false};
        std::string error_;
        mutable std::mutex mutex_;
        std::vector<MYSQL*> free_;
    };
}
