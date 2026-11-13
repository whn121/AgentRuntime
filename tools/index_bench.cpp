// 索引实验：10 万行下"按客户端 + 时间查历史"的扫描行数与延迟对比。
//
// 为什么单独做这个工具：面试问"这条 SQL 为什么慢、怎么优化"时，
// 需要能拿出 EXPLAIN 前后的 rows 数字，而不是背"加索引就好了"。
//
// 用法：
//   ./agentruntime_index_bench 127.0.0.1 3306 agent agent123 agent_runtime [rows]
// 输出：
//   无索引：rows=10xxxx  avg=xx ms
//   有索引：rows=xxx     avg=x.x ms

#include "DB/MySQLPool.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>

namespace
{
    // 跑 EXPLAIN，取 rows 列
    long long ExplainRows(db::MySQLPool& pool, const std::string& sql, std::string& err)
    {
        auto conn = pool.Acquire();
        if (!conn) { err = "no connection"; return -1; }

        const std::string explain = "EXPLAIN " + sql;
        if (::mysql_query(conn.get(), explain.c_str()) != 0)
        {
            err = ::mysql_error(conn.get());
            return -1;
        }

        MYSQL_RES* res = ::mysql_store_result(conn.get());
        if (res == nullptr) { err = "no result"; return -1; }

        MYSQL_FIELD* fields = ::mysql_fetch_fields(res);
        const unsigned n = ::mysql_num_fields(res);
        int rows_idx = -1;
        for (unsigned i = 0; i < n; ++i)
        {
            if (std::string(fields[i].name) == "rows") { rows_idx = static_cast<int>(i); break; }
        }

        long long rows = -1;
        MYSQL_ROW row = ::mysql_fetch_row(res);
        if (row != nullptr && rows_idx >= 0 && row[rows_idx] != nullptr)
        {
            rows = std::atoll(row[rows_idx]);
        }
        ::mysql_free_result(res);
        return rows;
    }

    // 跑 N 次查询取平均毫秒
    double AvgMs(db::MySQLPool& pool, const std::string& sql, int iters)
    {
        auto conn = pool.Acquire();
        if (!conn) return -1.0;

        const auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < iters; ++i)
        {
            if (::mysql_query(conn.get(), sql.c_str()) != 0) return -1.0;
            MYSQL_RES* res = ::mysql_store_result(conn.get());
            if (res != nullptr)
            {
                while (::mysql_fetch_row(res) != nullptr) { /* 消费掉 */ }
                ::mysql_free_result(res);
            }
        }
        const auto t1 = std::chrono::steady_clock::now();
        return std::chrono::duration<double, std::milli>(t1 - t0).count() / iters;
    }
}

int main(int argc, char** argv)
{
    const std::string host = (argc > 1) ? argv[1] : "127.0.0.1";
    const unsigned port = (argc > 2) ? static_cast<unsigned>(std::atoi(argv[2])) : 3306;
    const std::string user = (argc > 3) ? argv[3] : "agent";
    const std::string pass = (argc > 4) ? argv[4] : "agent123";
    const std::string dbase = (argc > 5) ? argv[5] : "agent_runtime";
    const int total_rows = (argc > 6) ? std::atoi(argv[6]) : 100000;

    db::MySQLPool pool(host, port, user, pass, dbase, 2);
    if (!pool.Valid())
    {
        std::printf("db unavailable: %s\n", pool.Error().c_str());
        return 1;
    }

    pool.Exec("DROP TABLE IF EXISTS bench_task");
    if (!pool.Exec(
            "CREATE TABLE bench_task ("
            "  id BIGINT AUTO_INCREMENT PRIMARY KEY,"
            "  client_id BIGINT NOT NULL,"
            "  created_at DATETIME(3) NOT NULL,"
            "  payload VARCHAR(64) NOT NULL"
            ") ENGINE=InnoDB"))
    {
        std::printf("create table failed\n");
        return 1;
    }

    // 批量插入（每批 1000 行，避免 10 万次单行 INSERT）
    const int batch = 1000;
    auto conn = pool.Acquire();
    if (!conn) { std::printf("no connection\n"); return 1; }

    for (int base = 0; base < total_rows; base += batch)
    {
        std::string sql = "INSERT INTO bench_task (client_id, created_at, payload) VALUES ";
        for (int i = 0; i < batch; ++i)
        {
            const int n = base + i;
            if (i > 0) sql += ",";
            sql += "(" + std::to_string(n % 1000) + ", NOW(3) - INTERVAL " +
                   std::to_string(n % 30) + " DAY, 'payload" + std::to_string(n) + "')";
        }
        if (::mysql_query(conn.get(), sql.c_str()) != 0)
        {
            std::printf("insert failed at %d: %s\n", base, ::mysql_error(conn.get()));
            return 1;
        }
    }
    std::printf("rows=%d inserted\n", total_rows);

    // 查询：某客户端最近 1 天的任务（典型"查我的历史"）
    const std::string query =
        "SELECT id FROM bench_task WHERE client_id=42 AND created_at > NOW() - INTERVAL 1 DAY";
    const int iters = 50;

    std::string err;
    const long long rows_before = ExplainRows(pool, query, err);
    const double ms_before = AvgMs(pool, query, iters);

    if (!pool.Exec("ALTER TABLE bench_task ADD KEY idx_client_time (client_id, created_at)"))
    {
        std::printf("add index failed\n");
        return 1;
    }

    const long long rows_after = ExplainRows(pool, query, err);
    const double ms_after = AvgMs(pool, query, iters);

    std::printf("\n=== 结果（%d 行）===\n", total_rows);
    std::printf("无索引: EXPLAIN rows=%-8lld avg=%.3f ms\n", rows_before, ms_before);
    std::printf("有索引: EXPLAIN rows=%-8lld avg=%.3f ms\n", rows_after, ms_after);
    if (ms_before > 0 && ms_after >= 0)
    {
        std::printf("延迟下降: %.1f%%\n", (ms_before - ms_after) / ms_before * 100.0);
    }
    return 0;
}
