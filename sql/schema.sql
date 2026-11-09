-- AgentRuntime 任务持久化表
--
-- 为什么需要：
--   Agent/LLM 任务可能跑几十秒。客户端断开后运行时会把任务取消，结果就丢了；
--   客户端超时重试又会重复消耗模型。落库之后可以按 request_id 查结果（幂等）。
--
-- 索引说明：
--   PRIMARY KEY (request_id)           —— 幂等键：同一请求只会有一行
--   KEY idx_client_time (client_id, created_at)
--                                      —— 支撑"某客户端最近 N 条"，
--                                         走覆盖索引后不需要回表扫全表
--
-- 状态：0 pending / 1 running / 2 succeeded / 3 failed / 4 cancelled

CREATE TABLE IF NOT EXISTS agent_task (
    request_id BIGINT UNSIGNED NOT NULL,
    client_id  BIGINT UNSIGNED NOT NULL,
    status     TINYINT NOT NULL,
    result     MEDIUMTEXT NULL,
    error      VARCHAR(128) NULL,
    created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (request_id),
    KEY idx_client_time (client_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
