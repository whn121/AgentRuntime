#pragma once

#include <memory>
#include <chrono>
#include "Net/Connection.h"


// 待响应请求:记录哪个客户端在等哪个响应 让消息双方对应上不会收到别人的
struct PendingRequest
{
    std::weak_ptr<Connection> client_conn; // 客户端连接 (弱引用)
    std::weak_ptr<Connection> backend_conn;
    uint32_t original_id; // 客户端原始requestId
    std::chrono::steady_clock::time_point created_at;
};