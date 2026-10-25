#pragma once

#include "Protocol/Protocol.h"
#include <string>
#include <any>
#include "Net/Buffer.h"
#include <netinet/in.h> //二进制转换


// 二进制协议格式：| 4字节长度 | 2字节类型 | 4字节请求ID | 变长payload |

// 网关框架级消息类型；0x04 起为业务层自定义类型，由网关透明转发
enum class MyType : uint16_t
{
    HEARTBEAT_REQ  = 0x01,   // 网关 -> 后端，健康探测
    HEARTBEAT_RESP = 0x02,   // 后端 -> 网关，健康应答
    ERROR_RESP     = 0x03,    // 网关 -> 客户端，超时 / 无可用后端
    RUNTIME_TASK_REQ    = 0x04,   
    RUNTIME_TASK_RESP   = 0x05,
    RUNTIME_TASK_FAILED = 0x06   //runtime执行失败
};

struct MyMessage
{
    MyType type_{MyType::ERROR_RESP};
    uint32_t id_{0};
    std::string payload_; 
};

class MyProtocol : public Protocol
{
public:
    ParseResult parse (Buffer& bufffer) override; //解析
    std::string encode (const std::any& message) override; //序列化二进制
    std::any getMessage() override; //获得最终消息
    void reset() override; //重置状态

    std::optional<std::any> getErrorResponse() override; //错误处理

    //满足虚继承参数不变
    // 辅助编码函数:根据类型和payload生成完整二进制消息
    static std::string encodeMessage (MyType type, uint32_t id, const std::string& payload);

    static constexpr uint32_t MAX_BODY_SIZE = 16 * 1024 * 1024;

private:
    MyMessage currentMessage_; // 最新消息

};