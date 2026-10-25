#include "Protocol/MyProtocol.h"
#include <netinet/in.h>
#include <stdexcept>
#include "Logger/AsyncLogger.h"


//| 4字节长度(网络字节序) | 2字节类型 | 4字节请求ID | 变长payload |

ParseResult MyProtocol::parse(Buffer& buf)
{

    // 第一步：读 4 字节 body_len
    if (buf.getreadable() < 4) 
    {
        return ParseResult::NEED_MORE;
    }

    uint32_t body_len = 0;
    std::memcpy(&body_len, buf.peek(), 4);
    body_len = ntohl(body_len);

    // 关键：body_len 上限检查，防止恶意超大包
    if (body_len > MAX_BODY_SIZE) 
    {
        LOG_ERROR("MyProtocol: body_len too large: " + std::to_string(body_len));
        // 协议错误：直接关连接，不返回错误响应
        return ParseResult::ERROR;
    }

    // body 至少要有 type(2) + id(4)
    if (body_len < 6) 
    {
        LOG_ERROR("MyProtocol: body_len too small: " + std::to_string(body_len));
        // 协议错误：直接关连接，不返回错误响应
        return ParseResult::ERROR;
    }

    // 第二步：判断完整包是否到达
    if (buf.getreadable() < 4 + body_len) 
    {
        return ParseResult::NEED_MORE;
    }

    // 第三步：完整包已到，开始解析
    buf.moveReadPtr(4);  // 跳过 body_len

    // 读 type(2)
    uint16_t type_raw = 0;
    std::memcpy(&type_raw, buf.peek(), 2);
    type_raw = ntohs(type_raw);
    buf.moveReadPtr(2);

    // 读 id(4)
    uint32_t id = 0;
    std::memcpy(&id, buf.peek(), 4);
    id = ntohl(id);
    buf.moveReadPtr(4);

    // 读 payload
    uint32_t payload_len = body_len - 6;
    std::string payload;
    if (payload_len > 0) 
    {
        payload.assign(buf.peek(), payload_len);
        buf.moveReadPtr(payload_len);
    }

    // 网关对业务消息透明：type 除框架类型外均按原样透传，不做白名单校验
    MyType type = static_cast<MyType>(type_raw);

    // 填充消息
    currentMessage_.type_    = type;
    currentMessage_.id_      = id;
    currentMessage_.payload_ = std::move(payload);


    return ParseResult::OK;
}

std::string MyProtocol::encodeMessage(MyType type, uint32_t id, const std::string &payload)
{
    if (payload.size() > MAX_BODY_SIZE - 6)
        throw std::length_error("MyProtocol payload exceeds maximum body size");
    uint32_t body_len = 6 + static_cast<uint32_t>(payload.size());
    std::string packet;
    packet.reserve (4 + body_len);

    uint32_t net_body_len = htonl (body_len);
    packet.append ((reinterpret_cast<const char*> (&net_body_len)), 4);

    uint16_t vtype = uint16_t (type);
    uint16_t net_type = htons (vtype);
    packet.append ((reinterpret_cast<const char*> (&net_type)), 2);

    uint32_t net_id = htonl (id);
    packet.append ((reinterpret_cast<const char*> (&net_id)), 4);

    packet.append (payload.data(), payload.size());
    
    return packet;
}

std::string MyProtocol::encode(const std::any &message)
{
    auto msg = std::any_cast<MyMessage> (message);
    return encodeMessage(msg.type_, msg.id_, msg.payload_);
}

std::any MyProtocol::getMessage()
{
    return currentMessage_;
}

void MyProtocol::reset()
{
    currentMessage_ = MyMessage();
}

std::optional<std::any> MyProtocol::getErrorResponse()
{
    // 协议错误：直接关连接，不返回错误响应
    return std::nullopt;
}

