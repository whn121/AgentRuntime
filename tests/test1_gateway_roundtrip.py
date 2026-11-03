#!/usr/bin/env python3
"""网关转发测试（不依赖模型服务，也不依赖心跳）。

为什么不用 HEARTBEAT_REQ：
    Gateway 用 HEARTBEAT_RESP 识别"自己对后端探活的回包"（见 gateway_main.cpp），
    客户端发心跳时后端回的那一帧会被网关当成探活结果吞掉，客户端永远收不到。
    所以这里改用一个后端明确不支持的类型，验证的是：
        client → Gateway → Backend → （后端回 ERROR_RESP）→ Gateway → client
    这条完整链路，同时证明请求 ID 被原样带回。

协议：4B body 长度(大端) + 2B 类型 + 4B 请求ID + payload
"""
import socket
import struct
import sys

HOST, PORT = "127.0.0.1", 8080
TIMEOUT = 5
UNSUPPORTED_TYPE = 0x7F
ERROR_RESP = 0x03
MAX_BODY = 16 * 1024 * 1024


def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        part = sock.recv(n - len(data))
        if not part:
            raise RuntimeError("连接提前关闭")
        data += part
    return data


def main():
    payload = b"phase1-roundtrip"
    body = struct.pack("!HI", UNSUPPORTED_TYPE, 101) + payload
    packet = struct.pack("!I", len(body)) + body

    with socket.create_connection((HOST, PORT), timeout=TIMEOUT) as sock:
        sock.settimeout(TIMEOUT)
        sock.sendall(packet)
        size = struct.unpack("!I", recv_exact(sock, 4))[0]
        if size < 6 or size > MAX_BODY:
            raise AssertionError(f"非法响应长度: {size}")
        data = recv_exact(sock, size)

    msg_type, request_id = struct.unpack("!HI", data[:6])
    assert msg_type == ERROR_RESP, f"响应类型错误: {msg_type:#x}"
    assert request_id == 101, f"响应 ID 错误: {request_id}"
    assert data[6:] == b"UNSUPPORTED_MESSAGE_TYPE", f"响应内容错误: {data[6:]!r}"
    print("PASS: 网关转发、后端错误回包、请求 ID 原样带回，均正确")
    return 0


if __name__ == "__main__":
    sys.exit(main())
