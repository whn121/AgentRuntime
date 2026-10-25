#!/usr/bin/env python3
import socket
import struct
import sys

GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 8080
TIMEOUT = 5

# 协议格式：4 字节 body 长度 + 2 字节类型 + 4 字节 request ID + payload
HEADER_SIZE = 10
ERROR_RESP = 0x03


def encode_frame(msg_type, request_id, payload):
    body = struct.pack("!HI", msg_type, request_id) + payload
    return struct.pack("!I", len(body)) + body


def recv_exact(sock, size):
    chunks = bytearray()
    while len(chunks) < size:
        data = sock.recv(size - len(chunks))
        if not data:
            raise ConnectionError(
                f"连接提前关闭：需要 {size} 字节，实际收到 {len(chunks)} 字节"
            )
        chunks.extend(data)
    return bytes(chunks)


def recv_frame(sock):
    length_bytes = recv_exact(sock, 4)
    body_len = struct.unpack("!I", length_bytes)[0]

    if body_len < 6 or body_len > 16 * 1024 * 1024:
        raise ValueError(f"非法响应帧长度：{body_len}")

    body = recv_exact(sock, body_len)
    msg_type, request_id = struct.unpack("!HI", body[:6])
    payload = body[6:]
    return msg_type, request_id, payload


def check_response(response, expected_id, expected_payload):
    msg_type, request_id, payload = response

    if msg_type == ERROR_RESP:
        raise AssertionError(
            f"Gateway 返回错误：request_id={request_id}, payload={payload!r}"
        )

    assert request_id == expected_id, (
        f"request ID 不匹配：期望 {expected_id}，实际 {request_id}"
    )
    assert payload == expected_payload, (
        f"payload 不匹配：期望 {expected_payload!r}，实际 {payload!r}"
    )


def main():
    print(f"连接现有 Gateway：{GATEWAY_HOST}:{GATEWAY_PORT}")

    try:
        with socket.create_connection(
            (GATEWAY_HOST, GATEWAY_PORT), timeout=TIMEOUT
        ) as sock:
            sock.settimeout(TIMEOUT)

            # 1. 普通业务消息往返，验证二进制 payload
            payload = b"test4-existing-services\x00\x01\xff"
            sock.sendall(encode_frame(0x100, 456, payload))

            response = recv_frame(sock)
            check_response(response, 456, payload)
            print("PASS: 现有服务业务消息往返正确")

            # 2. 一次发送两帧，按 request ID 校验响应，不假设返回顺序
            payload1 = b"pipeline-A"
            payload2 = b"pipeline-B\\x00\\xfe"

            sock.sendall(
                encode_frame(0x101, 457, payload1)
                + encode_frame(0x102, 458, payload2)
            )

            responses = [recv_frame(sock), recv_frame(sock)]
            by_id = {response[1]: response for response in responses}

            assert set(by_id) == {457, 458}, (
                f"连续帧响应 ID 不正确：{list(by_id.keys())}"
            )
            check_response(by_id[457], 457, payload1)
            check_response(by_id[458], 458, payload2)
            print("PASS: 连续发送两帧，两条响应的 ID 和 payload 均正确")

        print("PASS: 测试完成，未启动或停止任何服务器")
        return 0

    except Exception as exc:
        print(f"FAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
        print(
            "请确认 Gateway 正在监听 8080，"
            "且 Redis 服务发现中有可用 Backend。",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
