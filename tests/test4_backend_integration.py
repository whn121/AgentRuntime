#!/usr/bin/env python3
"""Backend 集成测试：验证 RUNTIME_TASK_REQ 的完整链路。

链路：client → Gateway → Backend → RuntimeEngine → 模型服务（需一个兼容
OpenAI Chat Completions 的服务在 8081）→ 原路返回。

断言的是"契约"而不是模型内容：
  - 响应 request id 与请求一致
  - 响应类型是 0x05（成功）或 0x06（失败，属于合法结果）
  - 成功时 payload 非空

协议：4B body 长度(大端) + 2B 类型 + 4B 请求ID + payload
"""
import socket
import struct
import sys

GATEWAY_HOST = "127.0.0.1"
GATEWAY_PORT = 8080
TIMEOUT = 10

HEADER_SIZE = 10
RUNTIME_TASK_REQ = 0x04
RUNTIME_TASK_RESP = 0x05
RUNTIME_TASK_FAILED = 0x06
ERROR_RESP = 0x03
MAX_BODY = 16 * 1024 * 1024


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
    body_len = struct.unpack("!I", recv_exact(sock, 4))[0]

    if body_len < 6 or body_len > MAX_BODY:
        raise ValueError(f"非法响应帧长度：{body_len}")

    body = recv_exact(sock, body_len)
    msg_type, request_id = struct.unpack("!HI", body[:6])
    payload = body[6:]
    return msg_type, request_id, payload


def check_response(response, expected_id):
    msg_type, request_id, payload = response

    if msg_type == ERROR_RESP:
        raise AssertionError(
            f"Gateway 返回错误：request_id={request_id}, payload={payload!r}"
        )

    assert request_id == expected_id, (
        f"request ID 不匹配：期望 {expected_id}，实际 {request_id}"
    )

    if msg_type == RUNTIME_TASK_FAILED:
        # 失败也是合法结果（例如后端不可用），但必须带错误信息
        assert payload, "失败响应必须带错误信息"
        return "failed"

    assert msg_type == RUNTIME_TASK_RESP, f"响应类型非法：{msg_type:#x}"
    assert payload, "成功响应 payload 不能为空"
    return "ok"


def main():
    print(f"连接现有 Gateway：{GATEWAY_HOST}:{GATEWAY_PORT}")

    try:
        with socket.create_connection(
            (GATEWAY_HOST, GATEWAY_PORT), timeout=TIMEOUT
        ) as sock:
            sock.settimeout(TIMEOUT)

            # 1. 单帧往返
            payload = b"test4-existing-services\x00\x01\xff"
            sock.sendall(encode_frame(RUNTIME_TASK_REQ, 456, payload))
            result = check_response(recv_frame(sock), 456)
            print(f"PASS: 单帧往返正确（{result}）")

            # 2. 一次发送两帧，按 request ID 校验响应，不假设返回顺序
            payload1 = b"pipeline-A"
            payload2 = b"pipeline-B\x00\xfe"
            sock.sendall(
                encode_frame(RUNTIME_TASK_REQ, 457, payload1)
                + encode_frame(RUNTIME_TASK_REQ, 458, payload2)
            )

            responses = [recv_frame(sock), recv_frame(sock)]
            by_id = {response[1]: response for response in responses}

            assert set(by_id) == {457, 458}, (
                f"连续帧响应 ID 不正确：{list(by_id.keys())}"
            )
            check_response(by_id[457], 457)
            check_response(by_id[458], 458)
            print("PASS: 连续发送两帧，两条响应的 ID 均正确")

        print("PASS: 测试完成")
        return 0

    except Exception as exc:
        print(f"FAIL: {type(exc).__name__}: {exc}", file=sys.stderr)
        print(
            "请确认 Gateway 监听 8080、Redis 中有可用 Backend、"
            "且 8081 有兼容 OpenAI Chat Completions 的服务。",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
