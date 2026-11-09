#!/usr/bin/env python3
"""任务持久化 + 查询令牌的端到端验证（直连后端，不需要网关）。

覆盖：
1) 提交 -> 收到受理回执（带查询令牌）-> 收到终态 -> 重连后按 request_id + 令牌查到结果；
2) 提交后立刻断开：排队中的任务被取消 -> 用回执里的令牌仍能查到 CANCELLED；
3) 启用 --query-secret 时：不带令牌 / 错令牌 / 拿 A 的令牌查 B 都被拒（FORBIDDEN）；
   查不存在的 id 也返回 FORBIDDEN（不泄漏"这个 id 存不存在"）；
4) 未启用鉴权时：查不存在的 id 返回 NOT_FOUND。

用法：
    python3 tests/e2e_persist.py [port]
"""

import socket
import struct
import sys
import time

TYPE_RUNTIME_TASK_REQ = 0x04
TYPE_RUNTIME_TASK_RESP = 0x05
TYPE_RUNTIME_TASK_FAILED = 0x06
TYPE_TASK_QUERY_REQ = 0x07
TYPE_TASK_QUERY_RESP = 0x08
TYPE_TASK_ACCEPTED = 0x09

STATUS_SUCCEEDED = 2
STATUS_CANCELLED = 4


def frame(msg_type: int, req_id: int, payload: bytes) -> bytes:
    body = struct.pack("!HI", msg_type, req_id) + payload
    return struct.pack("!I", len(body)) + body


def recv_frame(sock: socket.socket):
    hdr = b""
    while len(hdr) < 4:
        chunk = sock.recv(4 - len(hdr))
        if not chunk:
            return None
        hdr += chunk
    n = struct.unpack("!I", hdr)[0]
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    msg_type, req_id = struct.unpack("!HI", data[:6])
    return msg_type, req_id, data[6:]


def connect(port: int) -> socket.socket:
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.settimeout(5)
    return s


def extract_token(payload: bytes) -> str:
    text = payload.decode(errors="replace")
    if "|token=" in text:
        return text.split("|token=", 1)[1]
    return ""


def query(port: int, request_id: int, token: str = "") -> str:
    s = connect(port)
    payload = str(request_id) + ((" " + token) if token else "")
    s.sendall(frame(TYPE_TASK_QUERY_REQ, 9000, payload.encode()))
    got = recv_frame(s)
    s.close()
    if got is None:
        return "<no-response>"
    return got[2].decode(errors="replace")


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9101
    failures = []

    def check(cond, label):
        print(("PASS: " if cond else "FAIL: ") + label)
        if not cond:
            failures.append(label)

    # ── 用例 1：受理回执 -> 终态 -> 重连查询 ──
    s = connect(port)
    s.sendall(frame(TYPE_RUNTIME_TASK_REQ, 4242, b"persist-me"))
    first = recv_frame(s)

    token = ""
    final = first
    if first is not None and first[0] == TYPE_TASK_ACCEPTED:
        token = extract_token(first[2])
        final = recv_frame(s)      # 终态
    s.close()

    check(final is not None and final[0] in (TYPE_RUNTIME_TASK_RESP, TYPE_RUNTIME_TASK_FAILED),
          "收到终态响应")
    print(f"  查询令牌：{token or '<未启用鉴权>'}")

    body = ""
    for _ in range(50):            # 异步写，最多等 5 秒
        body = query(port, 4242, token)
        if body != "NOT_FOUND":
            break
        time.sleep(0.1)
    check(body.startswith(f"OK {STATUS_SUCCEEDED}"),
          f"重连后查到成功结果（实际：{body[:40]!r}）")

    # ── 用例 2：断开后，排队任务被取消，靠回执令牌仍可查 ──
    s = connect(port)
    for req_id in range(4243, 4249):
        s.sendall(frame(TYPE_RUNTIME_TASK_REQ, req_id, b"slow-task"))
    tokens = {}
    for _ in range(6):
        ack = recv_frame(s)
        if ack is not None and ack[0] == TYPE_TASK_ACCEPTED:
            tokens[ack[1]] = extract_token(ack[2])
    s.close()                      # 断开：在跑的继续，排队的被取消

    cancelled_seen = False
    sample = None
    for req_id in range(4245, 4249):
        tok = tokens.get(req_id, "")
        body2 = ""
        for _ in range(30):
            body2 = query(port, req_id, tok)
            if body2 != "NOT_FOUND":
                break
            time.sleep(0.1)
        if sample is None:
            sample = (req_id, body2)
        if body2.startswith(f"OK {STATUS_CANCELLED}"):
            cancelled_seen = True
    check(cancelled_seen, f"断开后排队任务标记为 CANCELLED（样例：{sample}）")

    # ── 用例 3：鉴权 ──
    if token:
        check(query(port, 4242) == "FORBIDDEN", "不带令牌被拒")
        check(query(port, 4242, "0" * 32) == "FORBIDDEN", "错误令牌被拒")
        check(query(port, 4243, token) == "FORBIDDEN", "拿 A 的令牌查 B 被拒")
        check(query(port, 999999, token) == "FORBIDDEN",
              "查不存在的 id 也返回 FORBIDDEN（不泄漏存在性）")
    else:
        check(query(port, 999999) == "NOT_FOUND", "未启用鉴权时返回 NOT_FOUND")

    print()
    if failures:
        print(f"{len(failures)} 个用例失败")
        return 1
    print("全部用例通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
