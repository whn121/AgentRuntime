#!/usr/bin/env python3
"""任务持久化的端到端验证（不需要网关，直连后端）。

覆盖三件事：
1) 正常完成的任务：提交 -> 拿到响应 -> 重连后按 request_id 查到 SUCCEEDED + 结果；
2) 客户端提交后立刻断开：任务被取消 -> 查到 CANCELLED（不是丢了）；
3) 查不存在的 request_id：返回 NOT_FOUND（幂等查询不虚构结果）。

用法：
    python3 tests/e2e_persist.py [port] [mysql_host] [mysql_port] [user] [password] [db]
默认：9101 / 127.0.0.1 / 3306 / agent / agent123 / agent_runtime
"""

import socket
import struct
import sys
import time

TYPE_HEARTBEAT_REQ = 0x01
TYPE_HEARTBEAT_RESP = 0x02
TYPE_RUNTIME_TASK_REQ = 0x04
TYPE_RUNTIME_TASK_RESP = 0x05
TYPE_RUNTIME_TASK_FAILED = 0x06
TYPE_TASK_QUERY_REQ = 0x07
TYPE_TASK_QUERY_RESP = 0x08

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


def query(port: int, request_id: int, label: str) -> str:
    s = connect(port)
    s.sendall(frame(TYPE_TASK_QUERY_REQ, 9000, str(request_id).encode()))
    got = recv_frame(s)
    s.close()
    if got is None:
        raise AssertionError(f"[{label}] 查询没有响应")
    msg_type, _, payload = got
    assert msg_type == TYPE_TASK_QUERY_RESP, f"[{label}] 期望 TASK_QUERY_RESP，收到 0x{msg_type:02x}"
    return payload.decode(errors="replace")


def main() -> int:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9101
    failures = []

    def check(cond, label):
        print(("PASS: " if cond else "FAIL: ") + label)
        if not cond:
            failures.append(label)

    # ── 用例 1：正常完成 + 重连查询 ──
    s = connect(port)
    s.sendall(frame(TYPE_RUNTIME_TASK_REQ, 4242, b"persist-me"))
    got = recv_frame(s)
    s.close()
    check(got is not None and got[0] in (TYPE_RUNTIME_TASK_RESP, TYPE_RUNTIME_TASK_FAILED),
          "提交任务并收到终态响应")

    body = ""
    for _ in range(50):  # 异步写，最多等 5 秒
        body = query(port, 4242, "case1")
        if body != "NOT_FOUND":
            break
        time.sleep(0.1)
    check(body.startswith(f"OK {STATUS_SUCCEEDED}"), f"重连后按 request_id 查到成功结果（实际：{body[:40]!r}）")

    # ── 用例 2：提交后立刻断开 → "待调度"的任务被取消并落库 ──
    # 语义提醒：cancelByClient 只取消还在排队的任务，**运行中的不打断**
    # （见 README 的取消语义）。所以要一次压进多个慢请求，
    # 让后面的任务留在 pending 队列里，断开后才测得到取消路径。
    s = connect(port)
    for req_id in range(4243, 4249):
        s.sendall(frame(TYPE_RUNTIME_TASK_REQ, req_id, b"slow-task"))
    s.close()          # 立刻断开：在跑的继续，排队的被取消

    cancelled_seen = False
    first_terminal = None
    for req_id in range(4245, 4249):   # 前两个大概率已在跑，检查后面几个
        body2 = ""
        for _ in range(30):
            body2 = query(port, req_id, "case2")
            if body2 != "NOT_FOUND":
                break
            time.sleep(0.1)
        if first_terminal is None:
            first_terminal = (req_id, body2)
        if body2.startswith(f"OK {STATUS_CANCELLED}"):
            cancelled_seen = True
    check(cancelled_seen, f"断开后排队中的任务被标记为 CANCELLED（样例：{first_terminal}）")

    # ── 用例 3：查不存在的 id ──
    body3 = query(port, 999999, "case3")
    check(body3 == "NOT_FOUND", f"不存在的 request_id 返回 NOT_FOUND（实际：{body3!r}）")

    # ── 用例 4：DB 关闭时不崩，明确告知 ──
    print("提示：DB_DISABLED 分支需要不带 --mysql-host 启动后端时验证")

    print()
    if failures:
        print(f"{len(failures)} 个用例失败")
        return 1
    print("全部用例通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
