#!/usr/bin/env python3
"""验证"跨网关重试幂等"：同一 (clientId, originalId) 在 TTL 内复用同一个 new_id。

场景：
  客户端同一条连接上，用同一个 request_id 发两次请求
  → 网关应复用同一个 new_id
  → 后端 TryClaim 识别为同一任务，第二次直接返回结果（不重复执行）
  → DB 里只有一行、attempt=1、会话计数=1

用法：python3 tests/e2e_gateway_idem.py [gateway_port] [mysql-password]
"""

import socket
import struct
import subprocess
import sys
import time

TYPE_TASK_REQ = 0x04
TYPE_TASK_RESP = 0x05
TYPE_TASK_FAILED = 0x06


def frame(t, i, payload: bytes) -> bytes:
    body = struct.pack("!HI", t, i) + payload
    return struct.pack("!I", len(body)) + body


def recv_frame(s):
    hdr = b""
    while len(hdr) < 4:
        c = s.recv(4 - len(hdr))
        if not c:
            return None
        hdr += c
    n = struct.unpack("!I", hdr)[0]
    data = b""
    while len(data) < n:
        c = s.recv(n - len(data))
        if not c:
            return None
        data += c
    t, rid = struct.unpack("!HI", data[:6])
    return t, rid, data[6:]


def mysql(pw, sql):
    out = subprocess.run(
        ["mysql", "--no-defaults", "-h", "127.0.0.1", "-u", "agent", f"-p{pw}",
         "agent_runtime", "-N", "-B", "-e", sql],
        capture_output=True, text=True)
    return out.stdout.strip()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9200
    pw = sys.argv[2] if len(sys.argv) > 2 else "agent123"
    failures = []

    def check(cond, label):
        print(("PASS: " if cond else "FAIL: ") + label)
        if not cond:
            failures.append(label)

    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    s.settimeout(10)

    # 同一连接、同一个 request_id，发两次
    s.sendall(frame(TYPE_TASK_REQ, 7001, b"gateway-idem"))
    r1 = recv_frame(s)
    s.sendall(frame(TYPE_TASK_REQ, 7001, b"gateway-idem"))
    r2 = recv_frame(s)
    s.close()

    check(r1 is not None and r1[0] == TYPE_TASK_RESP and b"pong" in r1[2],
          f"第一次成功（{r1[2] if r1 else None!r}）")
    check(r2 is not None and r2[0] == TYPE_TASK_RESP and r2[2] == r1[2],
          f"第二次返回同一结果、未重复执行（{r2[2] if r2 else None!r}）")

    time.sleep(0.4)
    rows = mysql(pw, "SELECT COUNT(*), MAX(attempt) FROM agent_task")
    check(rows == "1\t1", f"DB 只有一行且 attempt=1（实际 {rows!r}）")

    cnt = mysql(pw, "SELECT task_count FROM agent_session ORDER BY session_id DESC LIMIT 1")
    check(cnt == "1", f"会话计数只 +1（实际 {cnt!r}）")

    print()
    if failures:
        print(f"{len(failures)} 个用例失败")
        return 1
    print("全部用例通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
