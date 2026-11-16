#!/usr/bin/env python3
"""验证：经网关提交 + 经网关查询（开启 --query-secret）仍然成立。

关键点：token 与 DB 主键都绑定"客户端原始 id"（网关透传 o:<id>|），
所以即使换一条连接（clientId 变了）也能用同一个 id + token 查到结果。

用法：python3 tests/e2e_gateway_query.py [gateway_port] [mysql-password]
"""

import socket
import struct
import subprocess
import sys
import time

TYPE_TASK_REQ = 0x04
TYPE_TASK_RESP = 0x05
TYPE_TASK_FAILED = 0x06
TYPE_QUERY_REQ = 0x07
TYPE_QUERY_RESP = 0x08


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

    # 1) 经网关提交，拿 TASK_ACCEPTED 里的 token
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    s.settimeout(10)
    s.sendall(frame(TYPE_TASK_REQ, 7001, b"query-secret-test"))

    token = None
    final = None
    while True:
        f = recv_frame(s)
        if f is None:
            break
        t, rid, payload = f
        if t == 0x09:  # TASK_ACCEPTED
            txt = payload.decode("utf-8", "ignore")
            if "token=" in txt:
                token = txt.split("token=", 1)[1].strip()
        elif t in (TYPE_TASK_RESP, TYPE_TASK_FAILED):
            final = f
            break
    s.close()

    check(token is not None and len(token) > 0, f"拿到受理回执里的 token（{token!r}）")
    check(final is not None and final[0] == TYPE_TASK_RESP, f"任务完成（{final[2] if final else None!r}）")

    time.sleep(0.3)

    # 2) 换一条连接（clientId 变了）经网关查询
    s2 = socket.create_connection(("127.0.0.1", port), timeout=10)
    s2.settimeout(10)
    s2.sendall(frame(TYPE_QUERY_REQ, 9000, f"7001 {token}".encode()))
    q = recv_frame(s2)
    s2.close()

    check(q is not None and q[0] == TYPE_QUERY_RESP and q[2].startswith(b"OK"),
          f"换连接后查询成功（{q[2] if q else None!r}）")

    # 3) 错误 token 必须被拒
    s3 = socket.create_connection(("127.0.0.1", port), timeout=10)
    s3.settimeout(10)
    s3.sendall(frame(TYPE_QUERY_REQ, 9001, b"7001 deadbeefdeadbeefdeadbeefdeadbeef"))
    q2 = recv_frame(s3)
    s3.close()
    check(q2 is not None and b"FORBIDDEN" in q2[2], f"错误 token 被拒（{q2[2] if q2 else None!r}）")

    # 4) DB 主键应为客户端原始 id
    row = mysql(pw, "SELECT COUNT(*) FROM agent_task WHERE request_id=7001")
    check(row == "1", f"DB 主键用客户端 id 7001（实际 {row!r}）")

    print()
    if failures:
        print(f"{len(failures)} 个用例失败")
        return 1
    print("全部用例通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
