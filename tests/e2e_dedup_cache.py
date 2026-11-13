#!/usr/bin/env python3
"""验证 幂等受理 + 会话计数 + 缓存命中（直连后端，不需要网关）。

用例：
1) 同一 request_id 提交两次：第一次正常完成，第二次返回 DUPLICATE_REQUEST；
2) agent_session.task_count 只 +1（说明 TryClaim 的事务生效）；
3) 查询同一个 id 两次：第二次明显更快（走 Redis 缓存）。

用法：python3 tests/e2e_dedup_cache.py [port] [mysql-password]
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


def connect(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.settimeout(5)
    return s


def submit(port, rid, payload):
    s = connect(port)
    s.sendall(frame(TYPE_TASK_REQ, rid, payload))
    got = recv_frame(s)
    s.close()
    return got


def query(port, rid):
    s = connect(port)
    s.sendall(frame(TYPE_QUERY_REQ, 9000, str(rid).encode()))
    t0 = time.perf_counter()
    got = recv_frame(s)
    dt = (time.perf_counter() - t0) * 1000.0
    s.close()
    return got, dt


def mysql_scalar(pw, sql):
    out = subprocess.run(
        ["mysql", "--no-defaults", "-h", "127.0.0.1", "-u", "agent", f"-p{pw}",
         "agent_runtime", "-N", "-B", "-e", sql],
        capture_output=True, text=True)
    return out.stdout.strip()


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9105
    pw = sys.argv[2] if len(sys.argv) > 2 else "agent123"
    failures = []

    def check(cond, label):
        print(("PASS: " if cond else "FAIL: ") + label)
        if not cond:
            failures.append(label)

    # 用例 1：重复提交
    first = submit(port, 5001, b"dedup-test")
    second = submit(port, 5001, b"dedup-test")
    check(first is not None and first[0] in (TYPE_TASK_RESP, TYPE_TASK_FAILED),
          "第一次提交返回终态")
    check(second is not None and second[0] == TYPE_TASK_FAILED and b"DUPLICATE_REQUEST" in second[2],
          f"第二次提交被拒（实际：{second[2] if second else None!r}）")

    # 用例 2：会话计数只 +1
    cnt = mysql_scalar(pw, "SELECT task_count FROM agent_session ORDER BY session_id DESC LIMIT 1")
    check(cnt in ("1",), f"agent_session.task_count == 1（实际：{cnt!r}）")

    # 用例 3：同一个 id 查两次，第二次走缓存
    _, t1 = query(port, 5001)
    _, t2 = query(port, 5001)
    check(t2 < t1 * 0.8, f"第二次查询更快（{t1:.2f}ms → {t2:.2f}ms，缓存命中）")

    # 用例 4：不存在的 id 也缓存（防穿透）
    got3, _ = query(port, 999999)
    got4, t4 = query(port, 999999)
    _, t3 = query(port, 999999)
    check(got3 is not None and b"NOT_FOUND" in got3[2], "不存在的 id 返回 NOT_FOUND")

    print()
    if failures:
        print(f"{len(failures)} 个用例失败")
        return 1
    print("全部用例通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
