#!/usr/bin/env python3
"""验证幂等状态机 + 会话计数 + 缓存命中（直连后端）。

覆盖：
1) 首次提交执行，重复提交返回同一结果（幂等重放，不重复执行）
2) 会话计数只 +1
3) Failed 且未超上限 → 允许重领（Retry），attempt 递增
4) Failed 且已超上限 → RETRY_EXCEEDED
5) 查询走缓存（第二次更快）、不存在的 id 返回 NOT_FOUND

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


def submit(port, rid, payload, timeout=10):
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    s.settimeout(timeout)
    s.sendall(frame(TYPE_TASK_REQ, rid, payload))
    got = recv_frame(s)
    s.close()
    return got


def query(port, rid):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    s.settimeout(5)
    s.sendall(frame(TYPE_QUERY_REQ, 9000, str(rid).encode()))
    t0 = time.perf_counter()
    got = recv_frame(s)
    dt = (time.perf_counter() - t0) * 1000.0
    s.close()
    return got, dt


def mysql(pw, sql):
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

    # 1) 首次提交 + 重复提交（幂等重放）
    first = submit(port, 6001, b"idem-test")
    second = submit(port, 6001, b"idem-test")
    check(first is not None and first[0] == TYPE_TASK_RESP and b"pong" in first[2],
          f"首次提交成功（{first[2] if first else None!r}）")
    check(second is not None and second[0] == TYPE_TASK_RESP and second[2] == first[2],
          f"重复提交返回同一结果（{second[2] if second else None!r}）")

    time.sleep(0.3)

    # 2) 会话计数只 +1
    cnt = mysql(pw, "SELECT task_count FROM agent_session ORDER BY session_id DESC LIMIT 1")
    check(cnt == "1", f"agent_session.task_count == 1（实际 {cnt!r}）")

    # 3) Failed 未超上限：手动把 6002 置为 Failed、attempt=1
    mysql(pw, "INSERT INTO agent_task (request_id, client_id, status, attempt) VALUES (6002, 1, 3, 1) "
              "ON DUPLICATE KEY UPDATE status=3, attempt=1")
    r3 = submit(port, 6002, b"retry-test")
    check(r3 is not None and r3[0] == TYPE_TASK_RESP,
          f"Failed 未超上限 → 重新执行（{r3[2] if r3 else None!r}）")
    time.sleep(0.3)
    att = mysql(pw, "SELECT attempt FROM agent_task WHERE request_id=6002")
    check(att == "2", f"attempt 递增到 2（实际 {att!r}）")

    # 4) Failed 已超上限：6003 attempt=3
    mysql(pw, "INSERT INTO agent_task (request_id, client_id, status, attempt) VALUES (6003, 1, 3, 3) "
              "ON DUPLICATE KEY UPDATE status=3, attempt=3")
    r4 = submit(port, 6003, b"exceed-test")
    check(r4 is not None and r4[0] == TYPE_TASK_FAILED and b"RETRY_EXCEEDED" in r4[2],
          f"超过重试上限 → RETRY_EXCEEDED（{r4[2] if r4 else None!r}）")

    # 5) 缓存：同一 id 查两次，第二次更快
    _, t1 = query(port, 6001)
    _, t2 = query(port, 6001)
    check(t2 < t1 * 0.8, f"第二次查询更快（{t1:.2f}ms → {t2:.2f}ms）")

    # 6) 不存在的 id
    got, _ = query(port, 999999)
    check(got is not None and b"NOT_FOUND" in got[2], "不存在的 id 返回 NOT_FOUND")

    print()
    if failures:
        print(f"{len(failures)} 个用例失败")
        return 1
    print("全部用例通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
