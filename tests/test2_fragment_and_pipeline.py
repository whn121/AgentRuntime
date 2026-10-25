import socket
import struct

HOST, PORT = "127.0.0.1", 8080

def frame(msg_type, request_id, payload):
    body = struct.pack("!HI", msg_type, request_id) + payload
    return struct.pack("!I", len(body)) + body

def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        part = sock.recv(n - len(data))
        if not part:
            raise RuntimeError("连接提前关闭")
        data += part
    return data

def recv_frame(sock):
    size = struct.unpack("!I", recv_exact(sock, 4))[0]
    if size < 6 or size > 16 * 1024 * 1024:
        raise AssertionError(f"非法响应长度: {size}")
    body = recv_exact(sock, size)
    msg_type, request_id = struct.unpack("!HI", body[:6])
    return msg_type, request_id, body[6:]

def main():
    first = frame(0x11, 201, b"first")
    second = frame(0x12, 202, b"second")

    with socket.create_connection((HOST, PORT), timeout=5) as sock:
        sock.settimeout(5)
        # 第一帧拆成多个 TCP 写入；随后与第二帧一起发送。
        sock.sendall(first[:2])
        sock.sendall(first[2:7])
        sock.sendall(first[7:] + second)

        r1 = recv_frame(sock)
        r2 = recv_frame(sock)

    assert r1 == (0x11, 201, b"first"), f"第一帧错误: {r1!r}"
    assert r2 == (0x12, 202, b"second"), f"第二帧错误: {r2!r}"
    print("PASS: TCP 半包和粘包")

if __name__ == "__main__":
    main()
