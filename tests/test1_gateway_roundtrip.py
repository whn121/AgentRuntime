import socket
import struct

HOST, PORT = "127.0.0.1", 8080

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
    body = struct.pack("!HI", 0x10, 101) + payload
    packet = struct.pack("!I", len(body)) + body

    with socket.create_connection((HOST, PORT), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(packet)
        size = struct.unpack("!I", recv_exact(sock, 4))[0]
        if size < 6 or size > 16 * 1024 * 1024:
            raise AssertionError(f"非法响应长度: {size}")
        data = recv_exact(sock, size)

    msg_type, request_id = struct.unpack("!HI", data[:6])
    assert msg_type == 0x10, f"响应类型错误: {msg_type:#x}"
    assert request_id == 101, f"响应 ID 错误: {request_id}"
    assert data[6:] == payload, f"响应内容错误: {data[6:]!r}"
    print("PASS: 网关转发、响应 ID 和内容正确")

if __name__ == "__main__":
    main()
