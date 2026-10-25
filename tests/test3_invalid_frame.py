import socket
import struct

HOST, PORT = "127.0.0.1", 8080

def main():
    with socket.create_connection((HOST, PORT), timeout=5) as sock:
        sock.settimeout(4)
        # 协议长度字段小于固定头部长度 6，预期被识别为非法帧。
        sock.sendall(struct.pack("!I", 5) + b"\x00" * 5)
        data = sock.recv(1)

    assert data == b"", (
        f"预期网关关闭非法协议连接，但收到数据: {data!r}"
    )
    print("PASS: 非法协议帧触发连接关闭")

if __name__ == "__main__":
    main()
