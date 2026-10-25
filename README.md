# AgentRuntime 网络底座

当前实现为 C++17 / Linux TCP Reactor 库和分布式 TCP 网关。保留 CMake 项目名 EchoNet、库目标 `echonet_core` 和唯一可执行目标 `EchoNetGateway`，不包含可运行的业务后端。

本轮不实现 Agent、LLM、Runtime Worker、任务状态机、调度器、结果持久化或分布式容错。

## 实际模块

- `include/Net/`、`src/Net/`：Socket、Buffer、Connection。
- `include/Reactor/`、`src/Reactor/`：epoll、eventfd、timerfd、EventLoop、Channel、Acceptor。
- `include/ThreadPool/`、`src/ThreadPool/`：I/O 循环线程、工作线程、FIFO 任务队列与完成回调。
- `include/Protocol/`、`src/Protocol/`：协议接口和 MyProtocol 二进制编解码。
- `include/Server/`、`src/Server/`：监听与停服编排。
- `include/DB/`、`src/DB/`：hiredis 连接池；没有 MySQL 依赖。
- `include/Gateway/`、`src/Gateway/`：Redis 服务发现；`include/ServiceRegistry/` 保存节点描述。
- `include/Common/`、`src/Common/`：命令行配置、信号退出桥接。
- `include/Logger/`、`src/Logger/`：异步文件日志；`Metrics/` 保存网络字节计数。
- `src/gateway_main.cpp`：唯一入口；轮询后端、ID 关联、心跳、超时和动态节点维护。

## 依赖与构建

Linux、CMake >= 3.10、支持 C++17 的编译器、POSIX Threads、hiredis 开发包。Redis 是运行期外部服务；不需要 Qt、MySQL、Python 或测试框架。

```sh
sudo apt-get install build-essential cmake libhiredis-dev redis-server
cmake -S /home/whn/AgentRuntime -B /home/whn/AgentRuntime/build -DCMAKE_BUILD_TYPE=Debug
cmake --build /home/whn/AgentRuntime/build -j4
/home/whn/AgentRuntime/build/EchoNetGateway --help
/home/whn/AgentRuntime/build/EchoNetGateway --port=8080 --io=8 --work=4
```

默认端口 8080、I/O 线程 8、工作线程 4。端口范围 1–65535，线程范围 1–1024（安全输入上限，不代表推荐部署值）。拒绝零、负数、非数字、溢出、未知选项、缺少值及位置参数。启动失败返回非零状态。

Redis 地址当前在入口中固定为 `127.0.0.1:6379`，服务名固定为 `backend_service`。没有 `.env` 加载器、配置文件、Redis 认证/TLS 参数或另一种启动模式。

## 协议与后端约定

所有整数采用网络字节序：`body_len:u32 | type:u16 | request_id:u32 | payload`。`body_len` 不包含自身，必须在 6–16777216 字节之间；payload 最大 16777210 字节。编码和解析使用同一上限。支持半包与粘包；非法长度关闭连接。类型 0x01 为心跳请求、0x02 为心跳响应、0x03 为错误响应；其他类型保持透明转发，不引入业务白名单。

后端由外部程序实现并维护 Redis 字符串键：

```text
services:backend_service:<instance_id> = <IPv4>:<port>
```

网关不负责注册或续租。外部后端应自行维护键存活时间，并对心跳请求返回 0x02；响应必须沿原连接回传网关重写后的 request_id。发现线程每 2 秒执行 SCAN/GET，监控线程每 5 秒检查节点变化；这些不是上线时延保证。SCAN 非原子快照，发现失败保留上次成功缓存。

健康后端轮询选择。心跳间隔 5 秒，健康检查间隔 3 秒，15 秒无 pong 标记不健康；新连接有初始宽限。请求约 30 秒超时，后台每 5 秒扫描。无可用后端、后端断连、超时等返回 0x03 错误。请求映射同时校验后端身份；客户端断开、后端断开与停服清理映射。进程内不复用已发出的 ID，超过 UINT32_MAX 后拒绝新请求，需要运维重启；此取舍防止迟到响应关联到新请求。

## 所有权与停服边界

Connection 的 fd、Channel、Buffer 和协议状态由所属 EventLoop 线程操作；工作线程只执行业务回调并异步投递响应。Channel 回调使用弱引用。EventLoop 持有客户端和出站连接，移除连接时关闭 fd，并延迟销毁回调对象。出站连接不使用客户端的 60 秒空闲淘汰，而由心跳健康状态控制路由。

`runInLoop` 返回是否接收回调；循环停止后拒绝外线程新投递，退出前排空已接收回调。同线程仍可执行清理。`closeAllConnections()` 发起关闭后永久停止接收新连接，先前已排队的建连回调也不得重新注册 fd；完成通知只表示关闭请求已发起。后端身份登记同时校验 fd 和 Connection 对象，并在关闭 fd 前撤销，避免 fd 复用误判。对象必须在所有生产者退出后销毁，不能将裸 EventLoop 指针用于销毁后的投递。

Server 为单次启动对象。配置与回调在启动前设置；`start()` 阻塞运行主循环，完整 `stop()`/析构必须在主循环所属线程执行，不是任意线程同步停服 API。正常进程退出使用 SIGINT/SIGTERM，处理器仅设置标志并写 eventfd，由主循环执行停止。后台生产者退出后关闭出站连接，再关闭客户端，等待工作线程与 I/O 收尾，最后 join I/O 线程。业务回调不得无限阻塞，否则停服仍会等待。

日志写入启动工作目录中的 `echonet.log`。网关每 5 秒记录计数和节点状态；没有 HTTP/Prometheus 导出端点，不提供性能或可靠性数据承诺。

## 当前限制与验证边界

- 无后端业务可执行程序；运行网关不代表实现 Runtime。
- 无 TLS、客户端认证、连接数/队列/输出缓冲总量限制；16 MiB 单帧限制不能替代全局背压，不适合直接暴露到不可信公网。
- 同步后端 connect 最多等待约 3 秒；Redis 命令和连接采用约 1 秒超时，退出不是固定毫秒级保证。
- 没有请求重试、幂等、熔断、多网关高可用或崩溃恢复；转发不等于任务调度。
- 仅 TCP IPv4 地址，不支持 DNS/IPv6 注册地址。
- 构建与启动检查不等于端到端转发、故障恢复、压力或并发可靠性验证。本轮不新增任何测试设施。

下一阶段先明确容量/背压和服务边界，再独立设计 Runtime 任务模型、状态机、调度与执行接口；网络层不依赖 AI 业务。
