# AgentRuntime

C++17 实现的 Agent 执行运行时：在 Reactor 网络底座上构建 Gateway–Backend–Runtime 执行链路，处理并发连接、任务调度、模型 HTTP 调用，并在服务关闭或客户端断开时对在途任务做协作式取消。

## 核心特性

- **网络层**：Reactor + epoll + eventfd + timerfd，IO/Worker 两级线程池，主 EventLoop 负责 accept，多个子 EventLoop 各自绑定线程，`runInLoop` 保证跨线程投递时的线程归属。
- **网关**：Redis 服务注册发现（TTL 10s，心跳 3s），轮询负载均衡，请求 ID 重写 + pending 表关联，心跳与健康检查，超时清理。
- **调度**：`Scheduler` 按客户端轮转 + 单客户端配额 256，避免一个连接独占队列；`RuntimeEngine` 有界待调度队列（默认 1024）。
- **取消**：`RuntimeEngine::stop()` 通过 `CancellationToken` 传播至 `LlamaClient`，基于 libcurl `XFERINFOFUNCTION` 周期性检查，服务关闭时在途模型调用从等待 `CURLOPT_TIMEOUT_MS`（120s）缩短至实测 **108ms** 退出。客户端断开时，**所有关闭路径**（`close()` / 读错误 / 写错误 / 协议错误 / `forceCloseOnLoopStop`）统一在 `handleClose` 的 CAS 成功后通过 `notifyCancelOnce()` 触发一次 `cancelHook_`，使用不可变的 `clientId_`（构造时的 fd），不依赖 `finishClose` 之后的 `afd_`。
- **关闭路径**：统一 `closeCallbackPending_` 认领入口，停服时连接清理恰好一次；`EventLoop` 退出时对残余连接强制关闭兜底；对单连接回调异常隔离，防止 `bad_alloc` 拖垮进程。支持 `GRACEFUL`（取消在途任务，pending 继续执行）和 `FORCE`（额外清空 pending 队列）两种关闭模式。
- **可观测性**：新增 9 个运行时指标（队列深度、拒绝原因、活跃任务数、发送队列峰值），叠加原有连接/请求/字节计数器和 Connection 构造/析构计数；发送侧软背压 8MB/2MB 双水位，64MB 硬水位兜底。

## 架构

```
Client ──TCP──▶ Gateway ──TCP──▶ Backend ──▶ RuntimeEngine
                  │                 │              ├── Scheduler (轮转 + 配额)
                  │                 │              ├── Worker  (状态机 + 取消)
                  │                 │              └── LlamaClient (HTTP + 进度回调)
                  │                 │
                  │                 └── EchoNet 网络底座（Reactor / epoll）
                  └── Redis 服务发现 + 请求关联 + 健康检查
```

## 关键技术点

| 问题 | 方案 |
|---|---|
| 一个连接独占调度队列 | `Scheduler` 按客户端轮转取出，单客户端配额 256 |
| 服务关闭时阻塞 120s | `CancellationToken` + libcurl 进度回调，实测 108ms 退出 |
| 客户端断开后 pending 任务继续跑 | 所有关闭路径统一在 `handleClose` 中 `notifyCancelOnce()`，用不可变 `clientId_`；`RuntimeEngine::cancelByClient` 标记 `CLIENT_DISCONNECTED` |
| 关闭路径的重复清理 / 悬挂 | `finalizeCloseIfPending` 单一认领入口 + `EventLoop` 退出兜底 |
| 单连接回调异常拖垮进程 | `EventLoop::loop` 内 `handleEvent` 加 try/catch，异常连接走 `close()` |
| 发送队列无界增长 | 8MB 软水位暂停读取，2MB 恢复；64MB 硬断开止损 |
| 队列满时静默失败 | 分三层指标：单连接配额、全局 RuntimeEngine 上限、任务提交失败 |

## 构建

依赖：CMake ≥ 3.10、GCC 13、libcurl、hiredis、nlohmann/json。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

产物：`build/EchoNetGateway`、`build/EchoNetBackend`。

## 运行

```bash
# 依赖：Redis 在本地 6379
redis-server --daemonize yes

# 启动 Backend（监听 9001，需先有兼容 OpenAI Chat Completions 的服务在 8081）
./build/EchoNetBackend &

# 启动 Gateway（监听 8080）
./build/EchoNetGateway &
```

配置参数（`--help` 查看完整列表）：

```
--port=N          listen port (default 8080)
--io=N            IO threads (default 8)
--work=N          worker threads (default 4)
--bind-host=HOST  address registered to Redis (default 127.0.0.1)
--redis-host=HOST Redis host (default 127.0.0.1)
--redis-port=N    Redis port (default 6379)
```

协议：`| 4B 长度(net) | 2B 类型 | 4B 请求ID | 变长 payload |`，类型 `0x04` 为 `RUNTIME_TASK_REQ`，`0x05` 为 `RUNTIME_TASK_RESP`，`0x06` 为 `RUNTIME_TASK_FAILED`。

## 测试与验证

```bash
# Release
ctest --test-dir build --output-on-failure

# ASan + UBSan
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -g"
cmake --build build-asan -j
ctest --test-dir build-asan --output-on-failure
```

**结果**：14 个 CTest，Release 与 ASan/UBSan 构建均全通过，Release 0 warning。端到端链路与四个故障场景实测输出归档在 `results/`：

- `e2e_success.txt` —— Gateway → Backend → RuntimeEngine → 假 Qwen → 客户端，`type=5` 正常返回；
- `cancel_latency.txt` —— 关闭时取消在途模型调用，实测 108ms 退出（对比 120s 超时兜底）；
- `queue_overflow.txt` —— 队列满载时部分请求返回 `TASK_SUBMISSION_FAILED`，4 个槽位内正常执行；
- `client_disconnect.txt` —— 客户端发送后立即断开，后续请求正常，服务不崩。

测试覆盖的关键场景：

- `test_close_drain`：停服时连接清理恰好一次；验证 `closeCallback` 调用时 fd 仍有效、`finishClose` 后 fd 失效（`EBADF`）、`Connection` 构造数 = 析构数（资源级验证，不只计数器）；验证 `cancelHook_` 恰好一次且参数是原始 fd（不是 `finishClose` 后的 -1）；
- `test_runtime_cancel`：取消令牌从 `stop()` 传播到 `executor_`，200ms 阈值内完成；
- `test_scheduler_fairness`：一个客户端持续提交时另一客户端不被饿死，单客户端配额生效；
- `test_read_exception`：`Connection::read` 抛异常不会拖垮 `EventLoop`；
- `test_force_shutdown`：`FORCE` 模式下 5 个 pending 任务全部收到 `SERVER_SHUTTING_DOWN`，正在执行的任务被取消，毫秒级退出；
- `test_cancel_by_client`：`RuntimeEngine::cancelByClient` 单元测试——pending 队列里的 5 个任务收到 `CLIENT_DISCONNECTED`，正在执行的 1 个任务成功完成，另一个 `clientId` 不受影响；
- `test_cancel_by_client_e2e`：端到端验证——客户端断开触发 `Connection` 的 EOF 路径，验证 `handleClose → notifyCancelOnce → cancelHook_ → cancelByClient` 整条链路，5 个 pending 任务收到 `CLIENT_DISCONNECTED`，正在执行的 1 个任务成功完成。

性能数字均为单次观测，环境为 WSL2 + 本地回环，不代表生产规模。

## 设计取舍与已知边界

1. **`stop(GRACEFUL)` 取消正在执行的任务，`stop(FORCE)` 额外清空 pending 队列**
   `GRACEFUL` 下正在执行的任务会被 `cancel()`——这是有意为之：模型调用可能阻塞 120 秒，等它完成不叫“优雅关闭”，叫“关闭卡死”。`pending_queue_` 中的任务不受影响，会继续执行完毕，避免“提交成功但响应丢失”的不一致。`FORCE` 下额外调用 `Scheduler::drainAllPending()` 清空 pending 队列，每个任务 `fail("SERVER_SHUTTING_DOWN")`，`completion` 回调收到 `FAILED`。**两种模式都依赖 `executor_` 协作式响应取消令牌**——不检查令牌的第三方库，`join()` 仍会等它自然返回。

2. **`ClientId` 使用 fd，依赖“任务终态后 `eraseTerminal` 立即移除”**
   当前流程下 fd 复用时旧任务已清理，不会混排。若未来引入长生命周期任务或任务取消，需要替换为不依赖 fd 的唯一 ID。`Connection` 内部使用不可变的 `clientId_`（构造时的 fd），`cancelHook_` 传的是这个值，不受 `finishClose` 之后 `afd_ = -1` 的影响。

3. **`Worker::execute` 的 REJECTED 路径任务可能泄漏在 `Scheduler::tasks_`**
   `task->succeed` 失败时返回 `REJECTED`，但状态仍是 `RUNNING`，`eraseTerminal` 只删终态。理论上不会触发，但状态机边界未覆盖。

4. **关闭期间客户端无法区分“服务关闭”和“服务故障”**
   统一拒绝语义（发 `ERROR_RESP` 再关）推迟。需要“关闭前发送最后一帧”的机制，涉及 `handleClose` 和 `sendQueue_` 的交互。

5. **取消在真实网络阻塞场景的边界未做完整实测**
   只测了“服务端接受连接但永不响应”场景。TLS 握手、DNS 解析卡住等路径的取消耗时取决于 libcurl 内部行为，108ms 不能外推。

6. **所有关闭路径统一在 `handleClose` 中触发一次 `cancelHook_`**
   `handleClose` 的 CAS 成功后调用 `notifyCancelOnce()`，用不可变的 `clientId_`（构造时的 fd）而非 `afd_`——`finishClose` 之后 `afd_` 已被置 -1。`cancelHookCalled_` 的 CAS 保证只调一次。`close()` 本身不再调 `cancelHook_`，因为此时 `state_` 还是 `Connected`，`completion` 回调里 `sendResponse` 会发出无效的 `CLIENT_DISCONNECTED` 响应。`forceCloseOnLoopStop()` 也单独调用 `notifyCancelOnce()`——这条路径不经过 `handleClose`，但需要统一取消语义。

7. **未实现**：TLS、跨网关高可用、跨节点任务调度、推理引擎内部优化（CUDA / 算子 / 批处理）。

## 技术栈

C++17 · Linux · epoll · eventfd · timerfd · libcurl · hiredis · nlohmann/json · CMake · Redis

## 目录结构

```
include/         公共头文件（Net / Reactor / ThreadPool / Protocol / Runtime / Gateway / Metrics）
src/             实现
  backend_main.cpp   Backend 入口（RuntimeEngine 承载）
  gateway_main.cpp   Gateway 入口（服务发现 + 转发）
tests/           CTest 单元与集成测试
results/         端到端与故障场景的实测输出归档
```