cmake_minimum_required(VERSION 3.10)

project(AgentRuntime LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ───── Sanitizer 开关 ─────
# ASan/UBSan 查内存越界和 UB，查不到数据竞争。这个项目的卖点是并发与关闭路径，
# 所以并发相关改动必须再跑一遍 TSan：
#   cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DENABLE_TSAN=ON
#   cmake --build build-tsan -j && ctest --test-dir build-tsan --output-on-failure
# 注意：TSan 不能和 ASan 同时开。
option(ENABLE_TSAN "Build with ThreadSanitizer (data-race detection)" OFF)
if(ENABLE_TSAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fno-omit-frame-pointer -g")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
    message(STATUS "ThreadSanitizer enabled")
endif()

# ═══════ 核心库：Reactor 网络框架 + 二进制协议 + 网关服务发现 ═══════
set(SRC_CORE
    src/Net/Socket.cpp
    src/Net/Connection.cpp
    src/Net/Buffer.cpp
    src/ThreadPool/TaskQueue.cpp
    src/ThreadPool/WorkThreadPool.cpp
    src/ThreadPool/IoThreadPool.cpp
    src/Reactor/Channel.cpp
    src/Reactor/EventLoop.cpp
    src/Reactor/Acceptor.cpp
    src/Server/Server.cpp
    src/Common/Config.cpp
    src/Protocol/MyProtocol.cpp
    src/DB/RedisPool.cpp
    src/Logger/AsyncLogger.cpp
    src/Metrics/Metrics.cpp
    src/Gateway/ServiceRegistry.cpp
    src/Runtime/RuntimeTask.cpp
    src/Runtime/Scheduler.cpp
    src/Runtime/Worker.cpp
    src/Runtime/RuntimeEngine.cpp
)

add_library(agentruntime_core STATIC ${SRC_CORE})
target_include_directories(agentruntime_core PUBLIC ${PROJECT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h)
find_library(HIREDIS_LIBRARY NAMES hiredis)
if(NOT HIREDIS_INCLUDE_DIR OR NOT HIREDIS_LIBRARY)
    message(FATAL_ERROR "hiredis development headers/library are required (libhiredis-dev)")
endif()
target_include_directories(agentruntime_core PUBLIC ${HIREDIS_INCLUDE_DIR})
target_link_libraries(agentruntime_core PUBLIC ${HIREDIS_LIBRARY} Threads::Threads)
target_compile_options(agentruntime_core PRIVATE -Wall -Wextra -Wpedantic)

# ═══════ AgentRuntimeGateway 可执行文件 ═══════
add_executable(AgentRuntimeGateway src/gateway_main.cpp)
target_link_libraries(AgentRuntimeGateway PRIVATE agentruntime_core)
target_compile_options(AgentRuntimeGateway PRIVATE -Wall -Wextra -Wpedantic)

# 独立后端进程，复用同一网络核心。
add_executable(AgentRuntimeBackend
    src/backend_main.cpp
    src/Runtime/LlamaClient.cpp
)

find_package(CURL REQUIRED)

find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp)
if(NOT NLOHMANN_JSON_INCLUDE_DIR)
    message(FATAL_ERROR "nlohmann/json headers are required (nlohmann-json3-dev)")
endif()
target_include_directories(AgentRuntimeBackend PRIVATE ${NLOHMANN_JSON_INCLUDE_DIR})

target_link_libraries(AgentRuntimeBackend PRIVATE agentruntime_core CURL::libcurl)
target_compile_options(AgentRuntimeBackend PRIVATE -Wall -Wextra -Wpedantic)
# RuntimeEngine 独立行为测试
enable_testing()
add_executable(test_runtime_engine tests/test_runtime_engine.cpp)
target_link_libraries(test_runtime_engine PRIVATE agentruntime_core)
target_compile_options(test_runtime_engine PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME runtime_engine COMMAND test_runtime_engine)

# ───── 阶段 1a：接入其余测试源文件 ─────
# 规则：先全部加进来构建。编译失败或运行失败的，把该测试的
# add_executable / add_test 注释掉，旁边标注 # TODO(阶段 2): <现象>。
# 不修改测试源码，不加 WILL_FAIL 掩盖问题。

add_executable(test_scheduler tests/test_scheduler.cpp)
target_link_libraries(test_scheduler PRIVATE agentruntime_core)
target_compile_options(test_scheduler PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME scheduler COMMAND test_scheduler)

add_executable(test_scheduler_concurrent tests/test_scheduler_concurrent.cpp)
target_link_libraries(test_scheduler_concurrent PRIVATE agentruntime_core)
target_compile_options(test_scheduler_concurrent PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME scheduler_concurrent COMMAND test_scheduler_concurrent)

add_executable(test_task_concurrent tests/test_task_concurrent.cpp)
target_link_libraries(test_task_concurrent PRIVATE agentruntime_core)
target_compile_options(test_task_concurrent PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME task_concurrent COMMAND test_task_concurrent)

add_executable(test_task_model tests/test_task_model.cpp)
target_link_libraries(test_task_model PRIVATE agentruntime_core)
target_compile_options(test_task_model PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME task_model COMMAND test_task_model)

add_executable(test_worker tests/test_worker.cpp)
target_link_libraries(test_worker PRIVATE agentruntime_core)
target_compile_options(test_worker PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME worker COMMAND test_worker)

add_executable(test_worker_concurrent tests/test_worker_concurrent.cpp)
target_link_libraries(test_worker_concurrent PRIVATE agentruntime_core)
target_compile_options(test_worker_concurrent PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME worker_concurrent COMMAND test_worker_concurrent)

add_executable(test_close_drain tests/test_close_drain.cpp)
target_link_libraries(test_close_drain PRIVATE agentruntime_core)
target_compile_options(test_close_drain PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME close_drain COMMAND test_close_drain)

add_executable(test_scheduler_fairness tests/test_scheduler_fairness.cpp)
target_link_libraries(test_scheduler_fairness PRIVATE agentruntime_core)
target_compile_options(test_scheduler_fairness PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME scheduler_fairness COMMAND test_scheduler_fairness)

add_executable(test_runtime_cancel tests/test_runtime_cancel.cpp)
target_link_libraries(test_runtime_cancel PRIVATE agentruntime_core)
target_compile_options(test_runtime_cancel PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME runtime_cancel COMMAND test_runtime_cancel)

add_executable(test_read_exception tests/test_read_exception.cpp)
target_link_libraries(test_read_exception PRIVATE agentruntime_core)
target_compile_options(test_read_exception PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME read_exception COMMAND test_read_exception)

add_executable(test_force_shutdown tests/test_force_shutdown.cpp)
target_link_libraries(test_force_shutdown PRIVATE agentruntime_core)
target_compile_options(test_force_shutdown PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME force_shutdown COMMAND test_force_shutdown)

add_executable(test_cancel_by_client tests/test_cancel_by_client.cpp)
target_link_libraries(test_cancel_by_client PRIVATE agentruntime_core)
target_compile_options(test_cancel_by_client PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME cancel_by_client COMMAND test_cancel_by_client)

add_executable(test_cancel_by_client_e2e tests/test_cancel_by_client_e2e.cpp)
target_link_libraries(test_cancel_by_client_e2e PRIVATE agentruntime_core)
target_compile_options(test_cancel_by_client_e2e PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME cancel_by_client_e2e COMMAND test_cancel_by_client_e2e)

# stop() 的幂等性与重入（旧实现用 call_once，FORCE 无法升级、worker 线程调用会自 join）
add_executable(test_stop_escalation tests/test_stop_escalation.cpp)
target_link_libraries(test_stop_escalation PRIVATE agentruntime_core)
target_compile_options(test_stop_escalation PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME stop_escalation COMMAND test_stop_escalation)

# ───── 测试断言必须存活 ─────
# 这些测试大量使用 assert()。Release 构建下 CMake 会加 -DNDEBUG，断言被整体编译掉，
# 测试变成"永远通过"。这里统一给测试目标去掉 NDEBUG。
foreach(test_target
    test_runtime_engine
    test_scheduler
    test_scheduler_concurrent
    test_task_concurrent
    test_task_model
    test_worker
    test_worker_concurrent
    test_close_drain
    test_scheduler_fairness
    test_runtime_cancel
    test_read_exception
    test_force_shutdown
    test_cancel_by_client
    test_cancel_by_client_e2e
    test_stop_escalation)
    target_compile_options(${test_target} PRIVATE -UNDEBUG)
endforeach()