cmake_minimum_required(VERSION 3.10)

project(AgentRuntime LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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

add_library(echonet_core STATIC ${SRC_CORE})
target_include_directories(echonet_core PUBLIC ${PROJECT_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h)
find_library(HIREDIS_LIBRARY NAMES hiredis)
if(NOT HIREDIS_INCLUDE_DIR OR NOT HIREDIS_LIBRARY)
    message(FATAL_ERROR "hiredis development headers/library are required (libhiredis-dev)")
endif()
target_include_directories(echonet_core PUBLIC ${HIREDIS_INCLUDE_DIR})
target_link_libraries(echonet_core PUBLIC ${HIREDIS_LIBRARY} Threads::Threads)
target_compile_options(echonet_core PRIVATE -Wall -Wextra -Wpedantic)

# ═══════ EchoNetGateway 可执行文件 ═══════
add_executable(EchoNetGateway src/gateway_main.cpp)
target_link_libraries(EchoNetGateway PRIVATE echonet_core)
target_compile_options(EchoNetGateway PRIVATE -Wall -Wextra -Wpedantic)

# 独立后端进程，复用同一网络核心。
add_executable(EchoNetBackend
    src/backend_main.cpp
    src/Runtime/LlamaClient.cpp
)

find_package(CURL REQUIRED)

find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp)
if(NOT NLOHMANN_JSON_INCLUDE_DIR)
    message(FATAL_ERROR "nlohmann/json headers are required (nlohmann-json3-dev)")
endif()
target_include_directories(EchoNetBackend PRIVATE ${NLOHMANN_JSON_INCLUDE_DIR})

target_link_libraries(EchoNetBackend PRIVATE echonet_core CURL::libcurl)
target_compile_options(EchoNetBackend PRIVATE -Wall -Wextra -Wpedantic)
# RuntimeEngine 独立行为测试
enable_testing()
add_executable(test_runtime_engine tests/test_runtime_engine.cpp)
target_link_libraries(test_runtime_engine PRIVATE echonet_core)
target_compile_options(test_runtime_engine PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME runtime_engine COMMAND test_runtime_engine)

# ───── 阶段 1a：接入其余测试源文件 ─────
# 规则：先全部加进来构建。编译失败或运行失败的，把该测试的
# add_executable / add_test 注释掉，旁边标注 # TODO(阶段 2): <现象>。
# 不修改测试源码，不加 WILL_FAIL 掩盖问题。

add_executable(test_scheduler tests/test_scheduler.cpp)
target_link_libraries(test_scheduler PRIVATE echonet_core)
target_compile_options(test_scheduler PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME scheduler COMMAND test_scheduler)

add_executable(test_scheduler_concurrent tests/test_scheduler_concurrent.cpp)
target_link_libraries(test_scheduler_concurrent PRIVATE echonet_core)
target_compile_options(test_scheduler_concurrent PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME scheduler_concurrent COMMAND test_scheduler_concurrent)

add_executable(test_task_concurrent tests/test_task_concurrent.cpp)
target_link_libraries(test_task_concurrent PRIVATE echonet_core)
target_compile_options(test_task_concurrent PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME task_concurrent COMMAND test_task_concurrent)

add_executable(test_task_model tests/test_task_model.cpp)
target_link_libraries(test_task_model PRIVATE echonet_core)
target_compile_options(test_task_model PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME task_model COMMAND test_task_model)

add_executable(test_worker tests/test_worker.cpp)
target_link_libraries(test_worker PRIVATE echonet_core)
target_compile_options(test_worker PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME worker COMMAND test_worker)

add_executable(test_worker_concurrent tests/test_worker_concurrent.cpp)
target_link_libraries(test_worker_concurrent PRIVATE echonet_core)
target_compile_options(test_worker_concurrent PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME worker_concurrent COMMAND test_worker_concurrent)

add_executable(test_close_drain tests/test_close_drain.cpp)
target_link_libraries(test_close_drain PRIVATE echonet_core)
target_compile_options(test_close_drain PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME close_drain COMMAND test_close_drain)

add_executable(test_scheduler_fairness tests/test_scheduler_fairness.cpp)
target_link_libraries(test_scheduler_fairness PRIVATE echonet_core)
target_compile_options(test_scheduler_fairness PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME scheduler_fairness COMMAND test_scheduler_fairness)

add_executable(test_runtime_cancel tests/test_runtime_cancel.cpp)
target_link_libraries(test_runtime_cancel PRIVATE echonet_core)
target_compile_options(test_runtime_cancel PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME runtime_cancel COMMAND test_runtime_cancel)

add_executable(test_read_exception tests/test_read_exception.cpp)
target_link_libraries(test_read_exception PRIVATE echonet_core)
target_compile_options(test_read_exception PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME read_exception COMMAND test_read_exception)

add_executable(test_force_shutdown tests/test_force_shutdown.cpp)
target_link_libraries(test_force_shutdown PRIVATE echonet_core)
target_compile_options(test_force_shutdown PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME force_shutdown COMMAND test_force_shutdown)