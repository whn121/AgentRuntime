cmake_minimum_required(VERSION 3.10)

project(EchoNet LANGUAGES CXX)

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
target_link_libraries(EchoNetBackend PRIVATE echonet_core CURL::libcurl)
target_compile_options(EchoNetBackend PRIVATE -Wall -Wextra -Wpedantic)
# RuntimeEngine 独立行为测试
enable_testing()
add_executable(test_runtime_engine tests/test_runtime_engine.cpp)
target_link_libraries(test_runtime_engine PRIVATE echonet_core)
target_compile_options(test_runtime_engine PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME runtime_engine COMMAND test_runtime_engine)
