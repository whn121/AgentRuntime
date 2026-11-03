#pragma once
#include <memory>
#include <any>
#include <functional>


class Connection;
// 任务结构体，在工作线程池中传递
struct Task 
{
    // 注意：这里**不持有** Connection 的 shared_ptr。
    // run_ / onComplete_ 内部用 weak_ptr 提升，能避免"任务排队期间把连接寿命拖长"；
    // 如果这一层再放一个 shared_ptr，两套所有权语义会互相打架。
    std::any message_;                   // 解析后的请求消息
    std::function<void()> run_;          // 真正执行的业务逻辑

    // 当前任务执行完成后，通知 Connection：
    // “我执行完了，可以继续执行下一个任务了”
    std::function<void()> onComplete_;
};
