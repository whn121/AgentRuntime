#pragma once

#include <csignal>
#include <cerrno>
#include <cstdint>
#include <unistd.h>

// Single-server process signal bridge. The handler only sets sig_atomic_t and writes eventfd.
class SignalHandler
{
public:
    static void init(int wakeupFd)
    {
        requested_ = 0;
        wakeupFd_ = wakeupFd;
        struct sigaction action{};
        action.sa_handler = handler;
        sigemptyset(&action.sa_mask);
        sigaction(SIGINT, &action, nullptr);
        sigaction(SIGTERM, &action, nullptr);
    }
    static bool requested(int fd) { return wakeupFd_ == fd && requested_; }
    static void reset(int fd) { if (wakeupFd_ == fd) wakeupFd_ = -1; }
private:
    static void handler(int)
    {
        const int saved = errno;
        requested_ = 1;
        const uint64_t one = 1;
        if (wakeupFd_ >= 0) {
            const auto ignored = ::write(wakeupFd_, &one, sizeof(one));
            (void)ignored;
        }
        errno = saved;
    }
    inline static volatile sig_atomic_t wakeupFd_{-1};
    inline static volatile sig_atomic_t requested_{0};
};