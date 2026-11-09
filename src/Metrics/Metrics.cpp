#include "Metrics/Metrics.h"

#include <string>

std::atomic<uint64_t> Metrics::total_connections{0};
std::atomic<uint64_t> Metrics::active_connections{0};
std::atomic<uint64_t> Metrics::total_requests{0};
std::atomic<uint64_t> Metrics::error_requests{0};
std::atomic<uint64_t> Metrics::bytes_read{0};
std::atomic<uint64_t> Metrics::bytes_written{0};
std::atomic<uint64_t> Metrics::scheduler_pending{0};
std::atomic<uint64_t> Metrics::scheduler_rejected_quota{0};
std::atomic<uint64_t> Metrics::runtime_rejected_capacity{0};
std::atomic<uint64_t> Metrics::runtime_rejected_stopping{0};
std::atomic<uint64_t> Metrics::conn_rejected_pending_full{0};
std::atomic<uint64_t> Metrics::conn_rejected_submit_failed{0};
std::atomic<uint64_t> Metrics::conn_closed_send_high_water{0};
std::atomic<uint64_t> Metrics::runtime_active{0};
std::atomic<uint64_t> Metrics::conn_max_send_queue_bytes{0};
std::atomic<uint64_t> Metrics::conn_backpressure_paused{0};
std::atomic<uint64_t> Metrics::conn_backpressure_resumed{0};
std::atomic<uint64_t> Metrics::connection_constructed{0};
std::atomic<uint64_t> Metrics::connection_destructed{0};

std::string Metrics::Summary()
{
    auto v = [](const std::atomic<uint64_t>& a) { return a.load(std::memory_order_relaxed); };

    std::string s;
    s += "connections_constructed=" + std::to_string(v(connection_constructed));
    s += " connections_destructed=" + std::to_string(v(connection_destructed));
    s += " conns_total=" + std::to_string(v(total_connections));
    s += " conns_active=" + std::to_string(v(active_connections));
    s += " requests=" + std::to_string(v(total_requests));
    s += " errors=" + std::to_string(v(error_requests));
    s += " bytes_read=" + std::to_string(v(bytes_read));
    s += " bytes_written=" + std::to_string(v(bytes_written));
    s += " runtime_active=" + std::to_string(v(runtime_active));
    s += " runtime_rejected_capacity=" + std::to_string(v(runtime_rejected_capacity));
    s += " scheduler_rejected_quota=" + std::to_string(v(scheduler_rejected_quota));
    s += " send_high_water_closed=" + std::to_string(v(conn_closed_send_high_water));
    s += " bp_paused=" + std::to_string(v(conn_backpressure_paused));
    s += " bp_resumed=" + std::to_string(v(conn_backpressure_resumed));
    return s;
}