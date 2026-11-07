#!/bin/bash
# bench/run_bench.sh —— AgentRuntime 压测与故障注入
#
# 用法:
#   ./bench/run_bench.sh build              # 编译
#   ./bench/run_bench.sh sweep  [C] [S]     # engine worker 数扫描（2/4/8/16）
#   ./bench/run_bench.sh baseline [C] [S]   # 闭环压测
#   ./bench/run_bench.sh fault  [C] [S]     # 压测中途 kill 一个 backend
#   ./bench/run_bench.sh backpressure       # 只发不收，触发发送侧背压
#   ./bench/run_bench.sh shutdown           # 20 次停服延迟统计（SIGTERM→退出）
#   ./bench/run_bench.sh all                # sweep + baseline + fault + backpressure + shutdown
#
# 环境变量:
#   ENGINE_WORKERS   后端 RuntimeEngine 线程数（默认 2）
#   MODEL_DELAY_MS   假模型延迟毫秒（默认 5）
#   CONNS / SECONDS  默认 50 / 10

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1

BUILD_DIR="${BUILD_DIR:-$ROOT/build_bench}"
BENCH_BIN="$ROOT/bench/bench_client"
OUT_DIR="$ROOT/bench/results"
GW_PORT=8080
BE_PORTS="9001 9002 9003"
MODEL_PORT=8081
MODEL_DELAY_MS="${MODEL_DELAY_MS:-5}"
ENGINE_WORKERS="${ENGINE_WORKERS:-}"

mkdir -p "$OUT_DIR"

log() { printf '[bench] %s\n' "$*"; }

build_binaries() {
    log "配置并编译（Release）"
    cmake -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release >/dev/null || return 1
    cmake --build "$BUILD_DIR" -j"$(nproc)" >/dev/null || return 1
    g++ -O2 -std=c++17 -pthread -o "$BENCH_BIN" bench/bench_client.cpp || return 1
    log "编译完成"
}

start_fake_model() {
    # 注意：必须用 ThreadingHTTPServer。
    # 单线程的 HTTPServer 会把并发请求串行化，5ms/请求时吞吐上限只有 ~200 QPS，
    # 于是"QPS 不随 engine worker 增长"的假象会全部来自压测桩自己。
    cat > /tmp/bench_fake_model.py <<PY
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json, time, os
DELAY = float(os.environ.get("MODEL_DELAY_MS", "5")) / 1000.0

class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_POST(self):
        n = int(self.headers.get('Content-Length', 0)); self.rfile.read(n)
        time.sleep(DELAY)
        body = json.dumps({"choices": [{"message": {"content": "ok"}}]}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers(); self.wfile.write(body)
    def log_message(self, *a): pass

class S(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 512

S(('127.0.0.1', $MODEL_PORT), H).serve_forever()
PY
    MODEL_DELAY_MS="$MODEL_DELAY_MS" python3 /tmp/bench_fake_model.py &
    MODEL_PID=$!
    sleep 1
}

start_infra() {
    redis-server --daemonize yes >/dev/null 2>&1
    redis-cli ping >/dev/null 2>&1 || { log "redis 未就绪"; return 1; }
    redis-cli FLUSHALL >/dev/null 2>&1

    start_fake_model || return 1

    BE_PIDS=""
    for p in $BE_PORTS; do
        "$BUILD_DIR/AgentRuntimeBackend" --port="$p" \
            ${ENGINE_WORKERS:+--engine-workers="$ENGINE_WORKERS"} \
            ${RATE_LIMIT:+--rate-limit="$RATE_LIMIT"} \
            > "/tmp/bench_be_$p.log" 2>&1 &
        BE_PIDS="$BE_PIDS $!"
    done
    sleep 2

    "$BUILD_DIR/AgentRuntimeGateway" --port="$GW_PORT" > /tmp/bench_gw.log 2>&1 &
    GW_PID=$!
    sleep 2

    for _ in $(seq 1 20); do
        grep -q "registered services" /tmp/bench_be_9001.log 2>/dev/null && break
        sleep 0.5
    done
    log "就绪: gateway=$GW_PID backends=[$BE_PIDS] model=$MODEL_PID engine_workers=${ENGINE_WORKERS:-2}"
}

stop_infra() {
    for pid in ${BE_PIDS:-} ${GW_PID:-} ${MODEL_PID:-}; do
        kill -TERM "$pid" 2>/dev/null
    done
    sleep 0.5
    for pid in ${BE_PIDS:-} ${GW_PID:-} ${MODEL_PID:-}; do
        kill -KILL "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    BE_PIDS=""; GW_PID=""; MODEL_PID=""
}

run_client() {
    local tag="$1" conns="$2" secs="$3" payload="$4" mode="$5"
    "$BENCH_BIN" --host 127.0.0.1 --port "$GW_PORT" \
        --conns "$conns" --seconds "$secs" --payload "$payload" --mode "$mode" \
        --csv "$OUT_DIR/$tag.csv" | tee "$OUT_DIR/$tag.txt"
}

phase_sweep() {
    local conns="${1:-50}" secs="${2:-10}"
    # 关掉每连接令牌桶：它挂在网关的复用连接上，会掩盖真实容量
    export RATE_LIMIT=0
    for w in 2 4 8 16; do
        ENGINE_WORKERS="$w" start_infra || return 1
        log "sweep: engine_workers=$w conns=$conns seconds=$secs"
        run_client "sweep_w${w}" "$conns" "$secs" 32 closed_loop
        stop_infra
    done
}

phase_baseline() {
    local conns="${1:-50}" secs="${2:-10}"
    start_infra || return 1
    log "baseline: conns=$conns seconds=$secs"
    run_client baseline "$conns" "$secs" 32 closed_loop
    stop_infra
}

phase_fault() {
    local conns="${1:-50}" secs="${2:-10}"
    start_infra || return 1
    log "fault: 压测中途 kill 一个 backend"
    ( sleep $((secs / 2)); kill -KILL "$(echo $BE_PIDS | awk '{print $1}')" 2>/dev/null; \
      log "已 kill 9001" ) &
    run_client fault "$conns" "$secs" 32 closed_loop
    stop_infra
}

phase_backpressure() {
    start_infra || return 1
    log "backpressure: 只发不收，观察发送侧水位"
    run_client backpressure 20 10 1024 blast
    sleep 1
    log "gateway 日志尾部"
    tail -5 /tmp/bench_gw.log || true
    stop_infra
}

phase_shutdown() {
    redis-server --daemonize yes >/dev/null 2>&1
    MODEL_DELAY_MS=300000 start_fake_model
    local out="$OUT_DIR/shutdown.csv"
    echo "run,ms" > "$out"

    for i in $(seq 1 20); do
        "$BUILD_DIR/AgentRuntimeBackend" --port=9101 > /tmp/bench_be_sd.log 2>&1 &
        local be=$!
        sleep 1

        python3 - <<'PY' || true
import socket, struct
s = socket.create_connection(("127.0.0.1", 9101), timeout=3)
body = struct.pack("!HI", 0x04, 1) + b"hold"
s.sendall(struct.pack("!I", len(body)) + body)
PY
        sleep 0.3

        local t0 t1 ms
        t0=$(date +%s%3N)
        kill -TERM "$be" 2>/dev/null
        wait "$be" 2>/dev/null
        t1=$(date +%s%3N)
        ms=$((t1 - t0))
        echo "$i,$ms" >> "$out"
        log "shutdown run $i: ${ms}ms"
    done

    kill -TERM "$MODEL_PID" 2>/dev/null
    wait 2>/dev/null
    MODEL_PID=""

    python3 - "$out" <<'PY'
import csv, sys, statistics
vals = []
with open(sys.argv[1]) as f:
    for row in csv.DictReader(f):
        vals.append(float(row["ms"]))
vals.sort()
def pct(p):
    return vals[min(len(vals) - 1, int(p * len(vals)))]
print(f"shutdown(n={len(vals)}) median={statistics.median(vals):.0f}ms "
      f"p95={pct(0.95):.0f}ms min={min(vals):.0f}ms max={max(vals):.0f}ms")
PY
}

case "${1:-all}" in
    build) build_binaries ;;
    sweep) build_binaries && phase_sweep "${2:-50}" "${3:-10}" ;;
    baseline) build_binaries && phase_baseline "${2:-50}" "${3:-10}" ;;
    fault) build_binaries && phase_fault "${2:-50}" "${3:-10}" ;;
    backpressure) build_binaries && phase_backpressure ;;
    shutdown) build_binaries && phase_shutdown ;;
    all) build_binaries && phase_sweep 50 10 && phase_baseline 200 60 \
            && phase_fault 100 20 && phase_backpressure && phase_shutdown ;;
    *) echo "unknown phase: $1"; exit 2 ;;
esac
