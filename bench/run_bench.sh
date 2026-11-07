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
        # 后端不限流（内网连接是网关复用的，见 backend_main.cpp 注释）
        "$BUILD_DIR/AgentRuntimeBackend" --port="$p" \
            ${ENGINE_WORKERS:+--engine-workers="$ENGINE_WORKERS"} \
            > "/tmp/bench_be_$p.log" 2>&1 &
        BE_PIDS="$BE_PIDS $!"
    done
    sleep 2

    # 客户端准入限流在网关按真实客户端连接做
    "$BUILD_DIR/AgentRuntimeGateway" --port="$GW_PORT" \
        ${RATE_LIMIT:+--rate-limit="$RATE_LIMIT"} \
        ${RATE_BURST:+--rate-burst="$RATE_BURST"} \
        > /tmp/bench_gw.log 2>&1 &
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
    # 优雅停服约 0.7s（见 REPORT 的 20 次统计），这里最多等 5s 再 KILL，
    # 否则进程会在打印指标快照前被杀掉。
    for pid in ${BE_PIDS:-} ${GW_PID:-} ${MODEL_PID:-}; do
        for _ in $(seq 1 50); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.1
        done
    done
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

phase_ratelimit() {
    local conns="${1:-50}" secs="${2:-10}"
    export ENGINE_WORKERS=8
    for mode in on off; do
        if [ "$mode" = "off" ]; then export RATE_LIMIT=0; else unset RATE_LIMIT; fi
        start_infra || return 1
        log "ratelimit=$mode conns=$conns seconds=$secs"
        run_client "ratelimit_$mode" "$conns" "$secs" 32 closed_loop
        stop_infra
    done
    unset RATE_LIMIT
}

phase_leak() {
    local conns="${1:-50}" secs="${2:-10}"
    export ENGINE_WORKERS=8
    unset RATE_LIMIT
    start_infra || return 1
    log "leak: 压测 ${conns} 连接 × ${secs}s，然后优雅停服读指标"
    run_client leak "$conns" "$secs" 32 closed_loop
    stop_infra            # SIGTERM 后后端会打印 metrics 快照
    sleep 1

    grep -h "metrics:" /tmp/bench_be_*.log > "$OUT_DIR/leak.txt" 2>/dev/null
    cat "$OUT_DIR/leak.txt"

    python3 - "$OUT_DIR/leak.txt" <<'PY'
import re, sys
ok = True
rows = 0
for line in open(sys.argv[1]):
    m = re.search(r"connections_constructed=(\d+)\s+connections_destructed=(\d+)", line)
    if not m:
        continue
    rows += 1
    c, d = int(m.group(1)), int(m.group(2))
    if c != d:
        ok = False
        print(f"FAIL: constructed={c} destructed={d} (diff {c-d})")
if rows == 0:
    print("SKIP: 日志里没找到 metrics 行")
    sys.exit(3)
print("PASS: 所有后端 connections_constructed == connections_destructed" if ok else "FAIL")
sys.exit(0 if ok else 1)
PY
}

phase_emfile() {
    # 只起一个后端，并把 fd 上限压到 64：制造 EMFILE。
    # 验证点：
    #   1) 进程 CPU 不会飙到 100%（否则说明 listen fd 在 LT 下一直可读 → busy loop）
    #   2) 释放一部分连接后，新连接仍能被接受（预留 fd 方案能把服务救回来）
    redis-server --daemonize yes >/dev/null 2>&1
    redis-cli FLUSHALL >/dev/null 2>&1

    ( ulimit -n 64; exec "$BUILD_DIR/AgentRuntimeBackend" --port=9102 ) \
        > /tmp/bench_emfile.log 2>&1 &
    local be=$!
    sleep 2

    log "emfile: 打开 100 条连接把 fd 打满"
    python3 - "$be" <<'PY'
import socket, struct, subprocess, sys, time

pid = sys.argv[1]
socks = []
for _ in range(80):
    try:
        s = socket.create_connection(("127.0.0.1", 9102), timeout=0.3)
        socks.append(s)
    except OSError:
        break
print(f"opened={len(socks)}", flush=True)

time.sleep(1.5)
cpu = subprocess.run(["ps", "-o", "%cpu=", "-p", pid], capture_output=True, text=True).stdout.strip()
print(f"cpu_after_exhaust={cpu}", flush=True)

# 全部释放，等服务端把 fd 还回来，再验证能否重新建连
for s in socks:
    s.close()
time.sleep(1.5)

ok = True
for _ in range(5):
    try:
        socket.create_connection(("127.0.0.1", 9102), timeout=1).close()
    except OSError as e:
        ok = False
        print(f"reconnect failed: {e}", flush=True)
        break

print("PASS: 打满后能自行恢复接受新连接" if ok else "FAIL: 释放后仍无法建连", flush=True)
sys.exit(0 if ok else 1)
PY
    local rc=$?
    kill -TERM "$be" 2>/dev/null; wait "$be" 2>/dev/null
    return $rc
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
    ratelimit) build_binaries && phase_ratelimit "${2:-50}" "${3:-10}" ;;
    leak) build_binaries && phase_leak "${2:-50}" "${3:-10}" ;;
    emfile) build_binaries && phase_emfile ;;
    baseline) build_binaries && phase_baseline "${2:-50}" "${3:-10}" ;;
    fault) build_binaries && phase_fault "${2:-50}" "${3:-10}" ;;
    backpressure) build_binaries && phase_backpressure ;;
    shutdown) build_binaries && phase_shutdown ;;
    all) build_binaries && phase_sweep 50 10 && phase_baseline 200 60 \
            && phase_fault 100 20 && phase_backpressure && phase_shutdown ;;
    *) echo "unknown phase: $1"; exit 2 ;;
esac
