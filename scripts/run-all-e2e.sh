#!/usr/bin/env bash
# 跑完整 PHP e2e 套件作为生产回归。每个测试独立 socket / topic 前缀，互不打架。
# 输出汇总：哪些通过 / 哪些失败 / 总耗时。
#
# 用法：
#   ./scripts/run-all-e2e.sh                       # 默认全跑
#   E2E_TESTS="consumer-stream-v2 recovery" ./scripts/run-all-e2e.sh
#   EXT_PATH=/path/to/libhi_kafka.dylib ./scripts/run-all-e2e.sh
#   PHP_EXTRA_ARGS="-d extension_dir=$(php-config --extension-dir) -d extension=swoole" ./scripts/run-all-e2e.sh
set -uo pipefail

cd "$(dirname "$0")/.."

# PHP_BIN：默认 php；macbook 上自动路由有效但 bash 子 shell 不传，
# 这里支持直接给绝对路径
PHP_BIN="${PHP_BIN:-php}"
# 额外的 php 参数（例如加载 swoole 以覆盖协程用例）；按空白分词。
PHP_EXTRA_ARGS="${PHP_EXTRA_ARGS:-}"
EXT_PATH="${EXT_PATH:-$(realpath target/debug/libhi_kafka.dylib 2>/dev/null \
    || realpath target/debug/libhi_kafka.so 2>/dev/null)}"
if [[ -z "$EXT_PATH" || ! -f "$EXT_PATH" ]]; then
    echo "ERROR: 扩展产物缺失，先 cargo build -p hi-kafka --features kafka-vendored-tls" >&2
    exit 1
fi
# 校验 php 可用
if ! command -v "$PHP_BIN" &>/dev/null; then
    echo "ERROR: $PHP_BIN 不可用。设 PHP_BIN 环境变量到绝对路径，或先装 PHP" >&2
    exit 1
fi

# 默认全跑（除掉以 _ 开头的辅助脚本）
ALL_TESTS=(
    consumer-stream-v2
    producer-generation-v2
    binary-roundtrip-v2
    consumer-record-memory-v2
    consumer-record-memory-swoole-v2
    coroutine-consumer-stream-v2
    client-recovery-v2
    consumer-offset-v2
    consumer-rebalance-v2
    consumer-cooperative-v2
    worker-spawn-v2
    client-fork-v2
    worker-drain-v2
    panic-recovery
    recovery
)
TESTS="${E2E_TESTS:-${ALL_TESTS[@]}}"

green() { printf '\033[32m%s\033[0m' "$*"; }
red()   { printf '\033[31m%s\033[0m' "$*"; }
yellow() { printf '\033[33m%s\033[0m' "$*"; }

# 每个测试用独立 socket；测试结束后必须主动杀 worker，否则它被 init 收养后会一直
# 挂着（ps 里看是 'php' 因为 macOS 没 PR_SET_NAME 改不了 argv[0]）。
# pid 文件由 worker 自己在 bind socket 后写入；recovery 类测试 kill 旧 worker 后
# 新 worker 写新 pid。统一靠 pid 文件清理。
cleanup_worker() {
    local sock="$1"
    if [[ -f "${sock}.pid" ]]; then
        local pid
        pid=$(cat "${sock}.pid" 2>/dev/null || true)
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
            for _ in 1 2 3 4 5; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.2
            done
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
    rm -f "$sock" "${sock}.pid" "${sock}.spawn-lock"
}

pass=0
fail=0
failed_tests=()

global_start=$(date +%s)
for t in $TESTS; do
    script="tests/php/${t}.php"
    if [[ ! -f "$script" ]]; then
        echo "$(yellow "SKIP") $t (not found)"
        continue
    fi
    sock="/tmp/e2e-${t}-$$.sock"
    topic_prefix="e2e-${t}-$(date +%s%N)-$$"

    start=$(date +%s%N)
    printf "==> %-25s " "$t"
    # -n 跳过系统 php.ini：避免 xdebug 之类的 observer 在 MSHUTDOWN 阶段崩
    output=$(env \
        HI_KAFKA_EXT_PATH="$EXT_PATH" \
        HI_KAFKA_BROKERS="${HI_KAFKA_BROKERS:-127.0.0.1:9094}" \
        "$PHP_BIN" -n -d extension="$EXT_PATH" $PHP_EXTRA_ARGS "$script" "$sock" "$topic_prefix" 2>&1)
    rc=$?
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))

    if [[ $rc -eq 0 ]] && echo "$output" | grep -qE '★|PASS$|全部 PASS'; then
        echo "$(green PASS) (${elapsed_ms}ms)"
        pass=$((pass + 1))
    elif [[ $rc -eq 0 ]] && echo "$output" | grep -q '^SKIP:'; then
        echo "$(yellow SKIP) (${elapsed_ms}ms): $(echo "$output" | grep '^SKIP:' | sed 's/^SKIP: //')"
    else
        echo "$(red FAIL) (${elapsed_ms}ms, rc=$rc)"
        echo "$output" | tail -10 | sed 's/^/      /'
        fail=$((fail + 1))
        failed_tests+=("$t")
    fi

    # 清理：杀 worker + 删 socket / pid / lock 文件
    cleanup_worker "$sock"
done

global_end=$(date +%s)
total_secs=$((global_end - global_start))

echo
echo "==================================================================="
echo "总结：通过 $pass 个，失败 $fail 个，总耗时 ${total_secs}s"
if [[ $fail -gt 0 ]]; then
    echo "失败列表：${failed_tests[*]}"
    exit 1
fi
green "★ 全部通过"
echo
