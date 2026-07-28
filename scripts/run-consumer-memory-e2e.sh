#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PHP_BIN="${PHP_BIN:-php}"
EXT_PATH="${EXT_PATH:-$(realpath target/debug/libhi_kafka.dylib 2>/dev/null \
    || realpath target/debug/libhi_kafka.so 2>/dev/null)}"
BROKERS="${HI_KAFKA_BROKERS:-127.0.0.1:9094}"
MESSAGES="${HI_KAFKA_E2E_MESSAGES:-2048}"
MESSAGE_BYTES="${HI_KAFKA_E2E_MESSAGE_BYTES:-65536}"
MAX_WORKER_RSS_KB="${HI_KAFKA_E2E_MAX_WORKER_RSS_KB:-262144}"
MAX_TAIL_GROWTH_KB="${HI_KAFKA_E2E_MAX_TAIL_GROWTH_KB:-65536}"

if [[ -z "$EXT_PATH" || ! -f "$EXT_PATH" ]]; then
    echo "ERROR: extension artifact is missing" >&2
    exit 1
fi

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/hi-kafka-memory.XXXXXX")
socket="$tmpdir/worker.sock"
phase_file="$tmpdir/phase"
output_file="$tmpdir/php.out"
topic="memory-v2-$(date +%s)-$$"

cleanup() {
    if [[ -f "${socket}.pid" ]]; then
        worker_pid=$(cat "${socket}.pid" 2>/dev/null || true)
        if [[ -n "$worker_pid" ]] && kill -0 "$worker_pid" 2>/dev/null; then
            kill -TERM "$worker_pid" 2>/dev/null || true
        fi
    fi
    rm -rf "$tmpdir"
}
trap cleanup EXIT

env \
    HI_KAFKA_BROKERS="$BROKERS" \
    HI_KAFKA_E2E_MESSAGES="$MESSAGES" \
    HI_KAFKA_E2E_MESSAGE_BYTES="$MESSAGE_BYTES" \
    HI_KAFKA_E2E_PHASE_FILE="$phase_file" \
    "$PHP_BIN" -n -d extension="$EXT_PATH" \
    tests/php/consumer-stream-v2.php "$socket" "$topic" \
    >"$output_file" 2>&1 &
php_pid=$!

quarter=$((MESSAGES / 4))
(( quarter > 0 )) || quarter=1
quarter_rss_kb=0
tail_max_rss_kb=0
worker_max_rss_kb=0
producer_max_rss_kb=0

while kill -0 "$php_pid" 2>/dev/null; do
    if [[ -f "${socket}.pid" ]]; then
        worker_pid=$(cat "${socket}.pid" 2>/dev/null || true)
        rss_kb=$(ps -o rss= -p "$worker_pid" 2>/dev/null | tr -d ' ' || true)
        phase=$(cat "$phase_file" 2>/dev/null || true)
        if [[ "$rss_kb" =~ ^[0-9]+$ ]]; then
            (( rss_kb > worker_max_rss_kb )) && worker_max_rss_kb=$rss_kb
            if [[ "$phase" == produce:* ]]; then
                (( rss_kb > producer_max_rss_kb )) && producer_max_rss_kb=$rss_kb
            elif [[ "$phase" == consume:* ]]; then
                progress=${phase#consume:}
                progress=${progress:-0}
            else
                progress=0
            fi
            if [[ "$phase" == consume:* ]] && (( progress >= quarter )); then
                (( quarter_rss_kb == 0 )) && quarter_rss_kb=$rss_kb
                (( rss_kb > tail_max_rss_kb )) && tail_max_rss_kb=$rss_kb
            fi
        fi
    fi
    sleep 0.05
done

set +e
wait "$php_pid"
php_rc=$?
set -e
cat "$output_file"
(( php_rc == 0 )) || exit "$php_rc"

if (( worker_max_rss_kb == 0 || quarter_rss_kb == 0 )); then
    echo "ERROR: failed to sample worker RSS" >&2
    exit 1
fi
tail_growth_kb=$((tail_max_rss_kb - quarter_rss_kb))
(( tail_growth_kb >= 0 )) || tail_growth_kb=0

echo "Consumer memory samples: producer peak=${producer_max_rss_kb} KiB, 25%=${quarter_rss_kb} KiB, steady peak=${tail_max_rss_kb} KiB, overall peak=${worker_max_rss_kb} KiB"

if (( worker_max_rss_kb > MAX_WORKER_RSS_KB )); then
    echo "ERROR: worker peak RSS ${worker_max_rss_kb} KiB exceeds ${MAX_WORKER_RSS_KB} KiB" >&2
    exit 1
fi
if (( tail_growth_kb > MAX_TAIL_GROWTH_KB )); then
    echo "ERROR: worker RSS grew ${tail_growth_kb} KiB after 25% warm-up" >&2
    exit 1
fi

echo "★ Consumer memory PASS: worker peak=${worker_max_rss_kb} KiB, post-warmup growth=${tail_growth_kb} KiB"
