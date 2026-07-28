#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

PHP_BIN="${PHP_BIN:-php}"
EXT_PATH="${EXT_PATH:-$(realpath target/release/libhi_kafka.dylib 2>/dev/null \
    || realpath target/release/libhi_kafka.so 2>/dev/null \
    || realpath target/debug/libhi_kafka.dylib 2>/dev/null \
    || realpath target/debug/libhi_kafka.so 2>/dev/null)}"
RUNTIMES="${BENCH_RUNTIMES:-native,swoole}"
SCENARIOS="${BENCH_SCENARIOS:-produce-fnf,produce-sync,consume}"
MESSAGE_SIZES="${BENCH_MESSAGE_SIZES:-1024}"
CONCURRENCIES="${BENCH_CONCURRENCIES:-1,16}"
MESSAGES="${BENCH_MESSAGES:-20000}"
WARMUP="${BENCH_WARMUP:-2000}"
BROKERS="${HI_KAFKA_BROKERS:-127.0.0.1:9094}"
SAMPLE_INTERVAL="${BENCH_SAMPLE_INTERVAL:-0.1}"
RESULT_FILE="${BENCH_RESULT_FILE:-benchmarks/results/$(date +%Y%m%d-%H%M%S).jsonl}"

if [[ -z "$EXT_PATH" || ! -f "$EXT_PATH" ]]; then
    echo "ERROR: extension artifact is missing; set EXT_PATH" >&2
    exit 1
fi

extension_dir=$($PHP_BIN -i 2>/dev/null | awk -F ' => ' '/^extension_dir =>/ && !seen++ {print $2}')
SWOOLE_EXT_PATH="${SWOOLE_EXT_PATH:-${extension_dir}/swoole.so}"
SWOW_EXT_PATH="${SWOW_EXT_PATH:-${extension_dir}/swow.so}"

mkdir -p "$(dirname "$RESULT_FILE")"
: >"$RESULT_FILE"

IFS=',' read -r -a runtimes <<<"$RUNTIMES"
IFS=',' read -r -a scenarios <<<"$SCENARIOS"
IFS=',' read -r -a sizes <<<"$MESSAGE_SIZES"
IFS=',' read -r -a concurrencies <<<"$CONCURRENCIES"

printf "%-8s %-14s %8s %6s %12s %12s %12s %10s\n" runtime scenario bytes conc msg_per_s MiB_per_s p99_us worker_MiB

for runtime in "${runtimes[@]}"; do
    # Bash 3.2 (macOS) treats an empty array expansion as unbound under `set -u`.
    runtime_args=(-d "display_errors=1")
    if [[ "$runtime" == swoole ]]; then
        if [[ ! -f "$SWOOLE_EXT_PATH" ]]; then
            echo "SKIP swoole: extension not found at $SWOOLE_EXT_PATH" >&2
            continue
        fi
        runtime_args+=(-d "extension=$SWOOLE_EXT_PATH")
    elif [[ "$runtime" == swow ]]; then
        if [[ ! -f "$SWOW_EXT_PATH" ]]; then
            echo "SKIP swow: extension not found at $SWOW_EXT_PATH" >&2
            continue
        fi
        runtime_args+=(-d "extension=$SWOW_EXT_PATH")
    elif [[ "$runtime" != native ]]; then
        echo "ERROR: unknown runtime $runtime" >&2
        exit 2
    fi

    for scenario in "${scenarios[@]}"; do
        for bytes in "${sizes[@]}"; do
            for concurrency in "${concurrencies[@]}"; do
                if [[ "$runtime" == native && "$concurrency" != 1 ]]; then
                    continue
                fi

                tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/hi-kafka-bench.XXXXXX")
                socket="$tmpdir/worker.sock"
                raw_output="$tmpdir/output"
                samples="$tmpdir/samples"
                topic="bench-${runtime}-${scenario}-${bytes}-${concurrency}-$(date +%s)-$$"

                cleanup_case() {
                    if [[ -f "${socket}.pid" ]]; then
                        worker_pid=$(cat "${socket}.pid" 2>/dev/null || true)
                        if [[ -n "$worker_pid" ]] && kill -0 "$worker_pid" 2>/dev/null; then
                            kill -TERM "$worker_pid" 2>/dev/null || true
                        fi
                    fi
                    rm -rf "$tmpdir"
                }
                trap cleanup_case EXIT

                env HI_KAFKA_BROKERS="$BROKERS" \
                    "$PHP_BIN" -n -d "extension=$EXT_PATH" "${runtime_args[@]}" \
                    benchmarks/kafka.php \
                    --runtime="$runtime" \
                    --scenario="$scenario" \
                    --messages="$MESSAGES" \
                    --warmup="$WARMUP" \
                    --message-bytes="$bytes" \
                    --concurrency="$concurrency" \
                    --socket="$socket" \
                    --topic="$topic" \
                    >"$raw_output" 2>&1 &
                php_pid=$!

                while kill -0 "$php_pid" 2>/dev/null; do
                    php_sample=$(ps -o rss=,%cpu= -p "$php_pid" 2>/dev/null | awk '{$1=$1; print}' || true)
                    worker_sample="0 0"
                    if [[ -f "${socket}.pid" ]]; then
                        worker_pid=$(cat "${socket}.pid" 2>/dev/null || true)
                        if [[ -n "$worker_pid" ]]; then
                            worker_sample=$(ps -o rss=,%cpu= -p "$worker_pid" 2>/dev/null | awk '{$1=$1; print}' || true)
                            [[ -n "$worker_sample" ]] || worker_sample="0 0"
                        fi
                    fi
                    [[ -n "$php_sample" ]] || php_sample="0 0"
                    printf '%s %s\n' "$php_sample" "$worker_sample" >>"$samples"
                    sleep "$SAMPLE_INTERVAL"
                done

                set +e
                wait "$php_pid"
                rc=$?
                set -e
                if (( rc != 0 )); then
                    cat "$raw_output" >&2
                    cleanup_case
                    trap - EXIT
                    exit "$rc"
                fi

                raw_result=$(tail -n 1 "$raw_output")
                enriched=$($PHP_BIN -n benchmarks/enrich-result.php "$raw_result" "$samples")
                printf '%s\n' "$enriched" >>"$RESULT_FILE"
                $PHP_BIN -n -r '
                    $r = json_decode($argv[1], true, flags: JSON_THROW_ON_ERROR);
                    printf("%-8s %-14s %8d %6d %12.2f %12.2f %12.2f %10.2f\n",
                        $r["runtime"], $r["scenario"], $r["message_bytes"], $r["concurrency"],
                        $r["result"]["messages_per_s"], $r["result"]["payload_mib_per_s"],
                        $r["result"]["operation_latency"]["p99_us"] ?? 0,
                        ($r["worker_rss_kib_peak"] ?? 0) / 1024);
                ' "$enriched"

                cleanup_case
                trap - EXIT
            done
        done
    done
done

echo "JSONL result: $RESULT_FILE"
