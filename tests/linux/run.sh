#!/usr/bin/env bash
# 在 Linux（Alpine/musl）容器中运行全部验证。宿主只需要 docker 或 podman，以及一个
# 运行中的 broker 容器（默认 hi-kafka-test，EXTERNAL 监听 127.0.0.1:9094）。
# 必须在与目标架构相同的机器上原生运行，不要用跨架构模拟。
#
# 用法：
#   tests/linux/run.sh build                       # 构建 builder 与测试镜像
#   tests/linux/run.sh rust php e2e                # 运行指定套件
#   SOAK_DURATION=3600 tests/linux/run.sh soak
#   CONTAINER_CLI=podman CONTAINER_SOCKET=/run/user/$(id -u)/podman/podman.sock tests/linux/run.sh ...
#
# 环境变量：ARCH（默认取宿主架构）、BUILD_ARGS（透传给 Dockerfile.alpine，例如
# --build-arg RUNTIME_IMAGE=...）、RESULTS（soak 报告目录）。
#
# 套件：
#   rust  builder 镜像内 cargo test（Linux 专属路径：close_range、/proc 线程数等）
#   php   php-driver 单测（加载 Linux 版扩展）
#   e2e   tests/php 全部用例，阻塞 Client 与 Swoole 各一遍
#   soak  Swoole 运行时的可靠性 soak（账本 + 故障注入），报告写入 RESULTS 目录
set -euo pipefail

cd "$(dirname "$0")/../.."
case "$(uname -m)" in x86_64) host_arch=amd64 ;; aarch64|arm64) host_arch=arm64 ;; *) host_arch=$(uname -m) ;; esac
ARCH="${ARCH:-$host_arch}"
CLI="${CONTAINER_CLI:-docker}"
SOCKET="${CONTAINER_SOCKET:-/var/run/docker.sock}"
BUILD_ARGS="${BUILD_ARGS:-}"
PLATFORM="linux/${ARCH}"
# localhost/ 前缀：podman 不解析无 registry 的短名，docker 也接受这种本地 tag。
BUILDER="localhost/hi-kafka-builder:${ARCH}"
IMAGE="localhost/hi-kafka-test:${ARCH}"
RESULTS="${RESULTS:-$HOME/hi-kafka-linux-results}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-hi-kafka-test}"
mkdir -p "$RESULTS"

run_in() {
    local image="$1"; shift
    "$CLI" run --rm --platform "$PLATFORM" --network host \
        -v "$PWD:/src" -v "$RESULTS:/results" \
        -v "$SOCKET:/var/run/docker.sock" \
        -e DOCKER_HOST=unix:///var/run/docker.sock \
        -e HI_KAFKA_BROKERS=127.0.0.1:9094 \
        -e HI_KAFKA_E2E_COOPERATIVE_TOPIC="${HI_KAFKA_E2E_COOPERATIVE_TOPIC:-}" \
        -w /src "$image" "$@"
}

PHP_BASE="-d extension=pcntl"
EXT=/opt/hi-kafka/libhi_kafka.so

for suite in "$@"; do
    echo "===================== [$ARCH] $suite ====================="
    case "$suite" in
        build)
            # shellcheck disable=SC2086
            "$CLI" build --platform "$PLATFORM" -f Dockerfile.alpine --target builder $BUILD_ARGS -t "$BUILDER" .
            # shellcheck disable=SC2086
            "$CLI" build --platform "$PLATFORM" -f tests/linux/Dockerfile --build-arg "BUILDER=$BUILDER" $BUILD_ARGS -t "$IMAGE" .
            ;;
        rust)
            # builder 镜像里是构建时拷入的源码；挂载当前源码到 /work 运行，target 放在
            # 容器内，避免与宿主（macOS）产物混用。
            "$CLI" run --rm --platform "$PLATFORM" --network host -v "$PWD:/work:ro" "$BUILDER" sh -c '
                rm -rf /tmp/w && mkdir /tmp/w && cd /work && tar --exclude=./target --exclude=./php-driver/vendor -cf - . | tar -xf - -C /tmp/w
                cd /tmp/w && CARGO_TARGET_DIR=/src/target with-libclang cargo test --workspace --features hi-kafka-worker/kafka-vendored-tls 2>&1 | grep -E "test result|FAILED|panicked|^error|warning: unused" '
            ;;
        php)
            run_in "$IMAGE" sh -c "cd php-driver && php -n $PHP_BASE -d extension=$EXT vendor/bin/phpunit tests"
            ;;
        e2e)
            run_in "$IMAGE" env EXT_PATH=$EXT PHP_EXTRA_ARGS="$PHP_BASE" ./scripts/run-all-e2e.sh
            run_in "$IMAGE" env EXT_PATH=$EXT PHP_EXTRA_ARGS="$PHP_BASE -d extension=swoole" \
                E2E_TESTS="consumer-record-memory-swoole-v2 coroutine-consumer-stream-v2 client-recovery-v2 binary-roundtrip-v2" \
                ./scripts/run-all-e2e.sh
            ;;
        soak)
            out="/results/soak-${ARCH}-$(date +%Y%m%d-%H%M%S)"
            run_in "$IMAGE" php -n $PHP_BASE tests/reliability/soak.php \
                --out="$out" --ext=$EXT --runtime=swoole \
                --php-args="$PHP_BASE -d extension=swoole" \
                --kafka-container="$KAFKA_CONTAINER" \
                --duration="${SOAK_DURATION:-600}" --warmup="${SOAK_WARMUP:-60}" \
                --fault-interval="${SOAK_FAULT_INTERVAL:-60}" \
                --faults="${SOAK_FAULTS:-worker-kill,worker-term,worker-stop,consumer-kill,broker-restart,broker-pause}" \
                --rate="${SOAK_RATE:-200}" ${SOAK_EXTRA:-}
            ;;
        *)
            echo "unknown suite $suite" >&2
            exit 2
            ;;
    esac
done
