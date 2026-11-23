#!/usr/bin/env bash
# 在 Linux 容器中运行全部验证：默认 Alpine/musl（与生产一致），LIBC=glibc 时用
# Debian bookworm/glibc。宿主只需要 docker 或 podman，以及一个
# 运行中的 broker 容器（默认 hi-kafka-test，EXTERNAL 监听 127.0.0.1:9094）。
# 必须在与目标架构相同的机器上原生运行，不要用跨架构模拟。
#
# 用法：
#   tests/linux/run.sh build                       # 构建 builder 与测试镜像
#   tests/linux/run.sh rust php e2e                # 运行指定套件
#   SOAK_DURATION=3600 tests/linux/run.sh soak
#   CONTAINER_CLI=podman CONTAINER_SOCKET=/run/user/$(id -u)/podman/podman.sock tests/linux/run.sh ...
#   LIBC=glibc tests/linux/run.sh build rust php e2e
#
# 环境变量：ARCH（默认取宿主架构）、LIBC（musl | glibc，默认 musl）、BUILD_ARGS
# （透传给构建，例如 --build-arg RUNTIME_IMAGE=...）、RESULTS（soak 报告目录）。
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
LIBC="${LIBC:-musl}"
case "$LIBC" in
    musl) BUILD_DOCKERFILE=Dockerfile.alpine; TEST_DOCKERFILE=tests/linux/Dockerfile; TAG="$ARCH"; TEST_RUSTFLAGS="" ;;
    # ext 的单测二进制引用只有 PHP 进程才提供的符号；glibc 上 rustc 默认 full RELRO
    # （-z now）要求加载时解析全部符号，测试二进制会直接退出。仅测试构建改为 partial。
    glibc) BUILD_DOCKERFILE=Dockerfile; TEST_DOCKERFILE=tests/linux/Dockerfile.glibc; TAG="$ARCH-glibc"; TEST_RUSTFLAGS="-C relro-level=partial" ;;
    *) echo "unknown LIBC $LIBC (musl | glibc)" >&2; exit 2 ;;
esac
# localhost/ 前缀：podman 不解析无 registry 的短名，docker 也接受这种本地 tag。
BUILDER="localhost/hi-kafka-builder:${TAG}"
IMAGE="localhost/hi-kafka-test:${TAG}"
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
    echo "===================== [$ARCH/$LIBC] $suite ====================="
    case "$suite" in
        build)
            # shellcheck disable=SC2086
            "$CLI" build --platform "$PLATFORM" -f "$BUILD_DOCKERFILE" --target builder $BUILD_ARGS -t "$BUILDER" .
            # shellcheck disable=SC2086
            "$CLI" build --platform "$PLATFORM" -f "$TEST_DOCKERFILE" --build-arg "BUILDER=$BUILDER" $BUILD_ARGS -t "$IMAGE" .
            ;;
        rust)
            # builder 镜像里是构建时拷入的源码；挂载当前源码到 /work 运行，target 放在
            # 容器内，避免与宿主（macOS）产物混用。输出先落盘再过滤，退出码取 cargo 的。
            # 不能传空的 RUSTFLAGS：会覆盖 Alpine builder 镜像中必需的 -crt-static 设置。
            rust_env=()
            [[ -n "$TEST_RUSTFLAGS" ]] && rust_env=(-e "RUSTFLAGS=$TEST_RUSTFLAGS")
            "$CLI" run --rm --platform "$PLATFORM" --network host "${rust_env[@]}" \
                -v "$PWD:/work:ro" "$BUILDER" sh -c '
                rm -rf /tmp/w && mkdir /tmp/w && cd /work && tar --exclude=./target --exclude=./php-driver/vendor -cf - . | tar -xf - -C /tmp/w
                cd /tmp/w && CARGO_TARGET_DIR=/src/target with-libclang cargo test --workspace --features hi-kafka-worker/kafka-vendored-tls > /tmp/cargo-test.log 2>&1
                status=$?
                grep -E "test result|FAILED|panicked|^error" /tmp/cargo-test.log
                [ $status -eq 0 ] || tail -40 /tmp/cargo-test.log
                exit $status '
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
            out="/results/soak-${TAG}-$(date +%Y%m%d-%H%M%S)"
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
