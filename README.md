# hi-kafka

Rust 实现的 PHP Kafka 扩展与 pod 内共享 worker。

> [ARCHITECTURE.md](ARCHITECTURE.md) 是架构设计的唯一事实源。README 只说明构建、使用和验证入口；任何实现或示例与架构文档冲突时，以架构文档为准。

## 运行模型

- 每个 socket namespace 一个 worker，pod 内 PHP-FPM、CLI、Swoole 或 Swow worker 共享它。
- 每个 cluster generation 共享普通 Producer，保留 librdkafka batching 和连接复用。
- 每个逻辑 subscription 独占一个 librdkafka Consumer 和一个全双工 UDS stream。
- Consumer 一次交付一条 `ConsumerRecord`，不存在 batch poll、客户端消息队列或 overflow eviction。
- ACK 只推进连续 watermark；commit 永远不会越过未 ACK gap。
- message credit、byte credit、per-subscription 上限和 worker 全局 permit 共同约束内存。
- 瞬时 UDS 断开通过 subscription ID、epoch 和不可伪造 resume token 恢复；旧 epoch 的 ACK 会被拒绝。

## 目录

```text
ARCHITECTURE.md   架构设计唯一事实源
proto/            protocol v2 帧与有界 codec
worker/           resource actors、librdkafka backend、生命周期与 metrics
ext/              ext-php-rs 扩展、阻塞 Client/ConsumerStream
php-driver/       Swoole/Swow coroutine clients 和 IDE stubs
tests/            Rust/PHP correctness tests
scripts/          构建、镜像和回归入口
```

## 构建

要求 Rust 1.85+、PHP 8.2+、`php-config` 和 libclang。

```bash
cargo check --workspace
cargo test --workspace
cargo build -p hi-kafka --release --features kafka
```

Linux 多架构产物：

```bash
PHP_VERSION=8.3 ./scripts/build-so.sh
PHP_VERSIONS="8.2 8.3 8.4" ./scripts/build-so.sh
PLATFORM=linux/arm64 PHP_VERSION=8.3 ./scripts/build-so.sh
```

扩展和 worker 静态链接为同一个 `hi_kafka.so`；运行镜像不需要 sidecar 或独立 worker binary。

## Blocking API

```php
use Hi\Kafka\Client;

$client = new Client('/run/hi-kafka/worker-v2.sock');
$client->registerCluster('default', [
    'bootstrap.servers' => 'kafka:9092',
]);

$client->produceSync('default', 'events', 'key', 'value');

$stream = $client->consume('default', 'consumer-group', ['events'], [
    'auto.offset.reset' => 'earliest',
]);

while (true) {
    $message = $stream->next(30_000);
    if ($message === null) {
        continue;
    }
    try {
        handle($message);
        $stream->ack($message);
        $stream->commit();
    } catch (Throwable $error) {
        $stream->nack($message, $error);
        throw $error;
    }
}
```

`Hi\Kafka\ConsumerRecord` 暴露：

```text
subscriptionId()  subscriptionEpoch()  deliveryId()
topic()  partition()  offset()  timestampMs()
key()  value()  headers()  wireSize()
```

暂停或恢复完整 assignment 时传空数组；指定 partition 时使用下列结构：

```php
$partitions = [
    ['topic' => 'events', 'partition' => 0],
    ['topic' => 'events', 'partition' => 1],
];
$stream->pause($partitions);
$stream->resume($partitions);
$stream->close();
```

## Coroutine API

Composer autoload 后，在协程内使用相同语义的 driver：

```php
$client = new Hi\Kafka\SwooleClient('/run/hi-kafka/worker-v2.sock');
// 或 new Hi\Kafka\SwowClient(...)

$client->registerCluster('default', ['bootstrap.servers' => 'kafka:9092']);
$stream = $client->consume('default', 'consumer-group', ['events']);
$message = $stream->next();
if ($message !== null) {
    $stream->ack($message);
}
```

Blocking、Swoole 和 Swow 都使用独立 Consumer stream、相同 frame codec、epoch 校验和有界 reconnect/resume；不会退回 RPC poll。

worker 崩溃或被其它进程重启后，三种 Client 都会自动恢复：重新探测 / 拉起 worker，收到 `CLUSTER_NOT_REGISTERED` 时重放本进程注册过的集群与 OAuth token，然后重试一次（超时不重试，结果未知）。fork 后子进程自动丢弃继承的连接。

worker 在第一次使用时自动拉起，任何运行时、任何时刻都可以：扩展 double fork 后立即 exec 一个只加载本扩展的 PHP CLI（`ps` 中显示为 `hi-kafka-worker -n -d extension=... -- <socket>`），不继承业务进程的线程、锁和内存。PHP CLI 默认使用当前进程自身（FPM 下为同一安装的 `php`），也可以用 `hi_kafka.php_binary` / `HI_KAFKA_PHP_BINARY` 指定；找不到时退回为在 fork 出的进程中运行 worker，并记录 warning。需要消除首次调用的冷启动延迟时，可以在启动阶段调用一次 `ensureWorker()` 预热。

## 内存预算

主要环境变量：

| 变量 | 默认值 | 作用 |
| --- | ---: | --- |
| `HI_KAFKA_MAX_SUBSCRIPTIONS` | 1024 | worker subscription 硬上限 |
| `HI_KAFKA_CONSUMER_MAX_MESSAGES` | 256 | 单 subscription 最大 in-flight 条数 |
| `HI_KAFKA_CONSUMER_MAX_BYTES` | 16 MiB + frame header | 单 subscription 最大 in-flight 字节；始终受 protocol 单帧上限约束 |
| `HI_KAFKA_GLOBAL_IN_FLIGHT_MESSAGES` | 65536 | worker 全局交付条数预算 |
| `HI_KAFKA_GLOBAL_IN_FLIGHT_BYTES` | 512 MiB | worker 全局交付字节预算 |
| `HI_KAFKA_CONSUMER_RESUME_GRACE_MS` | 10000 | 断线后保留 Consumer 的恢复窗口 |
| `HI_KAFKA_WORKER_THREADS` | 可用 CPU 数（遵守容器 CPU limit） | worker 的 tokio 工作线程数。调低可减少 worker 内存（48 核机器上设为 8，负载时 RSS 少 20~80 MiB），代价是单进程上百个 Consumer stream 时消费吞吐下降 30%~45% |

Producer 默认值（集群配置中显式设置时以业务为准）：

| librdkafka 配置 | 默认值 | 说明 |
| --- | ---: | --- |
| `queue.buffering.max.kbytes` | 131072（128 MiB） | 共享 Producer 本地队列上限。librdkafka 自身默认 1 GiB 是按每进程一个客户端设计的；一个 worker 服务整个节点时会在 broker 变慢时涨到 GiB 级 |
| `compression.codec` | `lz4` | 未设置 `compression.codec` / `compression.type` 时生效 |

本地队列满时 `produceFnf` 最多等待 1 秒、`produceSync` 在其 `timeoutMs` 内等待空间，仍满则抛出可重试的 `PRODUCER_QUEUE_FULL`。key、value 与 header 值均为任意字节（不要求 UTF-8）。key 和 value 可以传 `null`：null key 由 partitioner 分配分区，null value 即 tombstone（compacted topic 的删除标记），与空串是不同的值。

真实 backend 在 borrowed Kafka record 被复制前申请精确全局 permit；detach/encode 瞬时双份内容按两倍 wire size 计费，编码后只保留一份。librdkafka 本地队列也按协商 byte 上限配置。RSS 验收方法和 allocator tolerance 见架构文档的 release gates。

## 验证

```bash
cargo fmt --all --check
cargo test --workspace
cargo check -p hi-kafka-worker --features kafka
find php-driver/src php-driver/stubs -name '*.php' -print0 | xargs -0 -n1 php -l
```

需要真实 broker 的回归：

```bash
docker compose -f docker-compose.kafka.yml up -d
EXT_PATH="$PWD/target/debug/libhi_kafka.so" ./scripts/run-all-e2e.sh
# 协程用例需要额外加载运行时扩展（swoole 与 swow 不能同时加载）
PHP_EXTRA_ARGS="-d extension=swoole" ./scripts/run-all-e2e.sh
# cooperative-sticky 用例需要预建多分区 topic
HI_KAFKA_E2E_COOPERATIVE_TOPIC=coop-e2e ./scripts/run-all-e2e.sh
HI_KAFKA_E2E_MESSAGES=2048 EXT_PATH="$PWD/target/debug/libhi_kafka.so" \
    ./scripts/run-consumer-memory-e2e.sh
BENCH_MESSAGES=100000 BENCH_WARMUP=10000 \
    EXT_PATH="$PWD/target/release/libhi_kafka.so" ./benchmarks/run.sh
```

吞吐、P99、RSS/CPU 采样和基线回归比较方法见 [`benchmarks/README.md`](benchmarks/README.md)。

镜像产物检查：

```bash
./scripts/docker-image-verify.sh your-image:tag
```

## 不兼容性

protocol v2 是有意的破坏性重构：v1 `subscribe/poll/unsubscribe`、batch message array、客户端权威 offsets、poll rebalance 和虚拟 subscription 映射均已删除。v1 与 v2 使用不同 socket namespace，不提供永久双栈或兼容 shim。
