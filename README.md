# hi-kafka-ext

Hi Framework Kafka 嵌入式 worker 扩展（Rust）。

- 用户文档：[docs/kafka/](../docs/kafka/integration.md)（架构 / Producer / Consumer / 事务 / 协程驱动 / 多集群 / 运维 / 全局函数）
- 设计文档：[src/Kafka/RFC-EMBEDDED-WORKER.md](../src/Kafka/RFC-EMBEDDED-WORKER.md)

## 仓库结构

```
hi-kafka-ext/
├── proto/                 # IPC 协议（帧编解码、消息类型）
├── ext/                   # PHP 扩展 cdylib（ext-php-rs 实现 + 内嵌 worker 入口）
├── worker/                # worker 核心库（被 ext 静态链接；保留 standalone binary 作 dev 工具）
├── php-driver/            # PSR-4 源码
│   ├── src/Hi/Kafka/
│   │   ├── Client.php             # 扩展类同名桩（class_exists guard，IDE / 无扩展 CI 兜底）
│   │   ├── SwooleClient.php       # Swoole 协程驱动（真实现）
│   │   └── SwowClient.php         # Swow 协程驱动（真实现）
│   └── stubs/
│       └── hi_kafka_functions.php # 全局函数桩（function_exists guard）
├── stubs/                 # 单文件 PHPStan/IDE stub 包（hi/kafka-stubs，单独可发布）
├── composer.json          # 仓库根 composer 包 hi/kafka-driver
├── tests/php/             # e2e PHP 测试脚本
├── scripts/               # build / smoke / kafka 测试
├── Dockerfile             # 跨架构 Linux 构建
└── docker-compose.kafka.yml   # 本地 KRaft 单节点 Kafka
```

## 独立性声明

本项目独立实现，**不依赖、不复用、不集成**任何第三方 PHP 扩展（含 SkyWalking PHP、php-rdkafka 等）的代码或运行时。

## 部署形态

**单 `.so` 分发**：worker 代码已链接进扩展，**部署只需 1 个文件**。

| 产物 | 位置 | 用途 |
| --- | --- | --- |
| `libhi_kafka.so` (Linux) / `.dylib` (macOS) | PHP extension dir | PHP 扩展 + 内嵌 worker（release ~3.9 MB on Linux / ~2.7 MB on macOS） |
| `Hi\Kafka\Client` | 扩展 MINIT 注册 | 阻塞 IO 客户端（PHP-FPM / CLI） |
| `Hi\Kafka\{SwooleClient,SwowClient}` | composer `hi/kafka-driver` | 协程感知 driver（Swoole / Swow） |
| `Hi\Kafka\{Connection,Producer,Consumer}Config` | 框架 `src/Kafka/` | 强类型配置 → librdkafka 参数翻译 |

**工作原理**：扩展首次需要 Kafka 时，在 `.so` 内部 `libc::fork()` 拉起守护子进程，子进程直接跳到 worker 入口（tokio + rdkafka），**不 exec 任何外部二进制**。同 pod 内多 PHP 进程通过 `flock` 互斥，只产 1 个 worker，通过 UDS 共享。子进程 `setsid()` 脱离 PHP 会话 + `prctl(PR_SET_NAME)` 改名 `hi-kafka-worker` 便于 `pgrep` 定位 + 写 PID 文件。

### 构建 `.so`

**一键 Docker（推荐，跨架构）**：

```bash
cd hi-kafka-ext
PHP_VERSION=8.3 ./scripts/build-so.sh
# → dist/hi_kafka-php8.3-linux-arm64.so
# → dist/checksums.txt
```

批量产 3 个 PHP 版本：

```bash
PHP_VERSIONS="8.2 8.3 8.4" ./scripts/build-so.sh
```

跨 CPU 架构（buildx）：

```bash
PLATFORM=linux/amd64 PHP_VERSION=8.3 ./scripts/build-so.sh
PLATFORM=linux/arm64 PHP_VERSION=8.3 ./scripts/build-so.sh
```

**直接 cargo（dev / 已在 Linux 目标机）**：

```bash
cd hi-kafka-ext
cargo build -p hi-kafka --release --features kafka
# 产物：target/release/libhi_kafka.{so,dylib}
```

### CI / Release artifact

GitHub Actions workflow：[`.github/workflows/hi-kafka-ext-ci.yml`](../.github/workflows/hi-kafka-ext-ci.yml)

矩阵：
- **test**：PHP 8.2/8.3/8.4 × {Ubuntu, macOS 8.3} → cargo fmt/clippy/test + smoke
- **integration**：跑真实 Kafka，验证 producer / consumer / 事务 / OAUTH / seek / rebalance / pause-resume 完整 e2e
- **release-build**：PHP 8.2/8.3/8.4 × {linux-x86_64, linux-aarch64} → 上传 `libhi_kafka.so` + checksum

### 业务镜像示例

```dockerfile
FROM php:8.3-fpm
RUN apt-get update && apt-get install -y libsasl2-2 zlib1g libcurl4 && rm -rf /var/lib/apt/lists/*
COPY hi_kafka.so /usr/lib/php/20230831/
RUN echo "extension=hi_kafka.so" > /usr/local/etc/php/conf.d/30-hi-kafka.ini
# 完。无 worker binary、无 env var、无 INI 配置。
```

业务代码 4 行投产：

```php
$c = new Hi\Kafka\Client();
$c->registerCluster('default', ['bootstrap.servers' => $brokers]);
$c->produceSync('default', 'topic', $key, $value);
```

## Composer / IDE 接入

仓库根 `composer.json` 发布为 `hi/kafka-driver`，一站式获得：

- `Hi\Kafka\Client` 桩（`class_exists` guard，扩展存在时让位给真实现）
- `Hi\Kafka\SwooleClient` / `Hi\Kafka\SwowClient` 协程驱动（真实现）
- `hi_kafka_*` 全局函数桩（`function_exists` guard，每次请求 require 安全无副作用）

```bash
composer require hi/kafka-driver
```

```json
{
    "require": { "hi/kafka-driver": "^0.1" }
}
```

效果：

| 环境 | `Hi\Kafka\Client` | 全局函数 |
| --- | --- | --- |
| 装了扩展（生产 / 测试） | 扩展真实现 | 扩展真实现 |
| 没装扩展（开发机 / IDE / CI 索引） | composer 桩，no-op 但类型可解析 | composer 桩，no-op |

PHPStan / Psalm / PHPStorm 自动识别签名，**无需手工 "Mark As Reference File"**。如果只想要单文件 stubs（不走 composer），仍可用 `stubs/hi_kafka.php`（包 `hi/kafka-stubs`，专门给 PHPStan 用）。

## 开发

### 依赖

- Rust 1.85+
- PHP 8.2+（含 `php-config` 在 PATH 中）
- 构建 Linux 扩展时还需 `libclang`（ext-php-rs 用 bindgen）

### 构建

```bash
# 单独 check
cargo check --workspace

# 构建（debug）
./scripts/build-dev.sh

# release
PROFILE=release ./scripts/build-dev.sh

# 构建并安装扩展到 php extension dir
INSTALL=1 ./scripts/build-dev.sh
```

### 单元测试

```bash
cargo test --workspace
```

### Smoke / e2e 测试

```bash
# LoggingProducer 路径（不依赖 Kafka）
./scripts/smoke-test.sh

# 真实 Kafka
docker compose -f docker-compose.kafka.yml up -d

EXT=$(realpath target/debug/libhi_kafka.dylib)
HI_KAFKA_BROKERS=127.0.0.1:9094 \
    php -d extension=$EXT tests/php/integration.php /tmp/x.sock topic-$$ --with-kafka
```

`tests/php/` 全套 e2e 脚本：

| 脚本 | 覆盖范围 |
| --- | --- |
| `integration.php` | producer + consumer 全链路 + 池命中率断言 |
| `recovery.php` | kill worker 后 producer 自愈（实测 ~62 ms） |
| `consumer-recovery.php` | kill worker 后 consumer virtual_id 自动重订阅（实测 ~5.5 s） |
| `control-recovery.php` | 控制面（registerCluster / setOAuthBearerToken）自愈 |
| `replay-recovery.php` | seek + replay 场景下的崩溃恢复 |
| `configs.php` | ConnectionConfig + ProducerConfig + ConsumerConfig 联用 |
| `transaction.php` | 事务 producer：begin / commit / abort + read_committed 隔离 |
| `consumer-in-txn.php` | EOS Stream（KIP-447）：sendOffsetsToTransaction |
| `rebalance.php` + `_rebalance_joiner.php` | rebalance 事件通知 + 多消费者协调 |
| `seek.php` | 精确 seek by offset / by timestamp |
| `pause-resume.php` | per-partition pause / resume |
| `partition-timestamp.php` | 显式 partition / timestamp 写入 |
| `headers.php` | message header 编解码 |
| `binary.php` | binary-safe `produceFnfBin` / `produceSyncBin` |
| `oauth-smoke.php` | SASL/OAUTHBEARER token 推送 |
| `swoole-client.php` + `swoole-phase3.php` | SwooleClient 端到端 |
| `_swow_load_check.php` | Swow 运行时探测 helper |

## PHP API 速览

> 完整签名、参数表与跨集群示例移到了用户文档：
> - [全局函数（hi_kafka_*）](../docs/kafka/functions.md)
> - [Producer](../docs/kafka/producer.md) / [Consumer](../docs/kafka/consumer.md) / [事务 + EOS Stream](../docs/kafka/transactions.md)
> - [多集群 / 云上](../docs/kafka/clusters.md) / [协程驱动](../docs/kafka/coroutines.md) / [运维](../docs/kafka/operations.md)

### `Hi\Kafka\Client`（阻塞 IO，PHP-FPM / CLI）

扩展 MINIT 注册的同名类，22 个公开方法分四组：

```php
$c = new Hi\Kafka\Client(/* ?string $socket = null */);

// === 控制面 ===
$c->socket(): string;
$c->ensureWorker(): void;
$c->registerCluster(string $cluster, array $config, ?int $timeoutMs = null): void;

// === Producer ===
$c->produceFnf($cluster, $topic, $key, $value, $headers = [], ?$partition = null, ?$ts = null): void;
$c->produceSync($cluster, $topic, $key, $value, $headers = [], ?$p = null, ?$ts = null, ?$t = null): array;
$c->produceFnfBin($cluster, $topic, $key, $value, $headerNames, $headerValues, ?$p = null, ?$ts = null): void;
$c->produceSyncBin($cluster, $topic, $key, $value, $headerNames, $headerValues, ?$p = null, ?$ts = null, ?$t = null): array;
// Bin 版接受任意字节 key/value/header value，用于 protobuf / msgpack / 加密 payload

// === Consumer ===
$sub = $c->subscribe($cluster, $groupId, $topics, ?$config = null, ?$timeoutMs = null): int;
$c->poll($sub, $maxMessages, $timeoutMs): array;
$c->commit($sub, ?$timeoutMs = null): void;
$c->unsubscribe($sub): void;
$c->pollRebalanceEvents($sub, ?$maxEvents = null, ?$timeoutMs = null): array;
$c->seek($sub, $topics, $partitions, $offsets, ?$timeoutMs = null): void;
$c->seekToTimestamp($sub, $timestampMs, $topics, $partitions, ?$timeoutMs = null): void;
$c->pause($sub, $topics, $partitions, ?$timeoutMs = null): void;
$c->resume($sub, $topics, $partitions, ?$timeoutMs = null): void;

// === 事务 + EOS Stream + SASL/OAUTHBEARER ===
$c->beginTransaction($cluster, ?$timeoutMs = null): void;
$c->commitTransaction($cluster, ?$timeoutMs = null): void;
$c->abortTransaction($cluster, ?$timeoutMs = null): void;
$c->sendOffsetsToTransaction($producerCluster, $sub, $groupId, $topics, $partitions, $offsets, ?$t = null): void;
$c->setOAuthBearerToken($cluster, $token, $lifetimeMs, $principalName, $extensions = [], ?$t = null): void;
```

### 全局函数（`hi_kafka_*`）

34 个全局函数分三层。**业务级 14 个**（与 Client 实例方法语义对应、`$socket` 参数可选）：

```php
// 元信息 / 观测
hi_kafka_version(): string
hi_kafka_runtime(): array              // ['blocking'] 或 ['blocking', 'swoole', ...]
hi_kafka_pool_stats(): array           // 各 socket 的连接池统计
hi_kafka_retry_stats(): array          // producer IPC 自动重试（worker 崩溃恢复）
hi_kafka_resubscribe_stats(): array    // consumer 自动重订阅

// 控制面
hi_kafka_ensure_worker(?string $socket = null): void
hi_kafka_register_cluster(string $cluster, array $config, ?string $socket = null, ?int $timeoutMs = null): void

// Producer / Consumer
hi_kafka_produce_fnf(...): void
hi_kafka_produce_sync(...): array
hi_kafka_subscribe(...): int
hi_kafka_poll(int $sub, int $max, int $timeoutMs): array
hi_kafka_commit(int $sub, ?int $timeoutMs = null): void
hi_kafka_unsubscribe(int $sub): void
```

**协议编解码原语 19 个**（`@internal`，给 PHP 协程 driver 用——业务代码不应调用）：

```php
hi_kafka_next_cid() / hi_kafka_header_len() / hi_kafka_parse_header()
hi_kafka_encode_hello_frame() / hi_kafka_verify_hello_resp()
hi_kafka_encode_fnf_frame() / hi_kafka_encode_req_frame() / hi_kafka_decode_resp_frame()
hi_kafka_encode_register_cluster_frame()
hi_kafka_encode_subscribe_frame() / hi_kafka_encode_poll_frame()
hi_kafka_encode_commit_frame() / hi_kafka_encode_unsubscribe_frame()
hi_kafka_decode_consumer_resp()
hi_kafka_encode_pause_resume_frame() / hi_kafka_encode_seek_by_offset_frame()
hi_kafka_encode_seek_by_timestamp_frame() / hi_kafka_encode_txn_frame()
hi_kafka_encode_send_offsets_frame() / hi_kafka_encode_set_oauth_token_frame()
hi_kafka_encode_poll_rebalance_frame()
```

### `Hi\Kafka\SwooleClient` / `Hi\Kafka\SwowClient`（协程感知）

纯 PHP 实现（不在扩展里），协议编解码复用扩展暴露的 `hi_kafka_encode_*_frame()` / `hi_kafka_decode_*_resp()` 原语——**单一协议源**。IO 走 `Swoole\Coroutine\Socket` / `Swow\Socket`，调度器自动 yield。

源文件：[php-driver/src/Hi/Kafka/SwooleClient.php](php-driver/src/Hi/Kafka/SwooleClient.php) / [SwowClient.php](php-driver/src/Hi/Kafka/SwowClient.php)

```php
use Swoole\Coroutine;

Coroutine\run(function () {
    $client = new Hi\Kafka\SwooleClient();
    $client->registerCluster('main', ['bootstrap.servers' => 'kafka:9094']);

    $r = $client->produceSync('main', 'orders', 'k', 'v', 5000);

    $sub = $client->subscribe('main', 'order-group', ['orders'], [
        'auto.offset.reset' => 'earliest',
    ]);
    while ($running) {
        foreach ($client->poll($sub, 100, 1000) as $msg) { process($msg); }
        $client->commit($sub);
    }
    $client->unsubscribe($sub);
});
```

`SwowClient` 接口完全对称，构造参数 `connectTimeoutMs` 为毫秒（Swoole 是 `connectTimeout` 秒）。详见 [docs/kafka/coroutines.md](../docs/kafka/coroutines.md)。

**协程并发实测**：
- 20 个协程并发 `produceSync` → 8.6 ms 全部完成
- 单协程 `poll` 阻塞 3.2 s 期间，后台计数协程跑了 500 次 → reactor 完全不阻塞

### 强类型配置 — `Hi\Kafka\{Connection,Producer,Consumer}Config`

避免散乱字符串键，用配置类把 librdkafka 全套参数包装好：

```php
use Hi\Kafka\{Client, ConnectionConfig, ProducerConfig, ConsumerConfig, ConsumeOffsetType};

$conn = new ConnectionConfig(
    brokers: ['kafka-1:9094', 'kafka-2:9094'],
    sasl: ['mechanism' => 'SCRAM-SHA-512', 'username' => env('KAFKA_USER'), 'password' => env('KAFKA_PWD')],
    ssl:  ['root_ca' => '/etc/ssl/kafka-ca.pem'],
);

$prod = (new ProducerConfig())
    ->setCompressionType('lz4')->setAcks('all')->setIdempotent(true)->setLingerMs(5);

$client = new Client();
$client->registerCluster('main', [
    ...$conn->toLibrdkafkaConfig(),   // 自动判定 security.protocol = SASL_SSL
    ...$prod->toLibrdkafkaConfig(),
]);

$cons = (new ConsumerConfig())
    ->setGroupId('order-processor')
    ->setOffset(ConsumeOffsetType::AtStart)        // → auto.offset.reset=earliest
    ->setIsolationLevel('read_committed')           // 仅消费已提交事务消息
    ->setPartitionAssignmentStrategy('cooperative-sticky');
$sub = $client->subscribe('main', $cons->getGroupId(), ['orders'], $cons->toLibrdkafkaConfig());
```

`ConnectionConfig` 按字段自动判定 `security.protocol`：

| 配置组合 | 翻译为 |
|---|---|
| `brokers` 单独 | `PLAINTEXT` |
| `brokers + sasl` | `SASL_PLAINTEXT` |
| `brokers + ssl` | `SSL` + `ssl.ca.location` |
| `brokers + ssl[cert,key]` | `SSL` 双向 mTLS |
| `brokers + sasl + ssl` | `SASL_SSL`（公网生产典型） |

`extra` 字段可注入**任意 librdkafka 键**，librdkafka 全套 200+ 参数无死角。

## Worker（dev tool）

主路径下 worker **内嵌进 .so**，无需独立部署。但 `worker/` crate 仍可单独 build 出 standalone binary，方便单独调试 / 性能压测 / 配 systemd：

```bash
cargo build -p hi-kafka-worker --features kafka --release
./target/release/hi-kafka-worker --socket /tmp/hi-kafka.sock --brokers 127.0.0.1:9094
```

| 选项 | 环境变量 | 默认 | 说明 |
| --- | --- | --- | --- |
| `--socket` | `HI_KAFKA_SOCKET` | `/tmp/hi-kafka.sock` | Unix socket 监听路径 |
| `--brokers` | `HI_KAFKA_BROKERS` | (无) | 启动时预注册 `default` 集群（仅 dev 兼容）|
| `--log-level` | `HI_KAFKA_LOG_LEVEL` | `info` | 日志级别 |
| `--drain-timeout-ms` | `HI_KAFKA_DRAIN_TIMEOUT_MS` | `10000` | SIGTERM 后 drain 超时 |
| `--metrics-addr` | `HI_KAFKA_METRICS_ADDR` | (关) | Prometheus 端点；空 = 不开 |

> 生产环境用单 `.so`，所有集群配置走 `Hi\Kafka\Client::registerCluster()`，不依赖 `--brokers`。

## 扩展端配置（ini / env）

详见 [docs/kafka/operations.md → 关键配置](../docs/kafka/operations.md#关键配置)。优先级：**env > ini > 内置默认**。

| ini 名 | env 名 | 默认 | 含义 |
| --- | --- | --- | --- |
| `hi_kafka.log_level` | `HI_KAFKA_LOG_LEVEL` | `info` | worker tracing 日志级别 |
| `hi_kafka.log_file` | `HI_KAFKA_LOG_FILE` | (stderr) | 日志重定向到文件 |
| `hi_kafka.drain_timeout_ms` | `HI_KAFKA_DRAIN_TIMEOUT_MS` | `10000` | SIGTERM grace（ms） |
| `hi_kafka.metrics_addr` | `HI_KAFKA_METRICS_ADDR` | (关) | Prometheus `/metrics`；显式给 `host:port` 才开 |

仅 env 配置（背压水位 / socket 路径 / 预注册 broker）见上述文档。

## Metrics

显式配 `hi_kafka.metrics_addr` 后启用，`http://<host>:<port>/metrics`：

```
hi_kafka_worker_uptime_seconds (gauge)
hi_kafka_worker_info{version="..."} (gauge)
hi_kafka_ipc_frames_total / hi_kafka_ipc_connections_total
hi_kafka_produce_fnf_total / hi_kafka_produce_fnf_failed_total
hi_kafka_produce_req_total / hi_kafka_produce_resp_ok_total / hi_kafka_produce_resp_err_total
hi_kafka_frames_dropped_draining_total
```

`/healthz` 返回 `ok` + 200。

PHP 端可观测：

```php
hi_kafka_pool_stats();         // 每 socket 的 acquires / hits / misses / closed / poisoned
hi_kafka_retry_stats();        // producer IPC 自动重试（worker 崩溃）次数 / 成功 / 失败
hi_kafka_resubscribe_stats();  // consumer 自动重订阅次数 / 成功 / 失败
```

## 状态

### Phase 1 — MVP（已完成 ✅）

- [x] Workspace skeleton（proto / worker / ext 三 crate）
- [x] IPC 帧编解码（13 B header：len/type/cid + payload）
- [x] PRODUCE_FNF payload v1 + Worker tokio UDS listener
- [x] Producer 抽象 + LoggingProducer + KafkaProducer (feature gated)
- [x] PHP 扩展（ext-php-rs）暴露基础 produce / ensure_worker / version
- [x] Worker 自动启动（`flock` 互斥 + `setsid` 守护化）
- [x] 真实 Kafka e2e + 并发 PHP 进程 autospawn 互斥验证
- [x] Docker Compose 本地 Kafka 环境

### Phase 2 — 生产可用（已完成 ✅）

**协议 + 同步 ack**：
- [x] PRODUCE_REQ / PRODUCE_RESP（cid 路由 + 同步 ack）

**生产可靠性**：
- [x] 优雅停机 drain（SIGTERM → 拒绝新 REQ → flush rdkafka → 退出）
- [x] Prometheus `/metrics` + `/healthz` + 11 项 worker 指标
- [x] `Hi\Kafka\Client` PHP 类封装

**性能**：
- [x] UDS 连接池（全局共享、RAII、半关闭探测、poison 机制）
- [x] 池效果：300 produce → 99.67 % 命中率
- [x] `ensure_worker` 探测缓存（300 produces 仅 2 个连接，1259 ms → 4.4 ms）

**协程**：
- [x] 协程运行时检测 `hi_kafka_runtime()`
- [x] Swoole 协程 driver + 协议原语
- [x] 20 协程并发 produceSync 8.6 ms

**Consumer 完整闭环**：
- [x] Consumer 协议 SUBSCRIBE / POLL / COMMIT / UNSUBSCRIBE
- [x] Worker Consumer trait + LoggingConsumer + KafkaConsumer
- [x] PHP consumer API：4 个全局函数 + Client 类 4 个方法

**崩溃自愈**：
- [x] 扩展端自动重试 worker 崩溃：BrokenPipe / EOF / connect-refused 透明 invalidate + ensure_worker + 重试
- [x] Producer 自愈 e2e：62 ms 内透明完成
- [x] Consumer virtual_id + 自动重订阅
- [x] Consumer 自愈 e2e：5.5 s 内拿到新消息

**工程化**：
- [x] 综合集成测试 `tests/php/integration.php` 7 阶段回归
- [x] Dockerfile 跨架构、`build-so.sh`、GitHub Actions CI 矩阵
- [x] 单 `.so` 分发（worker 内嵌进扩展），release ~3.9 MB (Linux) / ~2.7 MB (macOS)

**集群配置 PHP 化**：
- [x] REGISTER_CLUSTER 协议帧 + worker 端 `ClusterRegistry`
- [x] `registerCluster()` PHP API（Client / SwooleClient / 全局函数）
- [x] 多集群同时连接（每集群独立 librdkafka 实例 / 连接池 / 故障域）
- [x] `socket` 参数全局可选
- [x] `ConnectionConfig` / `ProducerConfig` / `ConsumerConfig` 三件套

### Phase 3 — 高级 Consumer / 事务（已完成 ✅）

**精确 seek**：
- [x] `seek(sub, topics, partitions, offsets)` 按 offset
- [x] `seekToTimestamp(sub, timestampMs, topics, partitions)` 按时间戳
- [x] e2e：`tests/php/seek.php`、`tests/php/replay-recovery.php`

**Per-message headers / partition / timestamp**：
- [x] `produceFnf` / `produceSync` 支持 `headers` / `partition` / `timestampMs`
- [x] Binary-safe 路径 `produceFnfBin` / `produceSyncBin`
- [x] e2e：`tests/php/headers.php`、`partition-timestamp.php`、`binary.php`

**Pause / Resume + 自动背压**：
- [x] `pause` / `resume` per-partition fetch 控制
- [x] worker 内 **hysteresis 双水位**自动 pause / resume librdkafka fetcher（条数 + 字节两维度）
- [x] env 可调 `HI_KAFKA_CONSUMER_{PAUSE,RESUME}_AT{,_BYTES}` + Prometheus `hi_kafka_consumer_auto_pause/resume_total`
- [x] e2e：`tests/php/pause-resume.php`

> hysteresis 是 credit-based 背压的本地等价实现：worker 内存上限 + broker 端留存消息。
> 业务侧 `pause` / `resume` 与自动背压**两层叠加生效**。

**Worker panic 防护**：
- [x] 每帧 dispatch 整体 `catch_unwind`（10 个 handler 路径全覆盖），panic 时关连接 + 客户端 EOF 重试
- [x] `handle_produce_req` 内部精细 panic guard，尽力写 `ProduceResp::Err(retryable=true)` 让客户端透明重试而非 EOF
- [x] 多层 panic backtrace 写入日志便于事后定位
- [x] 实现：`worker/src/server.rs:285-313`（dispatch 层）+ `:771-789`（produce_ack 层）

**Transactional producer + EOS Stream（KIP-447）**：
- [x] `beginTransaction` / `commitTransaction` / `abortTransaction`
- [x] `sendOffsetsToTransaction`（输出 + offset 原子提交）
- [x] 多 producer 共存（同 broker 不同 cluster 名，事务 / 非事务隔离）
- [x] e2e：`tests/php/transaction.php`、`consumer-in-txn.php`

**Rebalance 事件**：
- [x] `pollRebalanceEvents(sub, maxEvents, timeoutMs)` 拉 assign / revoke / error
- [x] e2e：`tests/php/rebalance.php` + `_rebalance_joiner.php`（多消费者协调）

**SASL/OAUTHBEARER 动态 token**：
- [x] `setOAuthBearerToken(cluster, token, lifetimeMs, principalName, extensions)`
- [x] PHP 侧业务自取 token（AWS STS / k8s SA / 自有 OIDC），周期 push
- [x] e2e：`tests/php/oauth-smoke.php`、`control-recovery.php`

**Swow driver**：
- [x] `Hi\Kafka\SwowClient`（接口对称 SwooleClient，IO 走 Swow\Socket）
- [x] 协议编解码完全复用扩展原语

**协程驱动**：
- [x] SwooleClient Phase 3 全面对齐 Client（pause / seek / 事务 / OAUTH）
- [x] e2e：`tests/php/swoole-phase3.php`

### Phase 4 — 待规划

> 自动背压（hysteresis）与 worker panic 防护已在 Phase 3 完成，详见上一节。
> 剩余项均**未启动**，目前没有强业务驱动；优先级会随 issue 反馈调整。

- [ ] **Producer 异步队列前置**：PHP `produceSync` 落本地队列立即返回 promise / future，worker 后台 flush。当前 `produceSync` 已经够快（broker 端 ack 即返回），主要价值在 PHP-FPM 短请求场景把 broker 往返从请求路径剔除。
- [ ] **Schema Registry 透传**：仅做 magic byte + schema id 编解码，不做 schema 解析。今天业务侧自己 pack/unpack 完全够用。
- [ ] **Admin API**：CreateTopics / DescribeConfigs / AlterConfigs。dev / 运维工具，业务运行时不需要；用 `kafka-topics.sh` 也能搞定。
- [ ] **单 subscription 流重建**：rdkafka stream 出错时只重建该 subscription，避免整 worker 重启。当前 worker panic 防护已能保证不挂，但 `stream_loop` 出错走的是退订-重订路径，理论上可以做得更轻。

详见 [RFC §12 实施计划](../src/Kafka/RFC-EMBEDDED-WORKER.md#12-实施计划)。
