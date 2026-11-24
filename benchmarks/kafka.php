<?php

declare(strict_types=1);

require __DIR__ . '/../php-driver/vendor/autoload.php';

/** @return never */
function fail(string $message): void
{
    fwrite(STDERR, "benchmark error: {$message}\n");
    exit(2);
}

/** @return array<string,string> */
function parseClusterConfig(string $brokers): array
{
    $config = [
        'bootstrap.servers' => $brokers,
        'acks' => 'all',
        'linger.ms' => '5',
    ];
    $json = getenv('HI_KAFKA_BENCH_CLUSTER_CONFIG');
    if ($json === false || $json === '') {
        return $config;
    }
    $extra = json_decode($json, true, flags: JSON_THROW_ON_ERROR);
    if (! is_array($extra)) {
        fail('HI_KAFKA_BENCH_CLUSTER_CONFIG must be a JSON object');
    }
    foreach ($extra as $key => $value) {
        if (! is_string($key) || ! is_scalar($value)) {
            fail('cluster config keys and values must be scalar strings');
        }
        $config[$key] = (string) $value;
    }
    return $config;
}

/** @return list<int> */
function shards(int $total, int $concurrency): array
{
    $base = intdiv($total, $concurrency);
    $remainder = $total % $concurrency;
    $counts = [];
    for ($i = 0; $i < $concurrency; $i++) {
        $counts[] = $base + ($i < $remainder ? 1 : 0);
    }
    return $counts;
}

function percentile(array $values, float $quantile): ?float
{
    if ($values === []) {
        return null;
    }
    sort($values, SORT_NUMERIC);
    $index = (int) ceil($quantile * count($values)) - 1;
    return round((float) $values[max(0, $index)], 3);
}

/** @return array{mean_us:?float,p50_us:?float,p95_us:?float,p99_us:?float,max_us:?float,samples:int} */
function latencySummary(array $values): array
{
    return [
        'mean_us' => $values === [] ? null : round(array_sum($values) / count($values), 3),
        'p50_us' => percentile($values, 0.50),
        'p95_us' => percentile($values, 0.95),
        'p99_us' => percentile($values, 0.99),
        'max_us' => $values === [] ? null : round(max($values), 3),
        'samples' => count($values),
    ];
}

function sampleStride(int $messages): int
{
    return max(1, (int) ceil($messages / 10_000));
}

/**
 * 测量区间边界上的 CPU 快照：PHP 进程用 getrusage，worker 读 /proc/<pid>/stat 的
 * utime + stime。ps 的 %cpu 是进程生命周期平均值，会把预装、warm-up 算进去，
 * 不能用来算每条消息的 CPU。非 Linux 上 worker 一项为 null。
 *
 * @return array{php_us:int,worker_ticks:?int}
 */
function cpuSnapshot(string $socket): array
{
    $usage = getrusage();
    $phpUs = $usage['ru_utime.tv_sec'] * 1_000_000 + $usage['ru_utime.tv_usec']
        + $usage['ru_stime.tv_sec'] * 1_000_000 + $usage['ru_stime.tv_usec'];
    $workerTicks = null;
    $pid = @file_get_contents($socket . '.pid');
    if ($pid !== false && ($stat = @file_get_contents('/proc/' . (int) trim($pid) . '/stat')) !== false) {
        // comm 字段可能含空格，从最后一个 ')' 之后切分；utime/stime 是第 14、15 个字段。
        $fields = explode(' ', substr($stat, strrpos($stat, ')') + 2));
        $workerTicks = (int) $fields[11] + (int) $fields[12];
    }
    return ['php_us' => $phpUs, 'worker_ticks' => $workerTicks];
}

/** @return array<string,?float> */
function cpuSummary(array $workers, int $messages, float $durationSeconds): array
{
    usort($workers, static fn (array $a, array $b): int => $a['start_ns'] <=> $b['start_ns']);
    $first = $workers[0]['cpu_start'];
    usort($workers, static fn (array $a, array $b): int => $b['end_ns'] <=> $a['end_ns']);
    $last = $workers[0]['cpu_end'];
    // Linux 上 USER_HZ 固定为 100。
    $workerSeconds = $first['worker_ticks'] === null || $last['worker_ticks'] === null
        ? null : ($last['worker_ticks'] - $first['worker_ticks']) / 100;
    $phpSeconds = ($last['php_us'] - $first['php_us']) / 1_000_000;
    return [
        'php_cpu_s' => round($phpSeconds, 3),
        'worker_cpu_s' => $workerSeconds,
        'php_cpu_us_per_msg' => round($phpSeconds * 1_000_000 / $messages, 3),
        'worker_cpu_us_per_msg' => $workerSeconds === null ? null : round($workerSeconds * 1_000_000 / $messages, 3),
        'php_cpu_cores' => round($phpSeconds / $durationSeconds, 3),
        'worker_cpu_cores' => $workerSeconds === null ? null : round($workerSeconds / $durationSeconds, 3),
    ];
}

function createClient(string $runtime, string $socket, int $concurrency): Hi\Kafka\ClientInterface
{
    return match ($runtime) {
        'native' => new Hi\Kafka\Client($socket),
        'swoole' => new Hi\Kafka\SwooleClient($socket, max(16, $concurrency)),
        'swow' => new Hi\Kafka\SwowClient($socket, max(16, $concurrency)),
        default => throw new InvalidArgumentException("unsupported runtime {$runtime}"),
    };
}

function makeChannel(string $runtime, int $capacity): object
{
    return match ($runtime) {
        'swoole' => new Swoole\Coroutine\Channel($capacity),
        'swow' => new Swow\Channel($capacity),
        default => throw new LogicException('native runtime does not use channels'),
    };
}

function spawn(string $runtime, callable $callback): void
{
    match ($runtime) {
        'swoole' => Swoole\Coroutine::create($callback),
        'swow' => Swow\Coroutine::run($callback),
        default => throw new LogicException('native runtime does not spawn coroutines'),
    };
}

/**
 * Factory performs setup/warm-up and returns the measured callable.
 *
 * @param callable(int):callable():array<string,mixed> $factory
 * @return list<array<string,mixed>>
 */
function runWorkers(string $runtime, int $concurrency, callable $factory): array
{
    if ($runtime === 'native') {
        if ($concurrency !== 1) {
            throw new InvalidArgumentException('native runtime benchmark supports concurrency=1; use process-level orchestration for multi-process load');
        }
        return [($factory(0))()];
    }

    $ready = makeChannel($runtime, $concurrency);
    $start = makeChannel($runtime, $concurrency);
    $results = makeChannel($runtime, $concurrency);

    for ($worker = 0; $worker < $concurrency; $worker++) {
        spawn($runtime, static function () use ($factory, $worker, $ready, $start, $results): void {
            // Release every coroutine before warm-up. A second barrier after
            // warm-up would leave early Consumer streams at zero credit long
            // enough to trigger their intentional Kafka pause debounce, then
            // incorrectly count the subsequent resume/fetch latency as steady
            // state throughput.
            $ready->push(['ok' => true]);
            $start->pop();
            try {
                $measure = $factory($worker);
                $results->push(['ok' => true, 'result' => $measure()]);
            } catch (Throwable $error) {
                $results->push([
                    'ok' => false,
                    'error' => $error::class . ': ' . $error->getMessage(),
                ]);
            }
        });
    }

    for ($i = 0; $i < $concurrency; $i++) {
        $ready->pop();
    }
    for ($i = 0; $i < $concurrency; $i++) {
        $start->push(true);
    }

    $collected = [];
    for ($i = 0; $i < $concurrency; $i++) {
        $result = $results->pop();
        if (! ($result['ok'] ?? false)) {
            throw new RuntimeException((string) ($result['error'] ?? 'benchmark worker failed'));
        }
        $collected[] = $result['result'];
    }
    return $collected;
}

/** @return array<string,mixed> */
function aggregate(array $workers, int $messages, int $messageBytes): array
{
    $startNs = min(array_column($workers, 'start_ns'));
    $endNs = max(array_column($workers, 'end_ns'));
    $durationSeconds = max(1, $endNs - $startNs) / 1_000_000_000;
    $latencies = [];
    $commitLatencies = [];
    $workerDurations = [];
    foreach ($workers as $worker) {
        array_push($latencies, ...$worker['latencies_us']);
        $workerDurations[] = ($worker['end_ns'] - $worker['start_ns']) / 1_000_000_000;
        if (isset($worker['commit_us'])) {
            $commitLatencies[] = $worker['commit_us'];
        }
    }
    $workerStats = array_map(static fn (array $worker): array => [
        'worker' => $worker['worker'],
        'messages' => $worker['messages'],
        'duration_s' => round(($worker['end_ns'] - $worker['start_ns']) / 1_000_000_000, 6),
        'latency' => latencySummary($worker['latencies_us']),
    ], $workers);
    usort($workerStats, static fn (array $left, array $right): int => $left['worker'] <=> $right['worker']);

    return [
        'duration_s' => round($durationSeconds, 6),
        'messages_per_s' => round($messages / $durationSeconds, 2),
        'payload_mib_per_s' => round(($messages * $messageBytes) / 1_048_576 / $durationSeconds, 2),
        'operation_latency' => latencySummary($latencies),
        'commit_latency' => latencySummary($commitLatencies),
        'worker_duration' => [
            'min_s' => round(min($workerDurations), 6),
            'max_s' => round(max($workerDurations), 6),
        ],
        'workers' => $workerStats,
        'cpu' => cpuSummary($workers, $messages, $durationSeconds),
    ];
}

function consumeMessage(Hi\Kafka\ConsumerStreamInterface $stream, int $timeoutMs = 5000): Hi\Kafka\ConsumerRecord
{
    $deadline = microtime(true) + 30;
    do {
        $message = $stream->next($timeoutMs);
        if ($message !== null) {
            return $message;
        }
    } while (microtime(true) < $deadline);
    throw new RuntimeException('timed out waiting for benchmark message');
}

/** @return array<string,mixed> */
function benchmark(array $options): array
{
    $runtime = $options['runtime'];
    $scenario = $options['scenario'];
    $messages = $options['messages'];
    $warmup = $options['warmup'];
    $messageBytes = $options['message_bytes'];
    $concurrency = $options['concurrency'];
    $socket = $options['socket'];
    $topicPrefix = $options['topic'];
    $cluster = 'benchmark';
    // 不能用 str_repeat：重复字节被 lz4 压缩到几乎为零，broker 与网络没有真实负载，
    // 消费侧还会出现数百倍的解压放大。hex 文本的压缩比接近真实业务数据，且是合法
    // UTF-8，旧版本扩展也能运行，便于与基线对比。
    // 每条消息也不能相同：同一批次内的重复 payload 会被 lz4 压缩约 35 倍，单次 1 MiB
    // 的 fetch 解压后就超出预取上限，worker 内存与吞吐都会失真。轮换的 payload 总长
    // 超过 lz4 的 64 KiB 匹配窗口。
    $payloads = [];
    $payloadCount = max(8, intdiv(256 * 1024, $messageBytes) + 1);
    for ($i = 0; $i < $payloadCount; $i++) {
        $payloads[] = substr(bin2hex(random_bytes(intdiv($messageBytes + 1, 2))), 0, $messageBytes);
    }
    $messageShards = shards($messages, $concurrency);
    $warmupShards = shards($warmup, $concurrency);
    $client = createClient($runtime, $socket, $concurrency);
    $client->registerCluster($cluster, $options['cluster_config'], 10_000);

    if ($scenario === 'produce-fnf' || $scenario === 'produce-sync') {
        $factory = static function (int $worker) use (
            $client,
            $scenario,
            $cluster,
            $topicPrefix,
            $payloads,
            $messageShards,
            $warmupShards,
            $socket,
        ): callable {
            $operation = static function (int $index) use ($client, $scenario, $cluster, $topicPrefix, $payloads, $worker): void {
                $payload = $payloads[(($index % \count($payloads)) + \count($payloads)) % \count($payloads)];
                $key = "{$worker}-{$index}";
                if ($scenario === 'produce-fnf') {
                    $client->produceFnf($cluster, $topicPrefix, $key, $payload);
                    return;
                }
                $result = $client->produceSync($cluster, $topicPrefix, $key, $payload, null, null, null, 10_000);
                if (! ($result['ok'] ?? false)) {
                    throw new RuntimeException('produceSync failed: ' . json_encode($result));
                }
            };

            for ($i = 0; $i < $warmupShards[$worker]; $i++) {
                $operation(-$i - 1);
            }

            return static function () use ($operation, $messageShards, $worker, $socket): array {
                $count = $messageShards[$worker];
                $stride = sampleStride($count);
                $latencies = [];
                $cpuStart = cpuSnapshot($socket);
                $start = hrtime(true);
                for ($i = 0; $i < $count; $i++) {
                    if ($i % $stride === 0) {
                        $operationStart = hrtime(true);
                        $operation($i);
                        $latencies[] = (hrtime(true) - $operationStart) / 1000;
                    } else {
                        $operation($i);
                    }
                }
                $end = hrtime(true);
                return [
                    'worker' => $worker,
                    'messages' => $count,
                    'start_ns' => $start,
                    'end_ns' => $end,
                    'latencies_us' => $latencies,
                    'cpu_start' => $cpuStart,
                    'cpu_end' => cpuSnapshot($socket),
                ];
            };
        };
        $workers = runWorkers($runtime, $concurrency, $factory);
        if ($scenario === 'produce-fnf') {
            $barrier = $client->produceSync($cluster, $topicPrefix, 'barrier', 'barrier', null, null, null, 30_000);
            if (! ($barrier['ok'] ?? false)) {
                throw new RuntimeException('producer barrier failed');
            }
        }
    } else {
        $streams = [];
        for ($worker = 0; $worker < $concurrency; $worker++) {
            $topic = "{$topicPrefix}-{$worker}";
            $total = $warmupShards[$worker] + $messageShards[$worker];
            for ($i = 0; $i < $total; $i++) {
                $client->produceFnf($cluster, $topic, "{$worker}-{$i}", $payloads[$i % $payloadCount]);
            }
            $barrier = $client->produceSync($cluster, $topic, "{$worker}-barrier", 'barrier', null, null, null, 30_000);
            if (! ($barrier['ok'] ?? false)) {
                throw new RuntimeException('consumer preload barrier failed');
            }
            $streams[$worker] = $client->consume($cluster, "{$topicPrefix}-group-{$worker}", [$topic], [
                'auto.offset.reset' => 'earliest',
                'session.timeout.ms' => '6000',
                'heartbeat.interval.ms' => '2000',
            ]);
        }

        $factory = static function (int $worker) use ($streams, $warmupShards, $messageShards, $messageBytes, $socket): callable {
            $stream = $streams[$worker];
            for ($i = 0; $i < $warmupShards[$worker]; $i++) {
                $message = consumeMessage($stream);
                $stream->ack($message);
            }

            return static function () use ($stream, $messageShards, $worker, $messageBytes, $socket): array {
                $count = $messageShards[$worker];
                $stride = sampleStride($count);
                $latencies = [];
                $cpuStart = cpuSnapshot($socket);
                $start = hrtime(true);
                for ($i = 0; $i < $count; $i++) {
                    $operationStart = $i % $stride === 0 ? hrtime(true) : null;
                    $message = consumeMessage($stream);
                    if (strlen($message->value()) !== $messageBytes) {
                        throw new RuntimeException('consumer payload size mismatch');
                    }
                    $stream->ack($message);
                    if ($operationStart !== null) {
                        $latencies[] = (hrtime(true) - $operationStart) / 1000;
                    }
                }
                $end = hrtime(true);
                $cpuEnd = cpuSnapshot($socket);
                $commitStart = hrtime(true);
                $stream->commit(30_000);
                $commitUs = (hrtime(true) - $commitStart) / 1000;
                $stream->close();
                return [
                    'worker' => $worker,
                    'messages' => $count,
                    'start_ns' => $start,
                    'end_ns' => $end,
                    'latencies_us' => $latencies,
                    'commit_us' => $commitUs,
                    'cpu_start' => $cpuStart,
                    'cpu_end' => $cpuEnd,
                ];
            };
        };
        $workers = runWorkers($runtime, $concurrency, $factory);
    }

    if (method_exists($client, 'close')) {
        $client->close();
    }

    return [
        'schema' => 1,
        'timestamp_utc' => gmdate(DATE_ATOM),
        'runtime' => $runtime,
        'scenario' => $scenario,
        'messages' => $messages,
        'warmup_messages' => $warmup,
        'message_bytes' => $messageBytes,
        'concurrency' => $concurrency,
        'php_version' => PHP_VERSION,
        'hi_kafka_version' => phpversion('hi-kafka') ?: 'unknown',
        'runtime_extension_version' => $runtime === 'native' ? null : (phpversion($runtime) ?: 'unknown'),
        'os' => php_uname(),
        'cluster_config' => $options['cluster_config'],
        'result' => aggregate($workers, $messages, $messageBytes),
        'php_peak_allocated_bytes' => memory_get_peak_usage(true),
    ];
}

$cli = getopt('', [
    'runtime:',
    'scenario:',
    'messages:',
    'warmup:',
    'message-bytes:',
    'concurrency:',
    'socket:',
    'topic:',
    'brokers:',
]);
$runtime = (string) ($cli['runtime'] ?? getenv('HI_KAFKA_BENCH_RUNTIME') ?: 'native');
$scenario = (string) ($cli['scenario'] ?? getenv('HI_KAFKA_BENCH_SCENARIO') ?: 'consume');
$messages = (int) ($cli['messages'] ?? getenv('HI_KAFKA_BENCH_MESSAGES') ?: 20_000);
$warmup = (int) ($cli['warmup'] ?? getenv('HI_KAFKA_BENCH_WARMUP') ?: 2_000);
$messageBytes = (int) ($cli['message-bytes'] ?? getenv('HI_KAFKA_BENCH_MESSAGE_BYTES') ?: 1024);
$concurrency = (int) ($cli['concurrency'] ?? getenv('HI_KAFKA_BENCH_CONCURRENCY') ?: 1);
$socket = (string) ($cli['socket'] ?? getenv('HI_KAFKA_BENCH_SOCKET') ?: '/tmp/hi-kafka-benchmark.sock');
$topic = (string) ($cli['topic'] ?? getenv('HI_KAFKA_BENCH_TOPIC') ?: 'hi-kafka-benchmark-' . getmypid());
$brokers = (string) ($cli['brokers'] ?? getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094');

if (! in_array($runtime, ['native', 'swoole', 'swow'], true)) {
    fail('runtime must be native, swoole or swow');
}
if (! in_array($scenario, ['produce-fnf', 'produce-sync', 'consume'], true)) {
    fail('scenario must be produce-fnf, produce-sync or consume');
}
if ($messages <= 0 || $warmup < 0 || $messageBytes <= 0 || $concurrency <= 0) {
    fail('messages/message-bytes/concurrency must be positive and warmup must not be negative');
}
if ($runtime === 'native' && $concurrency !== 1) {
    fail('native runtime only supports concurrency=1 in this harness');
}
if ($runtime !== 'native' && ! extension_loaded($runtime)) {
    fail("{$runtime} extension is not loaded");
}

$options = [
    'runtime' => $runtime,
    'scenario' => $scenario,
    'messages' => $messages,
    'warmup' => $warmup,
    'message_bytes' => $messageBytes,
    'concurrency' => $concurrency,
    'socket' => $socket,
    'topic' => $topic,
    'cluster_config' => parseClusterConfig($brokers),
];

$run = static fn (): array => benchmark($options);
if ($runtime === 'swoole') {
    $result = null;
    Swoole\Coroutine\run(static function () use ($run, &$result): void {
        $result = $run();
    });
} elseif ($runtime === 'swow') {
    $result = null;
    Swow\Coroutine::run(static function () use ($run, &$result): void {
        $result = $run();
    });
    Swow\Sync\waitAll();
} else {
    $result = $run();
}

echo json_encode($result, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES), "\n";
