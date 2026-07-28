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
    $payload = str_repeat('x', $messageBytes);
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
            $payload,
            $messageShards,
            $warmupShards,
        ): callable {
            $operation = static function (int $index) use ($client, $scenario, $cluster, $topicPrefix, $payload, $worker): void {
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

            return static function () use ($operation, $messageShards, $worker): array {
                $count = $messageShards[$worker];
                $stride = sampleStride($count);
                $latencies = [];
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
                return [
                    'worker' => $worker,
                    'messages' => $count,
                    'start_ns' => $start,
                    'end_ns' => hrtime(true),
                    'latencies_us' => $latencies,
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
                $client->produceFnf($cluster, $topic, "{$worker}-{$i}", $payload);
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

        $factory = static function (int $worker) use ($streams, $warmupShards, $messageShards, $messageBytes): callable {
            $stream = $streams[$worker];
            for ($i = 0; $i < $warmupShards[$worker]; $i++) {
                $message = consumeMessage($stream);
                $stream->ack($message);
            }

            return static function () use ($stream, $messageShards, $worker, $messageBytes): array {
                $count = $messageShards[$worker];
                $stride = sampleStride($count);
                $latencies = [];
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
