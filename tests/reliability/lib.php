<?php

declare(strict_types=1);

/**
 * 可靠性测试公共工具。
 *
 * 消息格式：value = "{run}|{producer}|{seq}|{crc32(payload)}|{payload}"，
 * key = "{producer}-{seq}"（使消息分散到所有分区）。
 *
 * 账本文件（全部 append-only、逐行 flush，进程被杀也不丢已写入的行）：
 * - acked-{producer}.log   "{seq} {ts}"          produceSync 成功（broker 已确认）
 * - consumed-{consumer}.log "{producer} {seq} {partition} {offset} {ts}"
 * - events.jsonl            produce/consume 错误、进程启停等事件
 */

const RELIABILITY_SEPARATOR = '|';

function opt(array $options, string $name, ?string $default = null): string
{
    $value = $options[$name] ?? $default;
    if (null === $value || false === $value) {
        fwrite(STDERR, "missing --{$name}\n");
        exit(2);
    }

    return (string) $value;
}

function parse_options(array $argv): array
{
    $options = [];
    foreach (array_slice($argv, 1) as $arg) {
        if (preg_match('/^--([a-z0-9-]+)=(.*)$/s', $arg, $match)) {
            $options[$match[1]] = $match[2];
        }
    }

    return $options;
}

function encode_value(string $run, string $producer, int $seq, string $payload): string
{
    return implode(RELIABILITY_SEPARATOR, [$run, $producer, $seq, crc32($payload), $payload]);
}

/**
 * @return array{run:string,producer:string,seq:int,payload:string}|null 校验失败返回 null
 */
function decode_value(?string $value): ?array
{
    if (null === $value) {
        return null;
    }
    $parts = explode(RELIABILITY_SEPARATOR, $value, 5);
    if (5 !== count($parts) || (int) $parts[3] !== crc32($parts[4])) {
        return null;
    }

    return ['run' => $parts[0], 'producer' => $parts[1], 'seq' => (int) $parts[2], 'payload' => $parts[4]];
}

/**
 * 以追加方式打开账本文件；每次写入后立即 flush。
 *
 * @return resource
 */
function open_ledger(string $path)
{
    $handle = fopen($path, 'ab');
    if (false === $handle) {
        throw new RuntimeException("cannot open {$path}");
    }

    return $handle;
}

function ledger_write($handle, string $line): void
{
    fwrite($handle, $line . "\n");
    fflush($handle);
}

function log_event(string $dir, array $event): void
{
    $event['ts'] ??= microtime(true);
    file_put_contents($dir . '/events.jsonl', json_encode($event, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND | LOCK_EX);
}

function error_event(Throwable $error): array
{
    return [
        'class' => $error::class,
        'message' => substr($error->getMessage(), 0, 500),
        'kind' => $error instanceof Hi\Kafka\KafkaException ? $error->kind_name : null,
        'retryable' => $error instanceof Hi\Kafka\KafkaException ? $error->retryable : null,
    ];
}

/**
 * 生产者与消费者注册的集群配置必须完全一致（与真实业务相同）。不一致会让每次
 * 注册都覆盖配置、触发共享 Producer 换代，给测试引入无关变量。
 *
 * HI_KAFKA_RDKAFKA_DEBUG=broker,metadata,... 打开 librdkafka 内部日志（经 worker 日志输出）。
 *
 * @return array<string,string>
 */
function cluster_config(string $brokers): array
{
    $config = ['bootstrap.servers' => $brokers, 'linger.ms' => '1'];
    if (false !== ($debug = getenv('HI_KAFKA_RDKAFKA_DEBUG')) && '' !== $debug) {
        $config['debug'] = $debug;
    }

    return $config;
}

/**
 * 按运行时创建 Client。swoole / swow 需要已加载对应扩展且在协程内调用。
 */
function create_client(string $runtime, string $socket): Hi\Kafka\ClientInterface
{
    return match ($runtime) {
        'blocking' => new Hi\Kafka\Client($socket),
        'swoole' => new Hi\Kafka\SwooleClient($socket),
        'swow' => new Hi\Kafka\SwowClient($socket),
        default => throw new InvalidArgumentException("unknown runtime {$runtime}"),
    };
}

/**
 * 在指定运行时中执行入口；协程内的异常带出协程后重新抛出。
 */
function run_in_runtime(string $runtime, Closure $main): void
{
    if ('blocking' === $runtime) {
        $main();
        return;
    }
    require_once __DIR__ . '/../../php-driver/vendor/autoload.php';
    $failure = null;
    $guarded = static function () use ($main, &$failure): void {
        try {
            $main();
        } catch (Throwable $error) {
            $failure = $error;
        }
    };
    if ('swoole' === $runtime) {
        Swoole\Coroutine\run($guarded);
    } else {
        Swow\Coroutine::run($guarded);
        Swow\Sync\waitAll();
    }
    if (null !== $failure) {
        throw $failure;
    }
}

/**
 * 协程友好的 sleep：Swoole 未开 hook 时 usleep 会阻塞整个进程；Swow 默认接管 usleep。
 */
function runtime_sleep(string $runtime, float $seconds): void
{
    if ($seconds <= 0) {
        return;
    }
    if ('swoole' === $runtime) {
        Swoole\Coroutine::sleep($seconds);
        return;
    }
    usleep((int) ($seconds * 1_000_000));
}
