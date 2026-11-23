<?php

declare(strict_types=1);

// 回归：produceFnf / produceSync 的 key、value、header 值必须二进制安全。
//
// 旧实现所有 produce 入口（含协程 driver 使用的 hi_kafka_encode_*_frame）把 key/value
// 声明为 &str，非 UTF-8 字节（protobuf / msgpack / 压缩数据）直接被拒绝；header 值同理。

if ($argc < 3) {
    fwrite(STDERR, "usage: binary-roundtrip-v2.php SOCKET TOPIC\n");
    exit(2);
}

require __DIR__ . '/../../php-driver/vendor/autoload.php';

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$runtime = match (true) {
    extension_loaded('swoole') => 'swoole',
    extension_loaded('swow') => 'swow',
    default => 'blocking',
};

$scenario = static function () use ($runtime, $socket, $topic, $brokers): void {
    $client = match ($runtime) {
        'swoole' => new Hi\Kafka\SwooleClient($socket),
        'swow' => new Hi\Kafka\SwowClient($socket),
        default => new Hi\Kafka\Client($socket),
    };
    $client->registerCluster('default', ['bootstrap.servers' => $brokers]);

    $expected = [];
    foreach (['fnf', 'sync'] as $mode) {
        for ($i = 0; $i < 5; $i++) {
            // 必含 NUL 与非法 UTF-8 序列。
            $key = "\xff\x00{$mode}{$i}" . random_bytes(8);
            $value = "\xc3\x28\x00" . random_bytes(1024);
            $header = "\x80\x00" . random_bytes(16);
            $expected[$key] = [$value, $header];
            if ('fnf' === $mode) {
                $client->produceFnf('default', $topic, $key, $value, ['bin' => $header]);
            } else {
                $result = $client->produceSync('default', $topic, $key, $value, ['bin' => $header], timeoutMs: 10_000);
                if (! ($result['ok'] ?? false)) {
                    throw new RuntimeException('produceSync failed: ' . json_encode($result));
                }
            }
        }
    }

    $stream = $client->consume('default', 'binary-' . getmypid(), [$topic], ['auto.offset.reset' => 'earliest']);
    $deadline = microtime(true) + 30;
    while ($expected !== [] && microtime(true) < $deadline) {
        $message = $stream->next(1_000);
        if (null === $message) {
            continue;
        }
        $key = $message->key();
        if (! isset($expected[$key])) {
            throw new RuntimeException('unexpected or corrupted key ' . bin2hex((string) $key));
        }
        [$value, $header] = $expected[$key];
        if ($message->value() !== $value) {
            throw new RuntimeException('value corrupted for key ' . bin2hex($key));
        }
        $headers = $message->headers();
        if (($headers[0]['name'] ?? null) !== 'bin' || ($headers[0]['value'] ?? null) !== $header) {
            throw new RuntimeException('header corrupted for key ' . bin2hex($key));
        }
        unset($expected[$key]);
        $stream->ack($message);
    }
    $stream->close();
    if ($expected !== []) {
        throw new RuntimeException(count($expected) . ' binary messages never arrived');
    }
};

$failure = null;
$guarded = static function () use ($scenario, &$failure): void {
    try {
        $scenario();
    } catch (Throwable $error) {
        $failure = $error;
    }
};
if ('swoole' === $runtime) {
    Swoole\Coroutine\run($guarded);
} elseif ('swow' === $runtime) {
    Swow\Coroutine::run($guarded);
    Swow\Sync\waitAll();
} else {
    $guarded();
}
if (null !== $failure) {
    throw $failure;
}

echo "★ Binary roundtrip v2 PASS ({$runtime})\n";
