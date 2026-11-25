<?php

declare(strict_types=1);

// 回归：produceFnf / produceSync 可以发送 null key 与 null value（tombstone），
// 且 null 与空串在 broker 上保持区分，消费端原样读回。

if ($argc < 3) {
    fwrite(STDERR, "usage: producer-null-v2.php SOCKET TOPIC\n");
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

    $cases = [[null, 'value'], ['key', null], [null, null], ['', '']];
    $expected = [];
    foreach (['fnf', 'sync'] as $mode) {
        foreach ($cases as $i => [$key, $value]) {
            $id = "{$mode}-{$i}";
            $expected[$id] = [$key, $value];
            if ('fnf' === $mode) {
                $client->produceFnf('default', $topic, $key, $value, ['id' => $id]);
            } else {
                $result = $client->produceSync('default', $topic, $key, $value, ['id' => $id], timeoutMs: 10_000);
                if (! ($result['ok'] ?? false)) {
                    throw new RuntimeException("produceSync {$id} failed: " . json_encode($result));
                }
            }
        }
    }

    $stream = $client->consume('default', 'null-' . getmypid(), [$topic], ['auto.offset.reset' => 'earliest']);
    $deadline = microtime(true) + 30;
    while ($expected !== [] && microtime(true) < $deadline) {
        $message = $stream->next(1_000);
        if (null === $message) {
            continue;
        }
        $id = $message->headers()[0]['value'] ?? null;
        if (! isset($expected[$id])) {
            throw new RuntimeException('unexpected message ' . json_encode($id));
        }
        $got = [$message->key(), $message->value()];
        if ($got !== $expected[$id]) {
            throw new RuntimeException(sprintf('%s: expected %s, got %s', $id, var_export($expected[$id], true), var_export($got, true)));
        }
        unset($expected[$id]);
        $stream->ack($message);
    }
    $stream->close();
    if ($expected !== []) {
        throw new RuntimeException('never arrived: ' . implode(', ', array_keys($expected)));
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

echo "★ Producer null key / tombstone v2 PASS ({$runtime})\n";
