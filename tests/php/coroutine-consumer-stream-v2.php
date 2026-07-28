<?php

declare(strict_types=1);

if ($argc < 3) {
    fwrite(STDERR, "usage: coroutine-consumer-stream-v2.php SOCKET TOPIC\n");
    exit(2);
}
if (! extension_loaded('swoole')) {
    echo "SKIP: swoole extension is not loaded\n";
    exit(0);
}

require __DIR__ . '/../../php-driver/vendor/autoload.php';

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';

Swoole\Coroutine\run(static function () use ($socket, $topic, $brokers): void {
    $client = new Hi\Kafka\SwooleClient($socket);
    $client->registerCluster('default', ['bootstrap.servers' => $brokers]);

    $expected = [];
    for ($i = 0; $i < 8; $i++) {
        $key = "coroutine-{$i}";
        $value = str_repeat(chr(65 + $i), 8192);
        $expected[$key] = hash('sha256', $value);
        $result = $client->produceSync('default', $topic, $key, $value, timeoutMs: 10_000);
        if (! ($result['ok'] ?? false)) {
            throw new RuntimeException('produce failed: ' . json_encode($result));
        }
    }

    $stream = $client->consume('default', 'coroutine-v2-' . getmypid(), [$topic], [
        'auto.offset.reset' => 'earliest',
        'session.timeout.ms' => '6000',
        'heartbeat.interval.ms' => '2000',
    ]);
    if (! $stream instanceof Hi\Kafka\ConsumerStreamInterface) {
        throw new RuntimeException('consume() did not return ConsumerStreamInterface');
    }

    $seen = [];
    $deadline = microtime(true) + 30;
    while (count($seen) < count($expected) && microtime(true) < $deadline) {
        $message = $stream->next(5_000);
        if ($message === null) {
            continue;
        }
        $key = $message->key();
        if (isset($expected[$key])) {
            if (hash('sha256', $message->value()) !== $expected[$key]) {
                throw new RuntimeException("payload mismatch for {$key}");
            }
            $seen[$key] = true;
        }
        $stream->ack($message);
    }

    if (count($seen) !== count($expected)) {
        throw new RuntimeException('received ' . count($seen) . '/' . count($expected));
    }
    $stream->commit(10_000);

    $afterCommitKey = 'coroutine-after-commit-' . getmypid();
    $result = $client->produceSync('default', $topic, $afterCommitKey, 'after-commit', timeoutMs: 10_000);
    if (! ($result['ok'] ?? false)) {
        throw new RuntimeException('post-commit produce failed: ' . json_encode($result));
    }
    $deadline = microtime(true) + 15;
    do {
        $message = $stream->next(5_000);
        if ($message !== null && $message->key() !== $afterCommitKey) {
            $stream->ack($message);
            $message = null;
        }
    } while (($message === null || $message->key() !== $afterCommitKey) && microtime(true) < $deadline);
    if ($message === null || $message->key() !== $afterCommitKey) {
        throw new RuntimeException('coroutine stream did not replenish credit after commit');
    }
    $stream->ack($message);
    $stream->commit(10_000);
    $stream->close();
    $client->close();
});

echo "★ CoroutineConsumerStream v2 PASS\n";
