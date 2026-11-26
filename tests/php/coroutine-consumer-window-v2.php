<?php

declare(strict_types=1);

// 回归：ConcurrentConsumerClientInterface 打开的多消息窗口。
// 1. 窗口满之前可连续取出多条未确认消息，窗口满时 next() 被本地拒绝；
// 2. 乱序 ACK 后 commit 只推进到连续 ACK 的位置，不越过未 ACK 的消息；
// 3. 全部 ACK 并 commit 后，重新打开不再重投；空闲时 next() 按超时返回 null。
//
// Swoole 与 Swow 各跑一遍（两者不能同时加载）。

if ($argc < 3) {
    fwrite(STDERR, "usage: coroutine-consumer-window-v2.php SOCKET TOPIC\n");
    exit(2);
}
$runtime = match (true) {
    extension_loaded('swoole') => 'swoole',
    extension_loaded('swow') => 'swow',
    default => null,
};
if (null === $runtime) {
    echo "SKIP: neither swoole nor swow extension is loaded\n";
    exit(0);
}

require __DIR__ . '/../../php-driver/vendor/autoload.php';

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
const WINDOW = 4;

$scenario = static function () use ($runtime, $socket, $topic, $brokers): void {
    $client = 'swoole' === $runtime ? new Hi\Kafka\SwooleClient($socket) : new Hi\Kafka\SwowClient($socket);
    $client->registerCluster('default', ['bootstrap.servers' => $brokers]);
    if (! $client instanceof Hi\Kafka\ConcurrentConsumerClientInterface) {
        throw new RuntimeException($client::class . ' must implement ConcurrentConsumerClientInterface');
    }

    for ($i = 0; $i < 6; $i++) {
        $result = $client->produceSync('default', $topic, "window-{$i}", "value-{$i}", timeoutMs: 10_000);
        if (! ($result['ok'] ?? false)) {
            throw new RuntimeException('produce failed: ' . json_encode($result));
        }
    }

    $group = 'window-v2-' . getmypid();
    $open = static fn () => $client->consumeConcurrent('default', $group, [$topic], [
        'auto.offset.reset' => 'earliest',
        'session.timeout.ms' => '6000',
        'heartbeat.interval.ms' => '2000',
    ], WINDOW);
    $next = static function (Hi\Kafka\ConsumerStreamInterface $stream): Hi\Kafka\ConsumerRecord {
        $deadline = microtime(true) + 20;
        do {
            $message = $stream->next(2_000);
            if ($message !== null) {
                return $message;
            }
        } while (microtime(true) < $deadline);
        throw new RuntimeException('message deadline exceeded');
    };

    $stream = $open();
    $held = [];
    for ($i = 0; $i < WINDOW; $i++) {
        $held[] = $next($stream);
    }
    $keys = array_map(static fn ($m) => $m->key(), $held);
    if ($keys !== ['window-0', 'window-1', 'window-2', 'window-3']) {
        throw new RuntimeException('unexpected window contents: ' . implode(',', $keys));
    }
    try {
        $stream->next(100);
        throw new RuntimeException('next accepted a read beyond the full window');
    } catch (LogicException $error) {
        if (! str_contains($error->getMessage(), 'ack or nack')) {
            throw $error;
        }
    }

    // 乱序 ACK 后三条，window-0 仍未确认：commit 不能越过它。
    foreach ([3, 1, 2] as $i) {
        $stream->ack($held[$i]);
    }
    $stream->commit(10_000);
    $stream->close();

    $stream = $open();
    $first = $next($stream);
    if ($first->key() !== 'window-0') {
        throw new RuntimeException('commit passed an unacked message: got ' . $first->key());
    }

    // 重投从 window-0 开始；全部 ACK 并 commit。
    $stream->ack($first);
    $remaining = ['window-1', 'window-2', 'window-3', 'window-4', 'window-5'];
    foreach ($remaining as $expected) {
        $message = $next($stream);
        if ($message->key() !== $expected) {
            throw new RuntimeException("expected {$expected}, got " . $message->key());
        }
        $stream->ack($message);
    }
    $stream->commit(10_000);
    $stream->close();

    $stream = $open();
    $leftover = $stream->next(3_000);
    if ($leftover !== null) {
        throw new RuntimeException('message redelivered after full commit: ' . $leftover->key());
    }
    // 空闲读超时必须按毫秒生效：此前 Swow transport 把毫秒当微秒传入，next() 实际阻塞约 16 分钟。
    $started = microtime(true);
    if (null !== $stream->next(1_000)) {
        throw new RuntimeException('unexpected message on an idle stream');
    }
    if (microtime(true) - $started > 3) {
        throw new RuntimeException(sprintf('idle next(1000) took %.1fs', microtime(true) - $started));
    }
    $stream->close();
    $client->close();
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
} else {
    Swow\Coroutine::run($guarded);
    Swow\Sync\waitAll();
}
if (null !== $failure) {
    throw $failure;
}

echo "★ Coroutine consumer window v2 PASS ({$runtime})\n";
