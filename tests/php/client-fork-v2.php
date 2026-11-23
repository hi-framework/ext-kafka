<?php

declare(strict_types=1);

// 回归：pcntl_fork 后父子进程各自使用阻塞 Client，不得共用继承的 UDS 连接。
//
// 旧实现的连接池没有进程归属：子进程复用父进程的空闲 fd，两个进程读写同一条字节流，
// 响应会被对方读走（cid mismatch / 超时）。

if ($argc < 3) {
    fwrite(STDERR, "usage: client-fork-v2.php SOCKET TOPIC\n");
    exit(2);
}
if (! function_exists('pcntl_fork')) {
    echo "SKIP: pcntl is not available\n";
    exit(0);
}

[$script, $socket, $topic] = $argv;
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094']);
// 让连接池里留下一条空闲连接，fork 后父子进程都会继承它。
$client->produceSync('default', $topic, 'warmup', 'warmup', timeoutMs: 10_000);

$hammer = static function (string $who) use ($client, $topic): void {
    for ($i = 0; $i < 200; $i++) {
        $result = $client->produceSync('default', $topic, "{$who}-{$i}", $who, timeoutMs: 10_000);
        if (! ($result['ok'] ?? false)) {
            throw new RuntimeException("{$who} produce {$i} failed: " . json_encode($result));
        }
    }
};

$pid = pcntl_fork();
if (0 === $pid) {
    try {
        $hammer('child');
        exit(0);
    } catch (Throwable $error) {
        fwrite(STDERR, 'child: ' . $error->getMessage() . "\n");
        exit(1);
    }
}

$hammer('parent');
pcntl_waitpid($pid, $status);
if (0 !== pcntl_wexitstatus($status)) {
    throw new RuntimeException('child process failed while sharing the worker');
}

echo "★ Client fork ownership v2 PASS\n";
