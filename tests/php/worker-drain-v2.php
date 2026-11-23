<?php

declare(strict_types=1);

// 回归：SIGTERM 后 worker 必须及时关闭空闲的 RPC 连接完成 drain。
//
// PHP 连接池总会保留空闲连接；旧实现的连接任务只阻塞在读下一帧上，不感知 drain，
// 每次停机都要等满 drain_timeout（默认 10s）后强制 abort。

if ($argc < 3) {
    fwrite(STDERR, "usage: worker-drain-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket, $topic] = $argv;
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094']);
$client->produceSync('default', $topic, 'k', 'v', timeoutMs: 10_000);

$pid = (int) trim((string) file_get_contents("{$socket}.pid"));
$started = microtime(true);
posix_kill($pid, SIGTERM);
while (posix_kill($pid, 0) && microtime(true) - $started < 15) {
    usleep(20_000);
}
$elapsed = microtime(true) - $started;

if (posix_kill($pid, 0)) {
    throw new RuntimeException('worker did not exit within 15s');
}
if ($elapsed > 3) {
    throw new RuntimeException(sprintf('drain took %.1fs with only idle connections', $elapsed));
}
if (file_exists($socket)) {
    throw new RuntimeException('worker left its socket file behind');
}

printf("★ Worker drain v2 PASS (%.0fms)\n", $elapsed * 1000);
