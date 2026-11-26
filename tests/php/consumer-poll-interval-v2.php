<?php

declare(strict_types=1);

// 回归：PHP 持有消息不 ACK、单订阅额度占满时，worker 暂停分区后仍须持续拉取 librdkafka。
// 旧实现此时只等待 PHP 命令，超过 max.poll.interval.ms 后 librdkafka 主动离开 group：
// 下一次 next() 抛 PollExceeded，订阅被重建，已 ACK 但未提交的消息被重复投递。

if ($argc < 3) {
    fwrite(STDERR, "usage: consumer-poll-interval-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$group = 'poll-interval-v2-' . getmypid();
$maxPollIntervalMs = 10_000;

// worker 由第一个连接拉起并继承本进程环境：单订阅只给 1 条额度，持有一条即占满。
putenv('HI_KAFKA_CONSUMER_MAX_MESSAGES=1');

$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

for ($i = 0; $i < 3; $i++) {
    $result = $client->produceSync('default', $topic, "poll-{$i}", "value-{$i}", [], null, null, 10_000);
    if (! ($result['ok'] ?? false)) {
        throw new RuntimeException("produce {$i} failed");
    }
}

$stream = $client->consume('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
    'max.poll.interval.ms' => (string) $maxPollIntervalMs,
]);
$next = static function (Hi\Kafka\ConsumerStream $stream): Hi\Kafka\ConsumerRecord {
    $deadline = microtime(true) + 20;
    do {
        $message = $stream->next(2_000);
        if ($message !== null) {
            return $message;
        }
    } while (microtime(true) < $deadline);
    throw new RuntimeException('message deadline exceeded');
};

$first = $next($stream);
if ($first->key() !== 'poll-0') {
    throw new RuntimeException('expected poll-0, got ' . $first->key());
}

// 持有超过 max.poll.interval.ms。
usleep(($maxPollIntervalMs + 4_000) * 1_000);
$stream->ack($first);

for ($i = 1; $i < 3; $i++) {
    $message = $next($stream);
    if ($message->key() !== "poll-{$i}") {
        throw new RuntimeException("expected poll-{$i} after long hold, got " . $message->key() . ' (left the group and redelivered?)');
    }
    if ($message->subscriptionEpoch() !== $first->subscriptionEpoch()) {
        throw new RuntimeException('subscription epoch changed during the hold');
    }
    $stream->ack($message);
}
$stream->commit(10_000);
$stream->close();

echo "★ Consumer poll interval keepalive v2 PASS\n";
