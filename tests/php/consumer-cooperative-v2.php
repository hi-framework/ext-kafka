<?php

declare(strict_types=1);

// 回归：partition.assignment.strategy=cooperative-sticky 下，第二个成员加入/离开
// 触发增量 rebalance 后，所有分区必须继续被消费、已提交的消息不重复丢失。
//
// 旧实现：增量 Assign 覆盖完整 assignment，backpressure 结束后只 resume 增量分区；
// 且 rebalance notice 要等下一条消息到达才被 actor 处理，空闲分区的 ownership
// 变化不会及时生效。
//
// 需要预先创建的多分区 topic（e2e 脚本无法建 topic）：
//   kafka-topics.sh --create --topic coop-e2e --partitions 4 ...
//   HI_KAFKA_E2E_COOPERATIVE_TOPIC=coop-e2e ./scripts/run-all-e2e.sh

if ($argc < 3) {
    fwrite(STDERR, "usage: consumer-cooperative-v2.php SOCKET TOPIC\n");
    exit(2);
}
$topic = getenv('HI_KAFKA_E2E_COOPERATIVE_TOPIC') ?: null;
if (null === $topic) {
    echo "SKIP: set HI_KAFKA_E2E_COOPERATIVE_TOPIC to a pre-created multi-partition topic\n";
    exit(0);
}

[$script, $socket] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$run = getmypid() . '-' . hrtime(true);
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

$produce = static function (string $phase, int $count) use ($client, $topic, $run): array {
    $keys = [];
    for ($i = 0; $i < $count; $i++) {
        // 不同 key 让消息分散到各个分区。
        $key = "{$run}-{$phase}-{$i}";
        $result = $client->produceSync('default', $topic, $key, $phase, [], null, null, 10_000);
        if (! ($result['ok'] ?? false)) {
            throw new RuntimeException("produce {$key} failed");
        }
        $keys[$key] = true;
    }
    return $keys;
};

$config = [
    // 分区在成员间转移时，新 owner 从已提交 offset 继续；没有提交过的分区从头读，
    // 本轮之外的旧消息按 key 前缀忽略。
    'auto.offset.reset' => 'earliest',
    'partition.assignment.strategy' => 'cooperative-sticky',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '1000',
];
$group = "cooperative-{$run}";
$seen = [];
// rebalance 进行中的 commit 返回可重试错误；watermark 保留在 worker，下次 commit 补上。
$commit = static function ($stream): void {
    try {
        $stream->commit(10_000);
    } catch (Hi\Kafka\KafkaException $error) {
        if (! $error->retryable) {
            throw $error;
        }
    }
};
$drain = static function (array $streams, array $expected, float $seconds) use (&$seen, $commit): void {
    $deadline = microtime(true) + $seconds;
    while (array_diff_key($expected, $seen) !== [] && microtime(true) < $deadline) {
        foreach ($streams as $stream) {
            $message = $stream->next(200);
            if ($message !== null) {
                $seen[$message->key()] = true;
                $stream->ack($message);
                $commit($stream);
            }
        }
    }
};

$first = $client->consume('default', $group, [$topic], $config);
// 持续发 probe，直到消费到其中任意一条，说明首次 assignment 已完成。
$deadline = microtime(true) + 20;
$assigned = false;
for ($i = 0; ! $assigned && microtime(true) < $deadline; $i++) {
    $produce("probe{$i}", 4);
    $message = $first->next(500);
    while ($message !== null) {
        $assigned = $assigned || str_starts_with($message->key(), "{$run}-probe");
        $first->ack($message);
        $commit($first);
        $message = $first->next(50);
    }
}
if (! $assigned) {
    throw new RuntimeException('first member was never assigned');
}

// 第二个成员加入：增量 rebalance，第一个成员保留部分分区。
$second = $client->consume('default', $group, [$topic], $config);
$during = $produce('join', 32);
$drain([$first, $second], $during, 20);

// 第二个成员离开：其分区增量回到第一个成员。
$second->close();
unset($second);
$after = $produce('leave', 32);
$drain([$first], $after, 30);
$first->close();

$missing = array_diff_key($during + $after, $seen);
if ($missing !== []) {
    throw new RuntimeException(sprintf(
        'cooperative rebalance stalled: %d/%d messages never delivered (join phase %d, leave phase %d)',
        count($missing),
        count($during) + count($after),
        count(array_diff_key($during, $seen)),
        count(array_diff_key($after, $seen)),
    ));
}

echo "★ Consumer cooperative rebalance v2 PASS\n";
