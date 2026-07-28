<?php

declare(strict_types=1);

/**
 * 快速写入内存回归测试所需的小消息，且不在 PHP 数组中保留逐条期望值。
 */
function hiKafkaProduceMemoryFixture(
    Hi\Kafka\ClientInterface $client,
    string $topic,
    int $messageCount,
    string $payload,
): void {
    for ($i = 0; $i < $messageCount - 1; $i++) {
        $client->produceFnf('default', $topic, "record-{$i}", $payload, [], null, null);
    }

    // 最后一条同步生产确保同一 producer 在开始消费前完成一次 broker delivery。
    $result = $client->produceSync(
        'default',
        $topic,
        'record-' . ($messageCount - 1),
        $payload,
        [],
        null,
        null,
        30_000,
    );
    if (! ($result['ok'] ?? false)) {
        throw new RuntimeException('final produce failed: ' . json_encode($result));
    }
}

/**
 * 用常量额外空间消费全部消息，并约束预热后的 PHP 内存增长。
 *
 * 该断言同时覆盖 native ConsumerStream 的直接对象返回路径，以及 Swoole/Swow
 * transport 调用 hi_kafka_decode_stream_frame() 的对象解码路径。
 */
function hiKafkaAssertConsumerRecordMemoryStable(
    Hi\Kafka\ClientInterface $client,
    string $topic,
    string $groupId,
    int $messageCount,
    string $payload,
    int $maxGrowth,
): int {
    $warmupCount = min(5_000, intdiv($messageCount, 10));
    $stream = $client->consume('default', $groupId, [$topic], [
        'auto.offset.reset' => 'earliest',
        'session.timeout.ms' => '6000',
        'heartbeat.interval.ms' => '2000',
    ]);

    $received = 0;
    $baseline = null;
    $deadline = microtime(true) + max(60, (int) ceil($messageCount / 1_000));
    while ($received < $messageCount && microtime(true) < $deadline) {
        $message = $stream->next(5_000);
        if ($message === null) {
            continue;
        }
        if ($message->value() !== $payload) {
            throw new RuntimeException('unexpected payload at message ' . $received);
        }
        $stream->ack($message);
        unset($message);
        $received++;

        // 先让连接、类元数据和 Zend allocator 完成预热，再量剩余 90% 消息。
        // 若每个 ConsumerRecord 多留一个引用，后半程会随消息数线性增长。
        if ($received === $warmupCount) {
            gc_collect_cycles();
            $baseline = memory_get_usage(true);
        }
    }

    if ($received !== $messageCount) {
        throw new RuntimeException("received {$received}/{$messageCount}");
    }
    $stream->commit(30_000);
    $stream->close();
    gc_collect_cycles();

    $current = memory_get_usage(true);
    $growth = max(0, $current - ($baseline ?? $current));
    if ($growth > $maxGrowth) {
        throw new RuntimeException(
            "ConsumerRecord memory grew by {$growth} bytes after warm-up; limit is {$maxGrowth}",
        );
    }

    return $growth;
}
