<?php

declare(strict_types=1);

if ($argc < 3) {
    fwrite(STDERR, "usage: consumer-record-memory-swoole-v2.php SOCKET TOPIC\n");
    exit(2);
}
if (! extension_loaded('swoole')) {
    echo "SKIP: swoole extension is not loaded\n";
    exit(0);
}

require __DIR__ . '/../../php-driver/vendor/autoload.php';
require __DIR__ . '/_consumer-record-memory.php';

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$messageCount = max(10_000, (int) (getenv('HI_KAFKA_RECORD_MEMORY_MESSAGES') ?: 50_000));
$maxGrowth = max(2 * 1024 * 1024, (int) (getenv('HI_KAFKA_RECORD_MEMORY_MAX_GROWTH') ?: 4 * 1024 * 1024));
$payload = 'consumer-record-memory';

Swoole\Coroutine\run(static function () use (
    $socket,
    $topic,
    $brokers,
    $messageCount,
    $maxGrowth,
    $payload,
): void {
    $client = new Hi\Kafka\SwooleClient($socket);
    $client->registerCluster('default', ['bootstrap.servers' => $brokers]);
    hiKafkaProduceMemoryFixture($client, $topic, $messageCount, $payload);
    $growth = hiKafkaAssertConsumerRecordMemoryStable(
        $client,
        $topic,
        'record-memory-swoole-' . getmypid(),
        $messageCount,
        $payload,
        $maxGrowth,
    );
    $client->close();

    echo "★ Swoole ConsumerRecord memory PASS ({$messageCount} messages, {$growth} PHP bytes after warm-up)\n";
});
