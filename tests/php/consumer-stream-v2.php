<?php

declare(strict_types=1);

if ($argc < 3) {
    fwrite(STDERR, "usage: consumer-stream-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$messageCount = max(1, (int) (getenv('HI_KAFKA_E2E_MESSAGES') ?: 32));
$messageBytes = max(1, (int) (getenv('HI_KAFKA_E2E_MESSAGE_BYTES') ?: 64 * 1024));
$phaseFile = getenv('HI_KAFKA_E2E_PHASE_FILE') ?: null;
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

$expected = [];
if ($phaseFile !== null) {
    file_put_contents($phaseFile, "produce:0\n", LOCK_EX);
}
for ($i = 0; $i < $messageCount; $i++) {
    $key = "v2-{$i}";
    $value = str_repeat(chr(65 + ($i % 26)), $messageBytes);
    $expected[$key] = hash('sha256', $value);
    $result = $client->produceSync('default', $topic, $key, $value, [], null, null, 10_000);
    if (! ($result['ok'] ?? false)) {
        throw new RuntimeException('produce failed: ' . json_encode($result));
    }
    if ($phaseFile !== null && ($i + 1) % 32 === 0) {
        file_put_contents($phaseFile, 'produce:' . ($i + 1) . "\n", LOCK_EX);
    }
}

$baseline = memory_get_usage(true);
$stream = $client->consume('default', 'v2-' . getmypid(), [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
]);
$stream->pause([]);
$stream->resume([]);
if ($phaseFile !== null) {
    file_put_contents($phaseFile, "consume:0\n", LOCK_EX);
}

$seen = [];
$deadline = microtime(true) + max(30, (int) ceil($messageCount / 20));
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
        if ($phaseFile !== null && count($seen) % 32 === 0) {
            file_put_contents($phaseFile, 'consume:' . count($seen) . "\n", LOCK_EX);
        }
    }
    $stream->ack($message);
    unset($message);
}

if (count($seen) !== count($expected)) {
    throw new RuntimeException('received ' . count($seen) . '/' . count($expected));
}
$stream->commit(10_000);

// commit() flushes the final deferred ACK with zero credit. A later next()
// must explicitly restore that credit rather than deadlocking the stream.
$afterCommitKey = 'v2-after-commit-' . getmypid();
$result = $client->produceSync('default', $topic, $afterCommitKey, 'after-commit', [], null, null, 10_000);
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
    throw new RuntimeException('stream did not replenish credit after commit');
}
$stream->ack($message);
$stream->commit(10_000);
$stream->close();
if ($phaseFile !== null) {
    file_put_contents($phaseFile, "consume:{$messageCount}\n", LOCK_EX);
}

$growth = memory_get_peak_usage(true) - $baseline;
if ($growth > 32 * 1024 * 1024) {
    throw new RuntimeException("PHP peak grew by {$growth} bytes for a one-message stream");
}

echo "★ ConsumerStream v2 PASS ({$messageCount} × {$messageBytes} bytes, {$growth} PHP peak bytes)\n";
