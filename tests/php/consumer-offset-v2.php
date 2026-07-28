<?php

declare(strict_types=1);

if ($argc < 3) {
    fwrite(STDERR, "usage: consumer-offset-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$group = 'offset-v2-' . getmypid();
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

for ($i = 0; $i < 6; $i++) {
    $result = $client->produceSync(
        'default',
        $topic,
        "offset-{$i}",
        "value-{$i}",
        [],
        null,
        null,
        10_000,
    );
    if (! ($result['ok'] ?? false)) {
        throw new RuntimeException("produce {$i} failed");
    }
}

$open = static fn () => $client->consume('default', $group, [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
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

$stream = $open();
$message = $next($stream);
if ($message->key() !== 'offset-0') {
    throw new RuntimeException('expected first offset, got ' . $message->key());
}
try {
    $stream->next(100);
    throw new RuntimeException('next accepted an unsettled outstanding message');
} catch (Exception $error) {
    if (! str_contains($error->getMessage(), 'ack or nack')) {
        throw $error;
    }
}
$stream->ack($message);
$stream->commit(10_000);
$stream->close();

$stream = $open();
$message = $next($stream);
if ($message->key() !== 'offset-1') {
    throw new RuntimeException('commit did not resume at offset-1: ' . $message->key());
}
$stream->nack($message, new RuntimeException('intentional redelivery'));
unset($stream, $message);

$stream = $open();
for ($i = 1; $i < 6; $i++) {
    $message = $next($stream);
    if ($message->key() !== "offset-{$i}") {
        throw new RuntimeException("expected redelivery offset-{$i}, got " . $message->key());
    }
    $stream->ack($message);
    unset($message);
}
$stream->commit(10_000);
$stream->close();

echo "★ Consumer offset/NACK v2 PASS\n";
