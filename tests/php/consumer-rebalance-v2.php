<?php

declare(strict_types=1);

if ($argc < 3) {
    fwrite(STDERR, "usage: consumer-rebalance-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$group = 'rebalance-v2-' . getmypid();
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);

for ($i = 0; $i < 12; $i++) {
    $result = $client->produceSync(
        'default',
        $topic,
        "rebalance-{$i}",
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

$config = [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
    'heartbeat.interval.ms' => '2000',
];
$first = $client->consume('default', $group, [$topic], $config);
$seen = [];
$accept = static function (Hi\Kafka\ConsumerStream $stream, Hi\Kafka\ConsumerRecord $message) use (&$seen): void {
    if (! str_starts_with($message->key(), 'rebalance-')) {
        throw new RuntimeException('unexpected key ' . $message->key());
    }
    $seen[$message->key()] = true;
    $stream->ack($message);
};

$deadline = microtime(true) + 20;
do {
    $message = $first->next(1_000);
    if ($message !== null) {
        $accept($first, $message);
        break;
    }
} while (microtime(true) < $deadline);
if ($seen === []) {
    throw new RuntimeException('first consumer was never assigned');
}
$first->commit(10_000);

// Joining and leaving a second logical Consumer forces real group rebalance
// callbacks through the worker actor while messages continue flowing.
$second = $client->consume('default', $group, [$topic], $config);
$joinDeadline = microtime(true) + 8;
while (count($seen) < 6 && microtime(true) < $joinDeadline) {
    foreach ([$first, $second] as $stream) {
        $message = $stream->next(250);
        if ($message !== null) {
            $accept($stream, $message);
        }
    }
}
$second->close();
unset($second);

$deadline = microtime(true) + 20;
while (count($seen) < 12 && microtime(true) < $deadline) {
    $message = $first->next(1_000);
    if ($message !== null) {
        $accept($first, $message);
    }
}
if (count($seen) !== 12) {
    throw new RuntimeException('rebalance delivered ' . count($seen) . '/12 unique messages');
}
$first->commit(10_000);
$first->close();

echo "★ Consumer rebalance v2 PASS\n";
