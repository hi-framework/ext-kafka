<?php

declare(strict_types=1);

// 回归：集群配置变化导致 Producer 换代时，旧代队列里已 ack 的 FNF 消息不能丢；
// 相同配置的重复注册（每个 PHP 进程启动都会做）不能触发换代。
//
// 旧实现直接丢弃旧 FutureProducer，rdkafka 的 Drop 会 purge 本地队列。这里用较大的
// linger.ms 让第一批消息停留在旧代队列中，再立即切换配置，最后从头消费核对全部 key。

if ($argc < 3) {
    fwrite(STDERR, "usage: producer-generation-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$perGeneration = max(1, (int) (getenv('HI_KAFKA_E2E_MESSAGES') ?: 500));
$client = new Hi\Kafka\Client($socket);

$gen1 = ['bootstrap.servers' => $brokers, 'linger.ms' => '2000', 'client.id' => 'gen-1'];
$client->registerCluster('default', $gen1);
// 预热：保证 topic 已存在、producer 已建立，后续 FNF 只进本地队列。
$warm = $client->produceSync('default', $topic, 'warmup', 'warmup', [], null, null, 10_000);
if (! ($warm['ok'] ?? false)) {
    throw new RuntimeException('warmup produce failed');
}

$expected = ['warmup' => true];
for ($i = 0; $i < $perGeneration; $i++) {
    $client->produceFnf('default', $topic, "gen1-{$i}", "v{$i}");
    $expected["gen1-{$i}"] = true;
}
// 相同配置重复注册：不得换代（换代会让上面仍在 linger 的消息走排干路径）。
$client->registerCluster('default', $gen1);
// 配置变化：触发换代。旧代必须先排干再释放。
$client->registerCluster('default', ['bootstrap.servers' => $brokers, 'client.id' => 'gen-2']);
for ($i = 0; $i < $perGeneration; $i++) {
    $client->produceFnf('default', $topic, "gen2-{$i}", "v{$i}");
    $expected["gen2-{$i}"] = true;
}
$tail = $client->produceSync('default', $topic, 'tail', 'tail', [], null, null, 10_000);
if (! ($tail['ok'] ?? false)) {
    throw new RuntimeException('tail produce failed');
}
$expected['tail'] = true;

$stream = $client->consume('default', 'generation-' . getmypid(), [$topic], [
    'auto.offset.reset' => 'earliest',
    'session.timeout.ms' => '6000',
]);
$seen = [];
$deadline = microtime(true) + 60;
while (count($seen) < count($expected) && microtime(true) < $deadline) {
    $message = $stream->next(2_000);
    if ($message === null) {
        continue;
    }
    $seen[$message->key()] = true;
    $stream->ack($message);
}
$stream->close();

$missing = array_diff_key($expected, $seen);
if ($missing !== []) {
    throw new RuntimeException(sprintf(
        'lost %d/%d messages across producer generation switch, e.g. %s',
        count($missing),
        count($expected),
        implode(',', array_slice(array_keys($missing), 0, 5)),
    ));
}

echo "★ Producer generation switch v2 PASS (" . count($expected) . " messages)\n";
