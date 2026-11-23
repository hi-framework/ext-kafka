<?php

declare(strict_types=1);

/**
 * 账本消费者：消费循环与 framework AbstractConsumer 一致——逐条 ACK，每 N 条或 1s
 * commit 一次；commit 失败记录后在下一轮重试。每条消息先校验 crc 再记账。
 *
 * 任何不可恢复的异常都让进程退出（非零），由 soak 编排器重启——与生产中进程管理器
 * 的行为一致。收到 SIGTERM 后提交最终 watermark 并关闭 stream。
 *
 * php consumer.php --socket=... --topic=... --run=... --id=c1 --group=... --dir=... \
 *     [--commit-every=64] [--runtime=blocking] [--brokers=127.0.0.1:9094]
 */

require __DIR__ . '/lib.php';

$options = parse_options($argv);
$socket = opt($options, 'socket');
$topic = opt($options, 'topic');
$run = opt($options, 'run');
$id = opt($options, 'id');
$group = opt($options, 'group');
$dir = opt($options, 'dir');
$commitEvery = max(1, (int) opt($options, 'commit-every', '64'));
$runtime = opt($options, 'runtime', 'blocking');
$brokers = opt($options, 'brokers', '127.0.0.1:9094');

$stop = false;
pcntl_async_signals(true);
pcntl_signal(SIGTERM, static function () use (&$stop): void {
    $stop = true;
});
pcntl_signal(SIGINT, static function () use (&$stop): void {
    $stop = true;
});

run_in_runtime($runtime, static function () use ($socket, $topic, $run, $id, $group, $dir, $commitEvery, $runtime, $brokers, &$stop): void {
    $client = create_client($runtime, $socket);
    $client->registerCluster('default', ['bootstrap.servers' => $brokers]);
    $ledger = open_ledger("{$dir}/consumed-{$id}.log");
    log_event($dir, ['type' => 'consumer_start', 'id' => $id, 'pid' => getmypid()]);

    $stream = $client->consume('default', $group, [$topic], [
        'auto.offset.reset' => 'earliest',
        'session.timeout.ms' => '6000',
        'heartbeat.interval.ms' => '1000',
    ]);
    $pending = 0;
    $lastCommit = microtime(true);
    $commit = static function () use ($stream, $dir, $id, &$pending, &$lastCommit): void {
        try {
            $stream->commit(10_000);
            $pending = 0;
        } catch (Hi\Kafka\KafkaException $error) {
            if (! $error->retryable) {
                throw $error;
            }
            log_event($dir, ['type' => 'commit_retryable_error', 'id' => $id] + error_event($error));
        }
        $lastCommit = microtime(true);
    };

    while (! $stop) {
        $message = $stream->next(1_000);
        if (null !== $message) {
            $decoded = decode_value($message->value());
            if (null === $decoded || $decoded['run'] !== $run) {
                if (null === $decoded) {
                    ledger_write($ledger, sprintf('CORRUPT %d %d %.3f', $message->partition(), $message->offset(), microtime(true)));
                }
            } else {
                ledger_write($ledger, sprintf(
                    '%s %d %d %d %.3f',
                    $decoded['producer'],
                    $decoded['seq'],
                    $message->partition(),
                    $message->offset(),
                    microtime(true),
                ));
            }
            $stream->ack($message);
            ++$pending;
        }
        if ($pending >= $commitEvery || ($pending > 0 && microtime(true) - $lastCommit >= 1.0)) {
            $commit();
        }
    }
    if ($pending > 0) {
        $commit();
    }
    $stream->close();
    log_event($dir, ['type' => 'consumer_stop', 'id' => $id]);
});
