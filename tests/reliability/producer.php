<?php

declare(strict_types=1);

/**
 * 账本生产者：按固定速率逐条 produceSync，只有 broker 确认后才把 seq 记入 acked 账本。
 *
 * 失败的 seq 不重试、不记账（结果可能未知：消息也许已写入，消费端出现时计为
 * 「未确认但已消费」，不算丢失）。收到 SIGTERM 后写完当前消息再退出。
 *
 * php producer.php --socket=... --topic=... --run=... --id=p1 --dir=... \
 *     [--rate=200] [--bytes=1024] [--runtime=blocking] [--brokers=127.0.0.1:9094]
 */

require __DIR__ . '/lib.php';

$options = parse_options($argv);
$socket = opt($options, 'socket');
$topic = opt($options, 'topic');
$run = opt($options, 'run');
$id = opt($options, 'id');
$dir = opt($options, 'dir');
$rate = max(1, (int) opt($options, 'rate', '200'));
$bytes = max(16, (int) opt($options, 'bytes', '1024'));
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

run_in_runtime($runtime, static function () use ($socket, $topic, $run, $id, $dir, $rate, $bytes, $runtime, $brokers, &$stop): void {
    $client = create_client($runtime, $socket);
    $client->registerCluster('default', ['bootstrap.servers' => $brokers, 'linger.ms' => '1']);
    $ledger = open_ledger("{$dir}/acked-{$id}.log");
    $seqFile = "{$dir}/next-seq-{$id}";
    // 进程被重启时从上次的 seq 继续，保证同一 producer 的 seq 不复用。
    $seq = is_file($seqFile) ? (int) file_get_contents($seqFile) : 0;
    log_event($dir, ['type' => 'producer_start', 'id' => $id, 'pid' => getmypid(), 'seq' => $seq]);

    $interval = 1 / $rate;
    $next = microtime(true);
    while (! $stop) {
        $payload = substr(bin2hex(random_bytes(intdiv($bytes + 1, 2))), 0, $bytes);
        $current = $seq++;
        file_put_contents($seqFile, (string) $seq);
        try {
            $result = $client->produceSync('default', $topic, "{$id}-{$current}", encode_value($run, $id, $current, $payload), timeoutMs: 10_000);
            if ($result['ok'] ?? false) {
                ledger_write($ledger, $current . ' ' . sprintf('%.3f', microtime(true)));
            } else {
                log_event($dir, ['type' => 'produce_error', 'id' => $id, 'seq' => $current, 'result' => $result]);
            }
        } catch (Throwable $error) {
            log_event($dir, ['type' => 'produce_error', 'id' => $id, 'seq' => $current] + error_event($error));
            runtime_sleep($runtime, 0.1);
        }
        $next += $interval;
        $delay = $next - microtime(true);
        if ($delay > 0) {
            runtime_sleep($runtime, $delay);
        } elseif ($delay < -1) {
            $next = microtime(true); // 落后超过 1s 不追赶，避免恢复后突发
        }
    }
    log_event($dir, ['type' => 'producer_stop', 'id' => $id, 'seq' => $seq]);
});
