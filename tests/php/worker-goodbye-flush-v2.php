<?php

declare(strict_types=1);

// 回归：worker 最后的使用者退出时，MSHUTDOWN 等 worker 把 fire-and-forget 消息发完。
//
// 场景：CLI 定时任务 / 容器 1 号进程发完 FNF 就退出，容器随即结束，内核 SIGKILL 掉
// worker。修复前，还在 librdkafka 队列里的消息全部丢失（linger.ms=100 时 100 条全丢）。
//
// 1. 子进程是唯一的使用者：发 N 条 FNF 后退出；父进程在子进程退出的瞬间 SIGKILL
//    worker，模拟容器结束。之后用新 worker 发一条同步哨兵消息，哨兵的 offset 就是
//    此前送达的条数，必须等于 N。
// 2. 父进程保持连接（还有其它使用者）：子进程退出不等待发送，退出耗时远小于 linger；
//    worker 继续运行，子进程的消息随后送达。

if ($argc < 3) {
    fwrite(STDERR, "usage: worker-goodbye-flush-v2.php SOCKET TOPIC\n");
    exit(2);
}
if (! function_exists('pcntl_fork')) {
    echo "SKIP: needs pcntl\n";
    exit(0);
}

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
const MESSAGES = 200;
const LINGER_MS = 300;

/** 在子进程里发 $count 条 FNF 后退出，返回子进程从开始退出到结束的耗时（毫秒）。 */
function produce_in_child(string $socket, string $brokers, string $topic, int $count): float
{
    $pid = pcntl_fork();
    if ($pid < 0) {
        throw new RuntimeException('fork failed');
    }
    if (0 === $pid) {
        $client = new Hi\Kafka\Client($socket);
        $client->registerCluster('default', ['bootstrap.servers' => $brokers, 'linger.ms' => (string) LINGER_MS]);
        for ($i = 0; $i < $count; $i++) {
            $client->produceFnf('default', $topic, "k{$i}", str_repeat('v', 256), null, 0);
        }
        file_put_contents("{$socket}.child-done", (string) microtime(true));
        exit(0); // MSHUTDOWN → GOODBYE
    }
    pcntl_waitpid($pid, $status);
    $exited = microtime(true);
    if (! pcntl_wifexited($status) || 0 !== pcntl_wexitstatus($status)) {
        throw new RuntimeException('producer child failed');
    }
    $done = (float) file_get_contents("{$socket}.child-done");
    unlink("{$socket}.child-done");
    return ($exited - $done) * 1000;
}

/** 立即 SIGKILL worker 并等它消失（已退出 / 成为僵尸都算）。 */
function kill_worker(string $socket): void
{
    $pid = (int) trim((string) @file_get_contents("{$socket}.pid"));
    if ($pid <= 0 || ! posix_kill($pid, SIGKILL)) {
        return; // 已自行退出
    }
    $deadline = microtime(true) + 5;
    while (posix_kill($pid, 0) && ! str_contains((string) @file_get_contents("/proc/{$pid}/stat"), ') Z ')) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException("worker {$pid} survived SIGKILL");
        }
        usleep(10_000);
    }
}

/** 同步发一条哨兵到分区 0，返回它的 offset（= 此前该分区的消息数）。 */
function sentinel_offset(Hi\Kafka\Client $client, string $topic): int
{
    $result = $client->produceSync('default', $topic, 'sentinel', 'sentinel', null, 0, null, 10_000);
    if (! ($result['ok'] ?? false)) {
        throw new RuntimeException('sentinel produce failed: ' . json_encode($result));
    }
    return (int) $result['offset'];
}

// 1. 唯一使用者退出 → 等发送完成；随后 worker 被立即杀掉也不丢
$soleTopic = "{$topic}-sole";
$soleExitMs = produce_in_child($socket, $brokers, $soleTopic, MESSAGES);
kill_worker($socket);
$client = new Hi\Kafka\Client($socket);
$client->registerCluster('default', ['bootstrap.servers' => $brokers]);
$delivered = sentinel_offset($client, $soleTopic);
if (MESSAGES !== $delivered) {
    throw new RuntimeException(sprintf('sole client: %d of %d FNF messages delivered before the worker was killed', $delivered, MESSAGES));
}

// 2. 父进程仍连着 worker → 子进程退出不等待
$sharedTopic = "{$topic}-shared";
sentinel_offset($client, $sharedTopic); // 建好 topic 与 producer，父进程保持连接
$sharedExitMs = produce_in_child($socket, $brokers, $sharedTopic, 1);
if ($sharedExitMs >= LINGER_MS) {
    throw new RuntimeException(sprintf('shared worker: child exit took %.0fms, it should not wait for delivery', $sharedExitMs));
}
usleep((LINGER_MS + 700) * 1000);
$offset = sentinel_offset($client, $sharedTopic);
if (2 !== $offset) {
    throw new RuntimeException("shared worker: expected the child's message before the sentinel, sentinel offset {$offset}");
}

printf(
    "★ Worker goodbye flush v2 PASS (sole client: %d/%d delivered, exit waited %.0fms; shared worker: exit %.0fms)\n",
    $delivered,
    MESSAGES,
    $soleExitMs,
    $sharedExitMs,
);
