<?php

declare(strict_types=1);

// 回归：每种 Client（阻塞 Client / SwooleClient / SwowClient）在 worker 崩溃后必须自愈。
//
// 场景 A：worker 被 kill，本进程下一次请求自己拉起新 worker 并重放集群注册。
// 场景 B：worker 被 kill 后由**另一个从未注册过集群的进程**拉起。本进程旧连接 EOF，
//         探测发现 worker 已在运行（不会触发重放），随后收到 CLUSTER_NOT_REGISTERED，
//         必须重放本进程记录的集群后重试成功。
//
// 旧实现：SwooleClient 的 workerEnsured 一旦置位不再复位，且注册不进扩展重放缓存，
// 两个场景都会持续失败直到进程重启。

if ($argc < 3) {
    fwrite(STDERR, "usage: client-recovery-v2.php SOCKET TOPIC\n");
    exit(2);
}
// 按当前加载的运行时选择 Client（swoole 与 swow 不能同时加载）；都没有时测阻塞 Client。
$runtime = match (true) {
    extension_loaded('swoole') => 'swoole',
    extension_loaded('swow') => 'swow',
    default => 'blocking',
};

require __DIR__ . '/../../php-driver/vendor/autoload.php';

[$script, $socket, $topic] = $argv;
$brokers = getenv('HI_KAFKA_BROKERS') ?: '127.0.0.1:9094';
$extension = getenv('HI_KAFKA_EXT_PATH') ?: throw new RuntimeException('HI_KAFKA_EXT_PATH is required');

$killWorker = static function () use ($socket): void {
    $pid = is_file("{$socket}.pid") ? (int) trim((string) file_get_contents("{$socket}.pid")) : 0;
    if ($pid <= 0) {
        throw new RuntimeException('worker pid file missing');
    }
    posix_kill($pid, SIGKILL);
    for ($i = 0; $i < 50 && posix_kill($pid, 0); $i++) {
        usleep(20_000);
    }
    @unlink($socket);
};

$produce = static function (Hi\Kafka\ClientInterface $client, string $key) use ($topic): void {
    $result = $client->produceSync('default', $topic, $key, $key, timeoutMs: 10_000);
    if (! ($result['ok'] ?? false)) {
        throw new RuntimeException("produce {$key} failed: " . json_encode($result));
    }
};

$scenario = static function () use ($runtime, $socket, $brokers, $extension, $killWorker, $produce): void {
    $client = match ($runtime) {
        'swoole' => new Hi\Kafka\SwooleClient($socket),
        'swow' => new Hi\Kafka\SwowClient($socket),
        default => new Hi\Kafka\Client($socket),
    };
    $client->registerCluster('default', ['bootstrap.servers' => $brokers]);
    $produce($client, 'before-kill');

    // 场景 A：本进程自愈。
    $killWorker();
    $produce($client, 'after-kill-self-spawn');
    echo "  scenario A: recovered by self spawn\n";

    // 场景 B：另一个进程拉起 worker，并持有一条连接防止 worker 空闲自退。
    $killWorker();
    $code = sprintf(
        'hi_kafka_ensure_worker(%s); $c = stream_socket_client(%s); echo "READY\n"; fflush(STDOUT); sleep(5);',
        var_export($socket, true),
        var_export("unix://{$socket}", true),
    );
    $process = proc_open(
        [PHP_BINARY, '-n', '-d', "extension={$extension}", '-r', $code],
        [1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
        $pipes,
    );
    if (! is_resource($process) || trim((string) fgets($pipes[1])) !== 'READY') {
        throw new RuntimeException('helper process failed: ' . stream_get_contents($pipes[2]));
    }
    $produce($client, 'after-foreign-spawn');
    echo "  scenario B: recovered by replaying registrations\n";
    proc_terminate($process);
    proc_close($process);

    if (method_exists($client, 'close')) {
        $client->close();
    }
};

// 协程内的异常在两个 runtime 下的传播方式不同；统一捕获后在协程外重新抛出。
$failure = null;
$guarded = static function () use ($scenario, &$failure): void {
    try {
        $scenario();
    } catch (Throwable $error) {
        $failure = $error;
    }
};
if ('swoole' === $runtime) {
    Swoole\Coroutine\run($guarded);
} elseif ('swow' === $runtime) {
    Swow\Coroutine::run($guarded);
    Swow\Sync\waitAll();
} else {
    $guarded();
}
if (null !== $failure) {
    throw $failure;
}

echo "★ Client recovery v2 PASS ({$runtime})\n";
