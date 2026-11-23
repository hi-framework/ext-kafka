<?php

declare(strict_types=1);

// 回归：扩展拉起的 worker 必须与 PHP 进程彻底分离。
//
// 1. 不继承 PHP 的 fd：PHP 监听过的端口在 PHP 关闭后必须能重新 bind。旧实现 fork 后
//    只处理了 stdio，worker 长期持有继承的监听 socket / 数据库连接 / spawn 锁。
// 2. 不是 PHP 进程的子进程（double fork）：避免僵尸进程，也不干扰 FPM / Swoole
//    manager 的 SIGCHLD 处理。
// 3. spawn 锁在启动完成后释放。

if ($argc < 3) {
    fwrite(STDERR, "usage: worker-spawn-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket] = $argv;

$listener = stream_socket_server('tcp://127.0.0.1:0', $errno, $error);
if (false === $listener) {
    throw new RuntimeException("listen failed: {$error}");
}
$address = stream_socket_get_name($listener, false);

hi_kafka_ensure_worker($socket);
$pid = (int) trim((string) @file_get_contents("{$socket}.pid"));
if ($pid <= 0 || ! posix_kill($pid, 0)) {
    throw new RuntimeException('worker is not running');
}

fclose($listener);
$rebound = @stream_socket_server("tcp://{$address}", $errno, $error);
if (false === $rebound) {
    throw new RuntimeException("worker inherited the PHP listening socket {$address}: {$error}");
}
fclose($rebound);

$parent = (int) trim((string) shell_exec('ps -o ppid= -p ' . $pid));
if ($parent === getmypid()) {
    throw new RuntimeException('worker is a direct child of the PHP process (expected double fork)');
}

$lock = fopen("{$socket}.spawn-lock", 'c');
if (false === $lock || ! flock($lock, LOCK_EX | LOCK_NB)) {
    throw new RuntimeException('spawn lock is still held after the worker became ready');
}
flock($lock, LOCK_UN);
fclose($lock);

echo "★ Worker spawn isolation v2 PASS (worker {$pid}, parent {$parent})\n";
