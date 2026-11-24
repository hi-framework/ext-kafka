<?php

declare(strict_types=1);

// 回归：worker 由 fork + exec 拉起，懒加载不再依赖「在线程启动之前」的时序。
//
// 1. worker 是 exec 出的全新进程：命令行是 `hi-kafka-worker -n -d extension=...`，
//    而不是父 PHP 进程命令行的拷贝（fork 不 exec 的回退路径会保留父进程命令行）。
// 2. Swoole 下先让异步 IO 线程池持续工作（进程变为多线程），再反复杀掉 worker 并
//    懒加载重新拉起：每一次都必须在超时内就绪。
// 3. hi_kafka.php_binary 指向不存在的文件时回退为进程内 fork，worker 仍然可用。

if ($argc < 3) {
    fwrite(STDERR, "usage: worker-exec-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket] = $argv;

function worker_pid(string $socket): int
{
    return (int) trim((string) @file_get_contents("{$socket}.pid"));
}

function worker_args(int $pid): string
{
    return trim((string) shell_exec('ps -o args= -p ' . $pid));
}

function thread_count(): int
{
    $status = @file_get_contents('/proc/self/status');
    if (false === $status || ! preg_match('/^Threads:\s+(\d+)/m', $status, $m)) {
        return 0; // 非 Linux：无法统计
    }
    return (int) $m[1];
}

function kill_worker(string $socket): void
{
    $pid = worker_pid($socket);
    if ($pid <= 0) {
        return;
    }
    posix_kill($pid, SIGKILL);
    $deadline = microtime(true) + 5;
    while (posix_kill($pid, 0) && microtime(true) < $deadline) {
        usleep(10_000);
    }
    if (posix_kill($pid, 0)) {
        throw new RuntimeException("worker {$pid} did not exit after SIGKILL");
    }
}

function assert_exec_worker(string $socket): int
{
    $pid = worker_pid($socket);
    if ($pid <= 0 || ! posix_kill($pid, 0)) {
        throw new RuntimeException('worker is not running');
    }
    $args = worker_args($pid);
    if (! str_starts_with($args, 'hi-kafka-worker -n -d extension=') || ! str_ends_with($args, $socket)) {
        throw new RuntimeException("worker {$pid} was not exec'd as expected: '{$args}'");
    }
    return $pid;
}

// 1. 普通拉起
hi_kafka_ensure_worker($socket);
$first = assert_exec_worker($socket);

// 2. 多线程进程中反复懒加载拉起
$rounds = 0;
$threads = 0;
$slowestMs = 0.0;
if (extension_loaded('swoole')) {
    Swoole\Coroutine\run(static function () use ($socket, &$rounds, &$threads, &$slowestMs): void {
        $busy = true;
        // 持续占用 Swoole 异步 IO 线程池，让 fork 发生在其它线程正在工作的时刻
        for ($i = 0; $i < 4; $i++) {
            Swoole\Coroutine::create(static function () use (&$busy): void {
                while ($busy) {
                    Swoole\Coroutine\System::readFile(__FILE__);
                }
            });
        }
        Swoole\Coroutine::sleep(0.2);
        $threads = thread_count();
        if (PHP_OS_FAMILY === 'Linux' && $threads < 2) {
            $busy = false;
            throw new RuntimeException("expected a multi-threaded process, got {$threads} thread(s)");
        }
        for ($rounds = 1; $rounds <= 30; $rounds++) {
            kill_worker($socket);
            $start = microtime(true);
            hi_kafka_ensure_worker($socket);
            $slowestMs = max($slowestMs, (microtime(true) - $start) * 1000);
            assert_exec_worker($socket);
        }
        $rounds--;
        $busy = false;
    });
}

// 3. 无法 exec 时回退为进程内 fork
kill_worker($socket);
putenv('HI_KAFKA_PHP_BINARY=/nonexistent/php');
hi_kafka_ensure_worker($socket);
putenv('HI_KAFKA_PHP_BINARY');
$fallback = worker_pid($socket);
$fallbackArgs = worker_args($fallback);
if ($fallback <= 0 || str_starts_with($fallbackArgs, 'hi-kafka-worker')) {
    throw new RuntimeException("fallback worker should keep the parent command line, got '{$fallbackArgs}'");
}

printf(
    "★ Worker exec v2 PASS (exec worker %d; %d multi-threaded respawns, threads=%d, slowest %.0fms; fallback worker %d)\n",
    $first,
    $rounds,
    $threads,
    $slowestMs,
    $fallback,
);
