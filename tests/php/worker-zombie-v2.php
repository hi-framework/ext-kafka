<?php

declare(strict_types=1);

// 回归：worker 拉起与退出不留僵尸进程。
//
// worker 由 double fork 拉起后被「最近的 subreaper 或 1 号进程」收养。本测试必须以
// 容器 1 号进程运行（tests/linux/run.sh 单独启动一个容器跑它），即「容器里 1 号进程
// 就是业务 PHP 进程」的部署方式，然后分两种情况反复杀掉 worker 再懒加载拉起：
//
// 1. 收养者不回收子进程：每次 worker 退出恰好留下 1 个僵尸（double fork 的中间进程
//    已被 spawner 回收，不额外留僵尸）；僵尸不影响下一次拉起；收养者补一次 waitpid
//    就能全部清掉。
// 2. 收养者在 SIGCHLD 里 waitpid(-1)（tini / php-fpm master / 带 SIGCHLD 处理的
//    Swoole、pcntl 进程）：僵尸始终为 0，处理器与 spawner 对中间进程的 waitpid
//    并存时拉起仍然成功。
//
// 不是 1 号进程（例如在 run-all-e2e.sh 里）或缺 /proc、pcntl 时 SKIP。

if ($argc < 3) {
    fwrite(STDERR, "usage: worker-zombie-v2.php SOCKET TOPIC\n");
    exit(2);
}

[$script, $socket] = $argv;

if (PHP_OS_FAMILY !== 'Linux' || 1 !== getmypid() || ! function_exists('pcntl_waitpid')) {
    echo "SKIP: must run as PID 1 of a Linux container with pcntl (see tests/linux/run.sh)\n";
    exit(0);
}

const ROUNDS = 10;

$self = getmypid();

/**
 * 本进程的直接子进程：pid => 状态字母（R/S/Z...）。
 *
 * @return array<int, string>
 */
function children(int $parent): array
{
    $children = [];
    foreach (glob('/proc/[0-9]*/stat') ?: [] as $file) {
        $stat = @file_get_contents($file);
        if (false === $stat) {
            continue; // 扫描期间退出并被回收
        }
        // comm 可能含空格和括号，从最后一个 ')' 之后解析：state ppid ...
        $fields = explode(' ', substr($stat, strrpos($stat, ')') + 2));
        if ((int) $fields[1] === $parent) {
            $children[(int) basename(dirname($file))] = $fields[0];
        }
    }
    return $children;
}

/** @return list<int> */
function zombies(int $parent): array
{
    return array_keys(array_filter(children($parent), static fn (string $state): bool => 'Z' === $state));
}

function worker_pid(string $socket): int
{
    return (int) trim((string) @file_get_contents("{$socket}.pid"));
}

function wait_until(callable $condition, float $seconds, string $what): void
{
    $deadline = microtime(true) + $seconds;
    while (! $condition()) {
        if (microtime(true) > $deadline) {
            throw new RuntimeException("timed out waiting for {$what}");
        }
        usleep(10_000);
    }
}

/** 拉起 worker，并确认它被本进程收养、子进程里没有残留的中间进程。 */
function spawn(string $socket, int $self, array $expectedZombies): int
{
    hi_kafka_ensure_worker($socket);
    $worker = worker_pid($socket);
    $children = children($self);
    if (! isset($children[$worker]) || 'Z' === $children[$worker]) {
        throw new RuntimeException("worker {$worker} should be a live child of PID 1, children: " . json_encode($children));
    }
    $unexpected = array_diff(array_keys($children), [$worker], $expectedZombies);
    if ([] !== $unexpected) {
        throw new RuntimeException('unexpected children after spawn: ' . json_encode(array_intersect_key($children, array_flip($unexpected))));
    }
    return $worker;
}

/** 交替用 SIGKILL（崩溃）与 SIGTERM（优雅退出）结束 worker。 */
function stop(int $worker, int $round): void
{
    posix_kill($worker, 0 === $round % 2 ? SIGKILL : SIGTERM);
}

// 1. 收养者不回收：僵尸逐个累积，且只来自 worker 本身
$kept = [];
for ($round = 0; $round < ROUNDS; $round++) {
    $worker = spawn($socket, $self, $kept);
    stop($worker, $round);
    wait_until(static fn (): bool => (children($self)[$worker] ?? '') === 'Z', 15, "worker {$worker} to exit");
    $kept[] = $worker;
    $found = zombies($self);
    sort($found);
    if ($found !== $kept) {
        throw new RuntimeException('zombies should be exactly the exited workers: ' . json_encode(['zombies' => $found, 'workers' => $kept]));
    }
}
$live = spawn($socket, $self, $kept); // 10 个僵尸在场也能正常拉起

$reaped = 0;
while (($pid = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
    $reaped++;
}
if ($reaped !== ROUNDS || [] !== zombies($self) || ! posix_kill($live, 0)) {
    throw new RuntimeException("waitpid should reap all " . ROUNDS . " zombies and keep the live worker, reaped {$reaped}");
}

// 2. 收养者在 SIGCHLD 里回收：僵尸始终为 0
$reapedByHandler = 0;
pcntl_async_signals(true);
pcntl_signal(SIGCHLD, static function () use (&$reapedByHandler): void {
    while (pcntl_waitpid(-1, $status, WNOHANG) > 0) {
        $reapedByHandler++;
    }
});
for ($round = 0; $round < ROUNDS; $round++) {
    $worker = $round === 0 ? $live : spawn($socket, $self, []);
    stop($worker, $round);
    wait_until(static fn (): bool => ! isset(children($self)[$worker]), 15, "worker {$worker} to be reaped");
    if ([] !== zombies($self)) {
        throw new RuntimeException('reaping parent should leave no zombies: ' . json_encode(children($self)));
    }
}
$last = spawn($socket, $self, []);

posix_kill($last, SIGTERM);
wait_until(static fn (): bool => ! isset(children($self)[$last]), 15, "worker {$last} to be reaped");
pcntl_signal(SIGCHLD, SIG_DFL);

printf(
    "★ Worker zombie v2 PASS (non-reaping parent: %d zombies from %d exits, all reaped; reaping parent: 0 zombies, %d reaped in SIGCHLD)\n",
    $reaped,
    ROUNDS,
    $reapedByHandler,
);
