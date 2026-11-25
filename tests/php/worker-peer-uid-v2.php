<?php

declare(strict_types=1);

// 回归：同一台机器上的其它用户既不能使用本用户的 worker，也不能冒充 worker。
//
// 以 root 运行，fork 出两个普通用户 A、B（umask 0，socket 文件权限位不设防，只靠
// 对端 uid 校验）：
//
// 1. A 拉起 worker；A 自己检查 socket 属主通过。
// 2. B 用原始连接发 HELLO：worker 拒绝其它用户，直接断开，不应答。
// 3. B 走扩展 ensure：客户端发现 worker 属于 A，报错且不拉起新 worker。
// 4. B 抢先在另一路径上监听；A 对该路径 ensure 报错，B 收不到任何字节；协程 driver
//    用的属主检查同样拒绝。
// 5. root 可以连接 A 的 worker（在容器里以 root 执行命令的场景）。
//
// 非 root 或缺 posix / pcntl 时 SKIP。

if ($argc < 3) {
    fwrite(STDERR, "usage: worker-peer-uid-v2.php SOCKET TOPIC\n");
    exit(2);
}
if (! function_exists('posix_setuid') || ! function_exists('pcntl_fork') || 0 !== posix_geteuid()) {
    echo "SKIP: must run as root with posix and pcntl\n";
    exit(0);
}

[$script, $socket] = $argv;
$squat = "{$socket}.squat";
const USER_A = 4001;
const USER_B = 4002;

/** 以 $uid 身份在子进程里运行 $fn，返回它的返回值。 */
function as_user(int $uid, callable $fn): mixed
{
    $pid = spawn_as($uid, $fn, $channel);
    $result = stream_get_contents($channel);
    pcntl_waitpid($pid, $status);
    $decoded = json_decode((string) $result, true);
    if (! is_array($decoded) || ! array_key_exists('result', $decoded)) {
        throw new RuntimeException("uid {$uid} child failed: {$result}");
    }
    return $decoded['result'];
}

/** fork 一个以 $uid 运行 $fn 的子进程，$channel 读到它的 JSON 结果。 */
function spawn_as(int $uid, callable $fn, mixed &$channel): int
{
    [$parent, $child] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    $pid = pcntl_fork();
    if ($pid < 0) {
        throw new RuntimeException('fork failed');
    }
    if (0 === $pid) {
        fclose($parent);
        umask(0);
        if (! posix_setgid($uid) || ! posix_setuid($uid)) {
            fwrite($child, 'setuid failed');
            exit(1);
        }
        try {
            fwrite($child, json_encode(['result' => $fn()]));
        } catch (Throwable $e) {
            fwrite($child, get_class($e) . ': ' . $e->getMessage());
        }
        fclose($child);
        exit(0);
    }
    fclose($child);
    $channel = $parent;
    return $pid;
}

/** 运行 $fn，返回其异常消息；没有抛出时返回 null。 */
function error_of(callable $fn): ?string
{
    try {
        $fn();
        return null;
    } catch (Throwable $e) {
        return $e->getMessage();
    }
}

function expect(bool $ok, string $message): void
{
    if (! $ok) {
        throw new RuntimeException($message);
    }
}

// 1. A 拉起 worker（禁用 idle 自退：A 退出时的 GOODBYE 不会让它跟着退出）
putenv('HI_KAFKA_IDLE_TIMEOUT_MS=0');
as_user(USER_A, static function () use ($socket): bool {
    hi_kafka_ensure_worker($socket);
    hi_kafka_check_socket_owner($socket);
    return true;
});
expect(USER_A === fileowner($socket), 'socket should belong to user A');
$workerPid = (int) trim((string) file_get_contents("{$socket}.pid"));

// 2. B 直接连上 socket 发 HELLO：worker 必须断开且不应答
$raw = as_user(USER_B, static function () use ($socket): array {
    $conn = stream_socket_client("unix://{$socket}", $errno, $errstr, 2);
    if (false === $conn) {
        return ['connected' => false, 'error' => $errstr];
    }
    stream_set_timeout($conn, 3);
    @fwrite($conn, hi_kafka_encode_hello_frame());
    $reply = (string) fread($conn, 64);
    return ['connected' => true, 'reply' => strlen($reply), 'eof' => feof($conn), 'timed_out' => stream_get_meta_data($conn)['timed_out']];
});
expect($raw['connected'], 'user B should reach the socket at the filesystem level (umask 0): ' . json_encode($raw));
expect(0 === $raw['reply'] && $raw['eof'] && ! $raw['timed_out'], 'worker must drop clients of another user without replying: ' . json_encode($raw));

// 3. B 走扩展：客户端拒绝 A 的 worker，也不会另行拉起
$bEnsure = as_user(USER_B, static fn (): ?string => error_of(static fn () => hi_kafka_ensure_worker($socket)));
expect(null !== $bEnsure && str_contains($bEnsure, 'served by uid ' . USER_A), "user B ensure should be refused, got: {$bEnsure}");
expect($workerPid === (int) trim((string) file_get_contents("{$socket}.pid")), 'user B must not replace the worker');

// 4. B 抢先监听另一个路径，A 不能把任何数据发给它
$squatter = spawn_as(USER_B, static function () use ($squat): array {
    $server = stream_socket_server("unix://{$squat}", $errno, $errstr);
    if (false === $server) {
        throw new RuntimeException($errstr);
    }
    $accepted = 0;
    $received = 0;
    $deadline = microtime(true) + 3;
    while (microtime(true) < $deadline) {
        $conn = @stream_socket_accept($server, max(0.05, $deadline - microtime(true)));
        if (false === $conn) {
            continue;
        }
        ++$accepted;
        stream_set_timeout($conn, 1);
        $received += strlen((string) stream_get_contents($conn));
        fclose($conn);
    }
    return ['accepted' => $accepted, 'received' => $received];
}, $squatChannel);
$deadline = microtime(true) + 5;
while (! file_exists($squat)) {
    expect(microtime(true) < $deadline, 'squatter did not start listening');
    usleep(10_000);
}
$aSquat = as_user(USER_A, static fn (): array => [
    'ensure' => error_of(static fn () => hi_kafka_ensure_worker($squat)),
    'owner' => error_of(static fn () => hi_kafka_check_socket_owner($squat)),
]);
$squatResult = json_decode((string) stream_get_contents($squatChannel), true)['result'] ?? null;
pcntl_waitpid($squatter, $status);
@unlink($squat);
expect(null !== $aSquat['ensure'] && str_contains($aSquat['ensure'], 'served by uid ' . USER_B), 'user A ensure on a squatted path should be refused, got: ' . json_encode($aSquat));
expect(null !== $aSquat['owner'] && str_contains($aSquat['owner'], 'served by uid ' . USER_B), 'owner check on a squatted path should fail, got: ' . json_encode($aSquat));
expect(is_array($squatResult) && 0 === $squatResult['received'], 'squatter must not receive any bytes: ' . json_encode($squatResult));

// 5. root 可以使用 A 的 worker
hi_kafka_ensure_worker($socket);
hi_kafka_check_socket_owner($socket);

printf(
    "★ Worker peer uid v2 PASS (other user's raw HELLO dropped, ensure refused; squatter accepted %d connection(s), received %d bytes; root allowed)\n",
    $squatResult['accepted'],
    $squatResult['received'],
);
