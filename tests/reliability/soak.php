<?php

declare(strict_types=1);

/**
 * 可靠性 soak 编排器：持续负载 + 周期故障注入 + 资源采样，结束后跑账本校验与报告。
 *
 * php tests/reliability/soak.php --out=/tmp/soak-1 --ext=target/release/libhi_kafka.dylib \
 *     [--duration=600] [--warmup=60] [--runtime=blocking] [--php-args="-d extension=swoole"] \
 *     [--rate=200] [--bytes=1024] [--consumers=2] [--partitions=4] [--commit-every=64] \
 *     [--faults=worker-kill,worker-term,worker-stop,consumer-kill,broker-restart,broker-pause] \
 *     [--fault-interval=60] [--fault-order=round-robin|random] \
 *     [--kafka-container=hi-kafka-test] [--brokers=127.0.0.1:9094] \
 *     [--sample-interval=5] [--drain-timeout=120] [--seed=N] [--metrics-addr=127.0.0.1:19876]
 *
 * 编排器自身不加载扩展；所有进程按 PID 管理（worker 由扩展 fork，命令行与父进程相同，
 * 不能按命令行匹配）。
 */

require __DIR__ . '/lib.php';

$options = parse_options($argv);
$out = opt($options, 'out');
$ext = realpath(opt($options, 'ext')) ?: throw new RuntimeException('extension not found');
$params = [
    'duration' => (int) opt($options, 'duration', '600'),
    'warmup' => (int) opt($options, 'warmup', '60'),
    'runtime' => opt($options, 'runtime', 'blocking'),
    'php_args' => opt($options, 'php-args', ''),
    'rate' => (int) opt($options, 'rate', '200'),
    'bytes' => (int) opt($options, 'bytes', '1024'),
    'consumers' => (int) opt($options, 'consumers', '2'),
    'partitions' => (int) opt($options, 'partitions', '4'),
    'commit_every' => (int) opt($options, 'commit-every', '64'),
    'faults' => array_values(array_filter(explode(',', opt($options, 'faults', 'worker-kill,worker-term,worker-stop,consumer-kill,broker-restart,broker-pause')))),
    'fault_interval' => (int) opt($options, 'fault-interval', '60'),
    // 默认轮询，保证每种故障都被覆盖；random 用于长时间运行时打散组合。
    'fault_order' => opt($options, 'fault-order', 'round-robin'),
    'kafka_container' => opt($options, 'kafka-container', 'hi-kafka-test'),
    'brokers' => opt($options, 'brokers', '127.0.0.1:9094'),
    'sample_interval' => (int) opt($options, 'sample-interval', '5'),
    'drain_timeout' => (int) opt($options, 'drain-timeout', '120'),
    'seed' => (int) opt($options, 'seed', (string) random_int(1, PHP_INT_MAX)),
    'metrics_addr' => opt($options, 'metrics-addr', '127.0.0.1:19876'),
    'ext' => $ext,
];
mt_srand($params['seed']);

if (! is_dir($out) && ! mkdir($out, 0777, true)) {
    throw new RuntimeException("cannot create {$out}");
}
$run = 'soak-' . date('Ymd-His') . '-' . getmypid();
$topic = $run;
$socket = "{$out}/worker.sock";
$params += ['run' => $run, 'topic' => $topic, 'socket' => $socket, 'started_at' => microtime(true)];
file_put_contents("{$out}/params.json", json_encode($params, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n");

$dockerEnv = ['DOCKER_HOST' => getenv('DOCKER_HOST') ?: 'unix://' . getenv('HOME') . '/.colima/default/docker.sock'];
$docker = static function (array $args, bool $wait = true) use ($dockerEnv): void {
    $process = proc_open(['docker', ...$args], [1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes, null, getenv() + $dockerEnv);
    if ($wait && is_resource($process)) {
        proc_close($process);
    }
};
// bitnami 镜像把 kafka-topics.sh 放在 PATH 上，apache/kafka 镜像在 /opt/kafka/bin。
$kafkaTopics = static fn (array $args) => $docker(['exec', $params['kafka_container'], 'sh', '-c',
    'PATH="$PATH:/opt/kafka/bin:/opt/bitnami/kafka/bin" kafka-topics.sh --bootstrap-server 127.0.0.1:9092 ' . implode(' ', array_map('escapeshellarg', $args))]);
$kafkaTopics(['--create', '--topic', $topic, '--partitions', (string) $params['partitions'], '--replication-factor', '1']);

// ---------------------------------------------------------------- processes
$phpCommand = array_merge([PHP_BINARY, '-n', '-d', "extension={$ext}"], preg_split('/\s+/', trim($params['php_args'])) ?: []);
$phpCommand = array_values(array_filter($phpCommand, static fn ($part) => '' !== $part));
$childEnv = getenv() + ['HI_KAFKA_METRICS_ADDR' => $params['metrics_addr']];

/** @var array<string,array{cmd:list<string>,proc:resource|null,pid:int,restarts:int}> $procs */
$procs = [];
$spawn = static function (string $name) use (&$procs, $out, $childEnv): void {
    $log = "{$out}/proc-{$name}.log";
    $proc = proc_open($procs[$name]['cmd'], [0 => ['file', '/dev/null', 'r'], 1 => ['file', $log, 'a'], 2 => ['file', $log, 'a']], $pipes, null, $childEnv);
    $procs[$name]['proc'] = $proc;
    $procs[$name]['pid'] = proc_get_status($proc)['pid'];
};
$common = ["--socket={$socket}", "--topic={$topic}", "--run={$run}", "--dir={$out}", "--runtime={$params['runtime']}", "--brokers={$params['brokers']}"];
$procs['producer'] = ['cmd' => [...$phpCommand, __DIR__ . '/producer.php', ...$common, '--id=p1', "--rate={$params['rate']}", "--bytes={$params['bytes']}"], 'proc' => null, 'pid' => 0, 'restarts' => 0];
for ($i = 1; $i <= $params['consumers']; ++$i) {
    $procs["c{$i}"] = ['cmd' => [...$phpCommand, __DIR__ . '/consumer.php', ...$common, "--id=c{$i}", "--group=group-{$run}", "--commit-every={$params['commit_every']}"], 'proc' => null, 'pid' => 0, 'restarts' => 0];
}
foreach (array_keys($procs) as $name) {
    $spawn($name);
}

$supervise = static function (bool $stopping) use (&$procs, $spawn, $out): void {
    foreach ($procs as $name => $info) {
        if (null === $info['proc']) {
            continue;
        }
        $status = proc_get_status($info['proc']);
        if ($status['running']) {
            continue;
        }
        proc_close($info['proc']);
        $procs[$name]['proc'] = null;
        log_event($out, ['type' => 'process_exit', 'name' => $name, 'pid' => $info['pid'], 'exitcode' => $status['exitcode'], 'signaled' => $status['signaled'], 'termsig' => $status['termsig']]);
        if (! $stopping) {
            ++$procs[$name]['restarts'];
            $spawn($name);
        }
    }
};

$workerPid = static function () use ($socket): int {
    $pid = is_file("{$socket}.pid") ? (int) trim((string) @file_get_contents("{$socket}.pid")) : 0;
    return $pid > 0 && posix_kill($pid, 0) ? $pid : 0;
};

// ---------------------------------------------------------------- faults
$activeFault = null;
$recordFault = static function (array $fault) use ($out): void {
    file_put_contents("{$out}/faults.jsonl", json_encode($fault, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
};
$inject = static function (string $name) use (&$procs, $workerPid, $docker, $params, &$activeFault, $recordFault): void {
    $now = microtime(true);
    $fault = ['fault' => $name, 'start' => $now, 'end' => $now];
    switch ($name) {
        case 'worker-kill':
        case 'worker-term':
        case 'worker-stop':
            $pid = $workerPid();
            if (0 === $pid) {
                $fault['skipped'] = 'no worker';
                break;
            }
            $fault['pid'] = $pid;
            $signal = ['worker-kill' => SIGKILL, 'worker-term' => SIGTERM, 'worker-stop' => SIGSTOP][$name];
            posix_kill($pid, $signal);
            if ('worker-stop' === $name) {
                $fault['until'] = $now + 10;
                $fault['recover'] = static fn () => posix_kill($pid, SIGCONT);
            }
            break;
        case 'consumer-kill':
            $names = array_values(array_filter(array_keys($procs), static fn ($n) => str_starts_with($n, 'c')));
            $victim = $names[mt_rand(0, count($names) - 1)];
            $fault['victim'] = $victim;
            $fault['pid'] = $procs[$victim]['pid'];
            posix_kill($procs[$victim]['pid'], SIGKILL);
            break;
        case 'broker-restart':
            $docker(['restart', $params['kafka_container']]);
            $fault['end'] = microtime(true);
            break;
        case 'broker-pause':
            $docker(['pause', $params['kafka_container']]);
            $fault['until'] = $now + 15;
            $fault['recover'] = static fn () => $docker(['unpause', $params['kafka_container']]);
            break;
        default:
            throw new InvalidArgumentException("unknown fault {$name}");
    }
    if (isset($fault['until'])) {
        $activeFault = $fault;
    } else {
        $recordFault($fault);
    }
};
$tickFault = static function (bool $force = false) use (&$activeFault, $recordFault): void {
    if (null === $activeFault || (! $force && microtime(true) < $activeFault['until'])) {
        return;
    }
    ($activeFault['recover'])();
    $activeFault['end'] = microtime(true);
    unset($activeFault['recover'], $activeFault['until']);
    $recordFault($activeFault);
    $activeFault = null;
};

// ---------------------------------------------------------------- sampling
$lineCounts = [];
$countLines = static function (string $pattern) use (&$lineCounts): int {
    $total = 0;
    foreach (glob($pattern) as $file) {
        $state = $lineCounts[$file] ?? ['offset' => 0, 'lines' => 0];
        $handle = fopen($file, 'rb');
        fseek($handle, $state['offset']);
        while (! feof($handle)) {
            $chunk = fread($handle, 1 << 20);
            if (false === $chunk || '' === $chunk) {
                break;
            }
            $state['lines'] += substr_count($chunk, "\n");
            $state['offset'] += strlen($chunk);
        }
        fclose($handle);
        $lineCounts[$file] = $state;
        $total += $state['lines'];
    }
    return $total;
};
$rss = static function (int $pid): int {
    return $pid > 0 ? (int) trim((string) shell_exec("ps -o rss= -p {$pid} 2>/dev/null")) : 0;
};
$fds = static function (int $pid): int {
    return $pid > 0 ? max(0, (int) trim((string) shell_exec("lsof -p {$pid} 2>/dev/null | wc -l")) - 1) : 0;
};
$metrics = static function () use ($params): array {
    $context = stream_context_create(['http' => ['timeout' => 1]]);
    $text = @file_get_contents("http://{$params['metrics_addr']}/metrics", false, $context);
    $values = [];
    foreach (explode("\n", (string) $text) as $line) {
        if (preg_match('/^(hi_kafka_[a-z_]+)\s+([0-9.eE+-]+)$/', $line, $match)) {
            $values[$match[1]] = (float) $match[2];
        }
    }
    return $values;
};
$sample = static function (string $phase) use ($out, $workerPid, $rss, $fds, $metrics, $countLines, &$procs, &$activeFault): void {
    $pid = $workerPid();
    $php = [];
    foreach ($procs as $name => $info) {
        $php[$name] = $rss($info['pid']);
    }
    $row = [
        'ts' => microtime(true),
        'phase' => $phase,
        'fault_active' => null !== $activeFault,
        'worker_pid' => $pid,
        'worker_rss_kib' => $rss($pid),
        'worker_fds' => $fds($pid),
        'php_rss_kib' => $php,
        'acked' => $countLines("{$out}/acked-*.log"),
        'consumed' => $countLines("{$out}/consumed-*.log"),
        'metrics' => $pid > 0 ? $metrics() : [],
    ];
    file_put_contents("{$out}/samples.jsonl", json_encode($row, JSON_UNESCAPED_SLASHES) . "\n", FILE_APPEND);
};

// ---------------------------------------------------------------- main loop
$stopRequested = false;
pcntl_async_signals(true);
pcntl_signal(SIGINT, static function () use (&$stopRequested): void {
    $stopRequested = true;
});

$start = microtime(true);
$nextSample = $start;
$nextFault = $start + $params['warmup'] + $params['fault_interval'];
$faultCount = 0;
fwrite(STDERR, "soak {$run}: duration={$params['duration']}s faults=" . implode(',', $params['faults']) . " out={$out}\n");
while (! $stopRequested && microtime(true) - $start < $params['duration']) {
    $supervise(false);
    $tickFault();
    $now = microtime(true);
    if ($now >= $nextSample) {
        $sample($now - $start < $params['warmup'] ? 'warmup' : 'load');
        $nextSample += $params['sample_interval'];
    }
    if ([] !== $params['faults'] && null === $activeFault && $now >= $nextFault) {
        $name = 'random' === $params['fault_order']
            ? $params['faults'][mt_rand(0, count($params['faults']) - 1)]
            : $params['faults'][$faultCount % count($params['faults'])];
        ++$faultCount;
        fwrite(STDERR, sprintf("[%5.0fs] inject %s\n", $now - $start, $name));
        $inject($name);
        $nextFault = microtime(true) + $params['fault_interval'];
    }
    usleep(200_000);
}
$tickFault(true);

// ---------------------------------------------------------------- shutdown & drain
$stopProcess = static function (string $name, int $graceSeconds) use (&$procs): void {
    $info = $procs[$name];
    if (null === $info['proc']) {
        return;
    }
    posix_kill($info['pid'], SIGTERM);
    $deadline = microtime(true) + $graceSeconds;
    while (proc_get_status($info['proc'])['running'] && microtime(true) < $deadline) {
        usleep(100_000);
    }
    if (proc_get_status($info['proc'])['running']) {
        posix_kill($info['pid'], SIGKILL);
    }
    proc_close($info['proc']);
    $procs[$name]['proc'] = null;
};
fwrite(STDERR, "stopping producer, draining consumers...\n");
$stopProcess('producer', 15);
$acked = $countLines("{$out}/acked-*.log");
$drainDeadline = microtime(true) + $params['drain_timeout'];
$lastConsumed = -1;
$lastProgress = microtime(true);
while (microtime(true) < $drainDeadline) {
    $supervise(false);
    $consumed = $countLines("{$out}/consumed-*.log");
    if ($consumed !== $lastConsumed) {
        $lastConsumed = $consumed;
        $lastProgress = microtime(true);
    }
    // 消费记录数达到确认数且 10s 无新进度，视为排空（重复投递会让记录数超过确认数）。
    if ($consumed >= $acked && microtime(true) - $lastProgress > 10) {
        break;
    }
    if (microtime(true) >= $nextSample) {
        $sample('drain');
        $nextSample += $params['sample_interval'];
    }
    usleep(500_000);
}
$sample('drain');
foreach (array_keys($procs) as $name) {
    if ('producer' !== $name) {
        $stopProcess($name, 15);
    }
}
$supervise(true);
if (($pid = $workerPid()) > 0) {
    posix_kill($pid, SIGTERM);
}
$kafkaTopics(['--delete', '--topic', $topic]);

$restarts = array_map(static fn ($info) => $info['restarts'], $procs);
file_put_contents("{$out}/restarts.json", json_encode($restarts) . "\n");

passthru(escapeshellarg(PHP_BINARY) . ' -n ' . escapeshellarg(__DIR__ . '/ledger-check.php') . ' --dir=' . escapeshellarg($out) . ' > /dev/null');
passthru(escapeshellarg(PHP_BINARY) . ' -n ' . escapeshellarg(__DIR__ . '/report.php') . ' --dir=' . escapeshellarg($out), $verdict);
exit($verdict);
