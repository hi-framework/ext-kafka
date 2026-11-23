<?php

declare(strict_types=1);

/**
 * 把一次 soak 的原始数据变成逐条判据的报告（report.json + report.md），全部通过时退出码 0。
 *
 * 判据与默认阈值（可用参数覆盖，按业务 SLA 调整）：
 * - missing == 0、corrupted == 0                         不丢、不损坏
 * - duplicates <= (commit_every + rate) × 扰动次数        重复只来自未提交窗口
 * - 非故障注入导致的进程崩溃 == 0（consumer-kill 的 SIGKILL 不计入）
 * - 故障后生产/消费恢复时间最大值 <= --max-recovery=30s
 * - 故障窗口之外的消费停顿 <= --max-stall=30s
 * - worker RSS 峰值 <= --max-worker-rss-mib=512
 * - 同一 worker 进程内、持续 >= --min-slope-window=1800s 的 RSS / fd 增长斜率
 *   <= --max-rss-slope=5 MiB/h、--max-fd-slope=10/h（更短的运行给出 N/A，不判通过）
 *
 * php report.php --dir=RUN_DIR [阈值参数...]
 */

require __DIR__ . '/lib.php';

// 账本时间戳全部载入内存做二分查找：内存随 soak 时长线性增长（2h / 200 msg/s 约
// 280 万个时间戳，超过 php -n 默认的 128M）。离线工具，不设上限。
ini_set('memory_limit', '-1');

$options = parse_options($argv);
$dir = opt($options, 'dir');
$limits = [
    'max_recovery' => (float) opt($options, 'max-recovery', '30'),
    'max_stall' => (float) opt($options, 'max-stall', '30'),
    'max_worker_rss_mib' => (float) opt($options, 'max-worker-rss-mib', '512'),
    'max_rss_slope' => (float) opt($options, 'max-rss-slope', '5'),
    'max_fd_slope' => (float) opt($options, 'max-fd-slope', '10'),
    'min_slope_window' => (float) opt($options, 'min-slope-window', '1800'),
];

$params = json_decode((string) file_get_contents("{$dir}/params.json"), true);
$ledger = json_decode((string) file_get_contents("{$dir}/ledger.json"), true);
$restarts = is_file("{$dir}/restarts.json") ? json_decode((string) file_get_contents("{$dir}/restarts.json"), true) : [];
$readJsonl = static function (string $file): array {
    if (! is_file($file)) {
        return [];
    }
    return array_map(static fn ($line) => json_decode($line, true), array_filter(explode("\n", (string) file_get_contents($file))));
};
$samples = $readJsonl("{$dir}/samples.jsonl");
$faults = $readJsonl("{$dir}/faults.jsonl");
$events = $readJsonl("{$dir}/events.jsonl");

/** 读取账本中的时间戳（acked 第 2 列，consumed 第 5 列），排序后返回。 */
$timestamps = static function (string $pattern, int $column): array {
    $values = [];
    foreach (glob($pattern) as $file) {
        $handle = fopen($file, 'rb');
        while (false !== ($line = fgets($handle))) {
            $parts = explode(' ', trim($line));
            if (isset($parts[$column]) && ! str_starts_with($line, 'CORRUPT')) {
                $values[] = (float) $parts[$column];
            }
        }
        fclose($handle);
    }
    sort($values);
    return $values;
};
$ackTs = $timestamps("{$dir}/acked-*.log", 1);
$consumeTs = $timestamps("{$dir}/consumed-*.log", 4);

/** 第一个 >= $t 的时间戳（二分）。 */
$firstAfter = static function (array $sorted, float $t): ?float {
    $lo = 0;
    $hi = count($sorted);
    while ($lo < $hi) {
        $mid = intdiv($lo + $hi, 2);
        if ($sorted[$mid] < $t) {
            $lo = $mid + 1;
        } else {
            $hi = $mid;
        }
    }
    return $sorted[$lo] ?? null;
};

// ---------------------------------------------------------------- recovery
// 负载结束时间取计划值，不能取最后一个 ack：真正卡死时最后一个 ack 就停在故障时刻，
// 用它做边界会把卡死的故障误标为「无法判定」。
$loadEnd = (float) $params['started_at'] + (float) $params['duration'];
$recoveries = [];
foreach ($faults as $fault) {
    if (isset($fault['skipped'])) {
        continue;
    }
    $end = (float) $fault['end'];
    // 故障结束距负载结束不足观察窗口：恢复与否无法判定（生产者已停止，不再有新 ack）。
    if ($end + $limits['max_recovery'] > $loadEnd) {
        $recoveries[] = ['fault' => $fault['fault'], 'at' => round($fault['start'] - $params['started_at'], 1), 'duration' => round($end - $fault['start'], 1), 'produce_recovery' => 'N/A', 'consume_recovery' => 'N/A'];
        continue;
    }
    $produce = $firstAfter($ackTs, $end);
    $consume = $firstAfter($consumeTs, $end);
    $recoveries[] = [
        'fault' => $fault['fault'],
        'at' => round($fault['start'] - $params['started_at'], 1),
        'duration' => round($end - $fault['start'], 1),
        'produce_recovery' => null === $produce ? null : round($produce - $end, 2),
        'consume_recovery' => null === $consume ? null : round($consume - $end, 2),
    ];
}
$maxRecovery = 0.0;
$unrecovered = 0;
foreach ($recoveries as $r) {
    foreach (['produce_recovery', 'consume_recovery'] as $key) {
        if ('N/A' === $r[$key]) {
            continue;
        }
        if (null === $r[$key]) {
            ++$unrecovered;
        } else {
            $maxRecovery = max($maxRecovery, $r[$key]);
        }
    }
}

// ---------------------------------------------------------------- stalls
$windows = array_map(static fn ($f) => [(float) $f['start'], (float) $f['end'] + $limits['max_recovery']], $faults);
$inFaultWindow = static function (float $a, float $b) use ($windows): bool {
    foreach ($windows as [$start, $end]) {
        if ($a < $end && $b > $start) {
            return true;
        }
    }
    return false;
};
$maxStall = 0.0;
$stallAt = null;
for ($i = 1, $n = count($consumeTs); $i < $n; ++$i) {
    $gap = $consumeTs[$i] - $consumeTs[$i - 1];
    if ($consumeTs[$i - 1] > $loadEnd) {
        break; // 生产停止后的排空阶段不计
    }
    if ($gap > $maxStall && ! $inFaultWindow($consumeTs[$i - 1], $consumeTs[$i])) {
        $maxStall = $gap;
        $stallAt = round($consumeTs[$i - 1] - $params['started_at'], 1);
    }
}

// ---------------------------------------------------------------- resources
$loadSamples = array_values(array_filter($samples, static fn ($s) => 'warmup' !== $s['phase'] && $s['worker_pid'] > 0));
$workerRssMax = 0;
$segments = [];
foreach ($loadSamples as $s) {
    $workerRssMax = max($workerRssMax, $s['worker_rss_kib']);
    $segments[$s['worker_pid']][] = $s;
}
/** 最小二乘斜率（每小时）。 */
$slope = static function (array $rows, string $field, float $scale): float {
    $n = count($rows);
    $mx = array_sum(array_column($rows, 'ts')) / $n;
    $my = array_sum(array_column($rows, $field)) / $n;
    $num = $den = 0.0;
    foreach ($rows as $row) {
        $num += ($row['ts'] - $mx) * ($row[$field] - $my);
        $den += ($row['ts'] - $mx) ** 2;
    }
    return 0.0 == $den ? 0.0 : $num / $den * 3600 / $scale;
};
$longest = null;
foreach ($segments as $pid => $rows) {
    $span = end($rows)['ts'] - $rows[0]['ts'];
    if (null === $longest || $span > $longest['span']) {
        $longest = ['pid' => $pid, 'span' => $span, 'rows' => $rows];
    }
}
$slopeResult = null;
if (null !== $longest && count($longest['rows']) >= 3) {
    $slopeResult = [
        'worker_pid' => $longest['pid'],
        'window_seconds' => round($longest['span']),
        'rss_mib_per_hour' => round($slope($longest['rows'], 'worker_rss_kib', 1024), 2),
        'fds_per_hour' => round($slope($longest['rows'], 'worker_fds', 1), 2),
    ];
}

// ---------------------------------------------------------------- crashes
// 进程自己退出（非零退出码）或被 SIGKILL 以外的信号终止，都意味着客户端没能在故障中
// 自愈，只能依赖进程管理器重启。consumer-kill 注入的 SIGKILL 是预期内的。
$crashes = array_values(array_filter($events, static fn ($e) => 'process_exit' === ($e['type'] ?? null)
    // 9 = SIGKILL；报告由 php -n 运行，不能依赖 pcntl 提供的信号常量。
    && ! ($e['signaled'] && 9 === $e['termsig'])
    && 0 !== $e['exitcode']));

// ---------------------------------------------------------------- verdict
$disruptions = count(array_filter($faults, static fn ($f) => ! isset($f['skipped'])))
    + array_sum(array_map(static fn ($n) => (int) $n, $restarts));
$dupBound = ($params['commit_every'] + $params['rate']) * max(1, $disruptions);
$slopeMeasurable = null !== $slopeResult && $slopeResult['window_seconds'] >= $limits['min_slope_window'];
$criteria = [
    ['name' => '不丢消息（acked 均被消费）', 'value' => $ledger['missing'], 'limit' => '0', 'pass' => 0 === $ledger['missing']],
    ['name' => '无损坏（crc 校验）', 'value' => $ledger['corrupted'], 'limit' => '0', 'pass' => 0 === $ledger['corrupted']],
    ['name' => '重复有界', 'value' => $ledger['duplicates'], 'limit' => "≤ {$dupBound}（(commit_every + rate) × {$disruptions} 次扰动）", 'pass' => $ledger['duplicates'] <= $dupBound],
    ['name' => '无非预期进程崩溃', 'value' => count($crashes), 'limit' => '0（consumer-kill 的 SIGKILL 除外）', 'pass' => [] === $crashes],
    ['name' => '故障后均恢复', 'value' => $unrecovered, 'limit' => '0 个未恢复', 'pass' => 0 === $unrecovered],
    ['name' => '恢复时间最大值', 'value' => round($maxRecovery, 2) . 's', 'limit' => "≤ {$limits['max_recovery']}s", 'pass' => $maxRecovery <= $limits['max_recovery']],
    ['name' => '故障窗口外的消费停顿', 'value' => round($maxStall, 2) . 's' . (null === $stallAt ? '' : " @{$stallAt}s"), 'limit' => "≤ {$limits['max_stall']}s", 'pass' => $maxStall <= $limits['max_stall']],
    ['name' => 'worker RSS 峰值', 'value' => round($workerRssMax / 1024, 1) . ' MiB', 'limit' => "≤ {$limits['max_worker_rss_mib']} MiB", 'pass' => $workerRssMax / 1024 <= $limits['max_worker_rss_mib']],
    ['name' => 'worker RSS 增长斜率', 'value' => null === $slopeResult ? 'N/A' : "{$slopeResult['rss_mib_per_hour']} MiB/h（{$slopeResult['window_seconds']}s 窗口）", 'limit' => "≤ {$limits['max_rss_slope']} MiB/h，窗口 ≥ {$limits['min_slope_window']}s", 'pass' => $slopeMeasurable ? $slopeResult['rss_mib_per_hour'] <= $limits['max_rss_slope'] : null],
    ['name' => 'worker fd 增长斜率', 'value' => null === $slopeResult ? 'N/A' : "{$slopeResult['fds_per_hour']}/h", 'limit' => "≤ {$limits['max_fd_slope']}/h，窗口 ≥ {$limits['min_slope_window']}s", 'pass' => $slopeMeasurable ? $slopeResult['fds_per_hour'] <= $limits['max_fd_slope'] : null],
];
$failed = array_filter($criteria, static fn ($c) => false === $c['pass']);
$verdict = [] === $failed ? (in_array(null, array_column($criteria, 'pass'), true) ? 'PASS（部分判据需更长运行）' : 'PASS') : 'FAIL';

$errorKinds = [];
foreach ($events as $event) {
    if (str_ends_with($event['type'] ?? '', '_error')) {
        $key = $event['type'] . ':' . ($event['kind'] ?? $event['class'] ?? 'unknown');
        $errorKinds[$key] = ($errorKinds[$key] ?? 0) + 1;
    }
}
arsort($errorKinds);

$report = [
    'verdict' => $verdict,
    'run' => $params['run'],
    'params' => array_intersect_key($params, array_flip(['duration', 'runtime', 'rate', 'bytes', 'consumers', 'partitions', 'commit_every', 'faults', 'fault_interval', 'seed'])),
    'limits' => $limits,
    'criteria' => $criteria,
    'ledger' => $ledger,
    'recoveries' => $recoveries,
    'process_restarts' => $restarts,
    'crashes' => $crashes,
    'errors' => $errorKinds,
    'resources' => ['worker_rss_max_mib' => round($workerRssMax / 1024, 1), 'slope' => $slopeResult, 'worker_processes' => count($segments)],
];
file_put_contents("{$dir}/report.json", json_encode($report, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE) . "\n");

$md = ["# Soak 报告：{$verdict}", '', "run `{$params['run']}`，" . json_encode($report['params'], JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES), '', '| 判据 | 实测 | 阈值 | 结果 |', '| --- | --- | --- | --- |'];
foreach ($criteria as $c) {
    $md[] = "| {$c['name']} | {$c['value']} | {$c['limit']} | " . (null === $c['pass'] ? 'N/A' : ($c['pass'] ? '✅' : '❌')) . ' |';
}
$md[] = '';
$md[] = "账本：acked {$ledger['acked']}，消费记录 {$ledger['consumed_records']}（去重 {$ledger['consumed_unique']}），重复 {$ledger['duplicates']}，未确认但已写入 {$ledger['unacked_consumed']}。";
$md[] = '';
$md[] = '| 故障 | 注入时刻 | 持续 | 生产恢复 | 消费恢复 |';
$md[] = '| --- | ---: | ---: | ---: | ---: |';
foreach ($recoveries as $r) {
    $cell = static fn ($v) => null === $v ? '未恢复' : ('N/A' === $v ? 'N/A（负载已结束）' : "{$v}s");
    $md[] = "| {$r['fault']} | {$r['at']}s | {$r['duration']}s | " . $cell($r['produce_recovery']) . ' | ' . $cell($r['consume_recovery']) . ' |';
}
$md[] = '';
$md[] = '进程重启：' . json_encode($restarts) . '；错误分布：' . json_encode($errorKinds, JSON_UNESCAPED_SLASHES);
file_put_contents("{$dir}/report.md", implode("\n", $md) . "\n");
echo implode("\n", $md), "\n";
exit('FAIL' === $verdict ? 1 : 0);
