<?php

declare(strict_types=1);

$options = getopt('', ['baseline:', 'candidate:', 'max-regression::']);
$baselinePath = $options['baseline'] ?? null;
$candidatePath = $options['candidate'] ?? null;
$maxRegression = (float) ($options['max-regression'] ?? 5.0);
if (! is_string($baselinePath) || ! is_string($candidatePath)) {
    fwrite(STDERR, "usage: compare.php --baseline=FILE --candidate=FILE [--max-regression=5]\n");
    exit(2);
}

/** @return array<string,array<string,mixed>> */
$load = static function (string $path): array {
    $rows = [];
    foreach (file($path, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [] as $line) {
        $row = json_decode($line, true, flags: JSON_THROW_ON_ERROR);
        $configHash = substr(hash('sha256', json_encode($row['cluster_config'], JSON_THROW_ON_ERROR)), 0, 8);
        $key = implode('|', [
            $row['runtime'],
            $row['scenario'],
            $row['message_bytes'],
            $row['concurrency'],
            $row['messages'],
            $row['warmup_messages'],
            $configHash,
        ]);
        $rows[$key] = $row;
    }
    return $rows;
};

$baseline = $load($baselinePath);
$candidate = $load($candidatePath);
$failed = false;
printf("%-70s %12s %12s %10s %10s\n", 'case', 'base msg/s', 'new msg/s', 'throughput', 'p99');
foreach ($baseline as $key => $base) {
    if (! isset($candidate[$key])) {
        printf("%-70s MISSING\n", $key);
        $failed = true;
        continue;
    }
    $new = $candidate[$key];
    $baseThroughput = (float) $base['result']['messages_per_s'];
    $newThroughput = (float) $new['result']['messages_per_s'];
    $baseP99 = (float) ($base['result']['operation_latency']['p99_us'] ?? 0);
    $newP99 = (float) ($new['result']['operation_latency']['p99_us'] ?? 0);
    $throughputChange = $baseThroughput > 0 ? (($newThroughput / $baseThroughput) - 1) * 100 : 0;
    $p99Change = $baseP99 > 0 ? (($newP99 / $baseP99) - 1) * 100 : 0;
    $caseFailed = $throughputChange < -$maxRegression || $p99Change > $maxRegression;
    $failed = $failed || $caseFailed;
    printf(
        "%-70s %12.2f %12.2f %+9.2f%% %+9.2f%%%s\n",
        $key,
        $baseThroughput,
        $newThroughput,
        $throughputChange,
        $p99Change,
        $caseFailed ? ' FAIL' : '',
    );
}
exit($failed ? 1 : 0);
