<?php

declare(strict_types=1);

/**
 * 比对 acked 与 consumed 账本。
 *
 * - missing：broker 已确认（acked）但从未被消费——必须为 0。
 * - duplicates：同一 seq 被消费多于一次——at-least-once 允许，但应有界。
 * - corrupted：crc 校验失败——必须为 0。
 * - unacked_consumed：produceSync 结果未知（超时/失败）但实际写入并被消费——不算错误。
 *
 * 每个 producer 用一个字节串计数（每个 seq 一个字节），千万级消息只需 MB 级内存。
 *
 * php ledger-check.php --dir=RUN_DIR   输出 JSON 并写入 RUN_DIR/ledger.json
 */

require __DIR__ . '/lib.php';

$options = parse_options($argv);
$dir = opt($options, 'dir');

/** @var array<string,string> $acked  producer => 每 seq 一字节（1 = acked） */
$acked = [];
/** @var array<string,string> $seen   producer => 每 seq 一字节（消费次数，封顶 255） */
$seen = [];
$size = static function (string $producer) use ($dir): int {
    $file = "{$dir}/next-seq-{$producer}";
    return is_file($file) ? (int) file_get_contents($file) : 0;
};

$result = [
    'acked' => 0,
    'consumed_records' => 0,
    'consumed_unique' => 0,
    'missing' => 0,
    'missing_samples' => [],
    'duplicates' => 0,
    'max_deliveries' => 0,
    'corrupted' => 0,
    'unacked_consumed' => 0,
    'unknown_records' => 0,
];

foreach (glob("{$dir}/acked-*.log") as $file) {
    $producer = substr(basename($file, '.log'), strlen('acked-'));
    $bits = str_repeat("\0", $size($producer));
    $handle = fopen($file, 'rb');
    while (false !== ($line = fgets($handle))) {
        $seq = (int) strtok($line, ' ');
        if ($seq < strlen($bits) && "\0" === $bits[$seq]) {
            $bits[$seq] = "\1";
            ++$result['acked'];
        }
    }
    fclose($handle);
    $acked[$producer] = $bits;
    $seen[$producer] = str_repeat("\0", strlen($bits));
}

foreach (glob("{$dir}/consumed-*.log") as $file) {
    $handle = fopen($file, 'rb');
    while (false !== ($line = fgets($handle))) {
        if (str_starts_with($line, 'CORRUPT')) {
            ++$result['corrupted'];
            continue;
        }
        $parts = explode(' ', trim($line));
        if (count($parts) < 5) {
            continue; // 进程被杀时可能留下半行
        }
        [$producer, $seq] = [$parts[0], (int) $parts[1]];
        ++$result['consumed_records'];
        if (! isset($seen[$producer]) || $seq >= strlen($seen[$producer])) {
            ++$result['unknown_records'];
            continue;
        }
        $count = ord($seen[$producer][$seq]);
        if ($count < 255) {
            $seen[$producer][$seq] = chr($count + 1);
        }
    }
    fclose($handle);
}

foreach ($seen as $producer => $counts) {
    $bits = $acked[$producer];
    $length = strlen($counts);
    for ($seq = 0; $seq < $length; ++$seq) {
        $count = ord($counts[$seq]);
        if ($count > 0) {
            ++$result['consumed_unique'];
            $result['duplicates'] += $count - 1;
            $result['max_deliveries'] = max($result['max_deliveries'], $count);
            if ("\0" === $bits[$seq]) {
                ++$result['unacked_consumed'];
            }
        } elseif ("\1" === $bits[$seq]) {
            ++$result['missing'];
            if (count($result['missing_samples']) < 20) {
                $result['missing_samples'][] = "{$producer}-{$seq}";
            }
        }
    }
}

$json = json_encode($result, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
file_put_contents("{$dir}/ledger.json", $json . "\n");
echo $json, "\n";
exit(0 === $result['missing'] && 0 === $result['corrupted'] ? 0 : 1);
