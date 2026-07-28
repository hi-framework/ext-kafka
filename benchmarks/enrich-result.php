<?php

declare(strict_types=1);

if ($argc !== 3) {
    fwrite(STDERR, "usage: enrich-result.php RESULT_JSON SAMPLE_FILE\n");
    exit(2);
}

$result = json_decode($argv[1], true, flags: JSON_THROW_ON_ERROR);
$samples = @file($argv[2], FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES) ?: [];
$columns = ['php_rss_kib' => [], 'php_cpu_percent' => [], 'worker_rss_kib' => [], 'worker_cpu_percent' => []];
foreach ($samples as $sample) {
    $values = preg_split('/\s+/', trim($sample));
    if (count($values) !== 4) {
        continue;
    }
    foreach (array_keys($columns) as $index => $name) {
        if (is_numeric($values[$index]) && (float) $values[$index] >= 0) {
            $columns[$name][] = (float) $values[$index];
        }
    }
}

$result['process_samples'] = count($samples);
foreach ($columns as $name => $values) {
    $result[$name . '_peak'] = $values === [] ? null : max($values);
    if (str_contains($name, 'cpu')) {
        $result[$name . '_mean'] = $values === [] ? null : round(array_sum($values) / count($values), 2);
    }
}
echo json_encode($result, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES), "\n";
