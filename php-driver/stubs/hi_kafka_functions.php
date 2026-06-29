<?php

declare(strict_types=1);

/*
 * 全局函数桩 —— 没法 PSR-4 autoload（没类），所以经 composer `autoload.files`
 * 每次请求都 require 一遍；每个声明都用 `function_exists()` guard，扩展存在时
 * 跳过定义，不会撞 "already declared"。
 *
 * 用途：
 * - 扩展缺失的开发/CI/IDE 环境下，让上层调用 `hi_kafka_*()` 至少词法可解析
 * - SwooleClient / SwowClient 内部用的低级 hi_kafka_encode_* / hi_kafka_decode_* /
 *   hi_kafka_next_cid 等 @internal 函数，被静态分析器扫到时有签名
 *
 * 函数体全部 no-op + 返回类型默认值——扩展缺失时调用任何函数都不会真生效。
 */

namespace {

    if (! \function_exists('hi_kafka_version')) {
        /** 扩展版本号 */
        function hi_kafka_version(): string
        {
            return '';
        }
    }

    if (! \function_exists('hi_kafka_ensure_worker')) {
        /** 显式启动 worker（如果还没在跑）。命中缓存时零开销。 */
        function hi_kafka_ensure_worker(?string $socket = null): void
        {
        }
    }

    if (! \function_exists('hi_kafka_register_cluster')) {
        /**
         * 注册或覆盖 Kafka 集群配置。全局函数版（不需要 Client 实例）。
         *
         * @param array<string,string> $config
         */
        function hi_kafka_register_cluster(
            string $cluster,
            array $config,
            ?string $socket = null,
            ?int $timeoutMs = null
        ): void {
        }
    }

    if (! \function_exists('hi_kafka_produce_fnf')) {
        /**
         * Fire-and-forget 生产。
         *
         * @param array<string,string>|null $headers
         */
        function hi_kafka_produce_fnf(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            ?array $headers = null,
            ?int $partition = null,
            ?int $timestampMs = null,
            ?string $socket = null
        ): void {
        }
    }

    if (! \function_exists('hi_kafka_produce_sync')) {
        /**
         * 同步生产。
         *
         * @param array<string,string>|null $headers
         *
         * @return array{ok: bool, partition?: int, offset?: int, code?: int, message?: string, retryable?: bool}
         */
        function hi_kafka_produce_sync(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            ?array $headers = null,
            ?int $partition = null,
            ?int $timestampMs = null,
            ?int $timeoutMs = null,
            ?string $socket = null
        ): array {
            return ['ok' => true];
        }
    }

    if (! \function_exists('hi_kafka_subscribe')) {
        /**
         * @param string[]                  $topics
         * @param array<string,string>|null $config
         */
        function hi_kafka_subscribe(
            string $cluster,
            string $groupId,
            array $topics,
            ?array $config = null,
            ?string $socket = null,
            ?int $timeoutMs = null
        ): int {
            return 0;
        }
    }

    if (! \function_exists('hi_kafka_poll')) {
        /**
         * 拉一批消息。
         *
         * @return list<array{
         *     topic: string,
         *     partition: int,
         *     offset: int,
         *     timestamp_ms: int,
         *     key: string,
         *     value: string,
         *     headers: array<string,string>,
         * }>
         */
        function hi_kafka_poll(int $subscriptionId, int $maxMessages, int $timeoutMs): array
        {
            return [];
        }
    }

    if (! \function_exists('hi_kafka_commit')) {
        function hi_kafka_commit(int $subscriptionId, ?int $timeoutMs = null): void
        {
        }
    }

    if (! \function_exists('hi_kafka_unsubscribe')) {
        function hi_kafka_unsubscribe(int $subscriptionId): void
        {
        }
    }

    if (! \function_exists('hi_kafka_pool_stats')) {
        /**
         * @return array<string, array{
         *     max_idle: int, idle: int, acquires: int, hits: int,
         *     misses: int, closed: int, poisoned: int,
         * }>
         */
        function hi_kafka_pool_stats(): array
        {
            return [];
        }
    }

    if (! \function_exists('hi_kafka_retry_stats')) {
        /** @return array{attempts: int, successes: int, failures: int} */
        function hi_kafka_retry_stats(): array
        {
            return ['attempts' => 0, 'successes' => 0, 'failures' => 0];
        }
    }

    if (! \function_exists('hi_kafka_resubscribe_stats')) {
        /** @return array{attempts: int, successes: int, failures: int} */
        function hi_kafka_resubscribe_stats(): array
        {
            return ['attempts' => 0, 'successes' => 0, 'failures' => 0];
        }
    }

    if (! \function_exists('hi_kafka_runtime')) {
        /** @return list<string> 例如 `["blocking"]` 或 `["blocking", "swoole"]` */
        function hi_kafka_runtime(): array
        {
            return [];
        }
    }

    // ========================================================================
    // 低级协议编解码原语（@internal）—— 给 SwooleClient / SwowClient driver 用
    // ========================================================================

    if (! \function_exists('hi_kafka_next_cid')) {
        /** @internal */
        function hi_kafka_next_cid(): int
        {
            return 0;
        }
    }

    if (! \function_exists('hi_kafka_header_len')) {
        /** @internal 协议帧头长度（常量 13） */
        function hi_kafka_header_len(): int
        {
            return 13;
        }
    }

    if (! \function_exists('hi_kafka_encode_hello_frame')) {
        /** @internal */
        function hi_kafka_encode_hello_frame(): string
        {
            return '';
        }
    }

    if (! \function_exists('hi_kafka_verify_hello_resp')) {
        /** @internal */
        function hi_kafka_verify_hello_resp(string $bytes): void
        {
        }
    }

    if (! \function_exists('hi_kafka_encode_fnf_frame')) {
        /**
         * @internal
         * @param array<string,string>|null $headers
         */
        function hi_kafka_encode_fnf_frame(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            ?array $headers = null,
            ?int $partition = null,
            ?int $timestampMs = null
        ): string {
            return '';
        }
    }

    if (! \function_exists('hi_kafka_encode_req_frame')) {
        /**
         * @internal
         * @param array<string,string>|null $headers
         * @return array{cid: int, frame: string}
         */
        function hi_kafka_encode_req_frame(
            string $cluster,
            string $topic,
            string $key,
            string $value,
            ?array $headers = null,
            ?int $partition = null,
            ?int $timestampMs = null
        ): array {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_parse_header')) {
        /**
         * @internal
         * @return array{kind: int, cid: int, payload_len: int}
         */
        function hi_kafka_parse_header(string $bytes): array
        {
            return ['kind' => 0, 'cid' => 0, 'payload_len' => 0];
        }
    }

    if (! \function_exists('hi_kafka_decode_resp_frame')) {
        /**
         * @internal
         * @return array{cid: int, ok: bool, partition?: int, offset?: int, code?: int, message?: string, retryable?: bool}
         */
        function hi_kafka_decode_resp_frame(string $bytes): array
        {
            return ['cid' => 0, 'ok' => true];
        }
    }

    if (! \function_exists('hi_kafka_encode_subscribe_frame')) {
        /**
         * @internal
         * @param string[] $topics
         * @param array<string,string>|null $config
         * @return array{cid: int, frame: string}
         */
        function hi_kafka_encode_subscribe_frame(
            string $cluster,
            string $groupId,
            array $topics,
            ?array $config = null
        ): array {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_poll_frame')) {
        /**
         * @internal
         * @return array{cid: int, frame: string}
         */
        function hi_kafka_encode_poll_frame(int $subscriptionId, int $maxMessages, int $timeoutMs): array
        {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_commit_frame')) {
        /**
         * @internal
         * @return array{cid: int, frame: string}
         */
        function hi_kafka_encode_commit_frame(int $subscriptionId): array
        {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_unsubscribe_frame')) {
        /** @internal */
        function hi_kafka_encode_unsubscribe_frame(int $subscriptionId): string
        {
            return '';
        }
    }

    if (! \function_exists('hi_kafka_encode_register_cluster_frame')) {
        /**
         * @internal
         * @param array<string,string> $config
         * @return array{cid: int, frame: string}
         */
        function hi_kafka_encode_register_cluster_frame(string $cluster, array $config): array
        {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_decode_consumer_resp')) {
        /**
         * @internal
         * @return array{
         *     kind: string,
         *     cid: int,
         *     ok: bool,
         *     subscription_id?: int,
         *     messages?: array,
         *     events?: array,
         *     message?: string,
         * }
         */
        function hi_kafka_decode_consumer_resp(string $bytes): array
        {
            return ['kind' => '', 'cid' => 0, 'ok' => true];
        }
    }

    if (! \function_exists('hi_kafka_encode_pause_resume_frame')) {
        /**
         * @internal
         * @param string[] $topics
         * @param int[]    $partitions
         * @return array{cid:int, frame:string}
         */
        function hi_kafka_encode_pause_resume_frame(
            int $subscriptionId,
            int $op,
            array $topics,
            array $partitions
        ): array {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_seek_by_offset_frame')) {
        /**
         * @internal
         * @param string[] $topics
         * @param int[]    $partitions
         * @param int[]    $offsets
         * @return array{cid:int, frame:string}
         */
        function hi_kafka_encode_seek_by_offset_frame(
            int $subscriptionId,
            array $topics,
            array $partitions,
            array $offsets
        ): array {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_seek_by_timestamp_frame')) {
        /**
         * @internal
         * @param string[] $topics
         * @param int[]    $partitions
         * @return array{cid:int, frame:string}
         */
        function hi_kafka_encode_seek_by_timestamp_frame(
            int $subscriptionId,
            int $timestampMs,
            array $topics,
            array $partitions
        ): array {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_txn_frame')) {
        /**
         * @internal
         * @return array{cid:int, frame:string}
         */
        function hi_kafka_encode_txn_frame(string $cluster, int $op): array
        {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_send_offsets_frame')) {
        /**
         * @internal
         * @param string[] $topics
         * @param int[]    $partitions
         * @param int[]    $offsets
         * @return array{cid:int, frame:string}
         */
        function hi_kafka_encode_send_offsets_frame(
            string $producerCluster,
            int $subscriptionId,
            string $groupId,
            array $topics,
            array $partitions,
            array $offsets
        ): array {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_set_oauth_token_frame')) {
        /**
         * @internal
         * @param array<string,string>|null $extensions
         * @return array{cid:int, frame:string}
         */
        function hi_kafka_encode_set_oauth_token_frame(
            string $cluster,
            string $token,
            int $lifetimeMs,
            string $principalName,
            ?array $extensions = null
        ): array {
            return ['cid' => 0, 'frame' => ''];
        }
    }

    if (! \function_exists('hi_kafka_encode_poll_rebalance_frame')) {
        /**
         * @internal
         * @return array{cid:int, frame:string}
         */
        function hi_kafka_encode_poll_rebalance_frame(int $subscriptionId, int $maxEvents): array
        {
            return ['cid' => 0, 'frame' => ''];
        }
    }
}
