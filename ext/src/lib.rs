mod client;
mod ini_config;
mod ipc;
mod pool;
mod protocol;
mod spawn;
mod subscription;
mod worker_entry;
mod worker_health;

use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

pub use client::Client;

/// 默认 Unix socket 路径。所有 `socket` 参数缺省时使用此值。
/// 可通过环境变量 `HI_KAFKA_SOCKET` 覆盖（仅在扩展首次解析时读取）。
pub(crate) const DEFAULT_SOCKET: &str = "/tmp/hi-kafka.sock";

pub(crate) fn resolve_socket(socket: Option<&str>) -> String {
    if let Some(s) = socket.filter(|s| !s.is_empty()) {
        return s.to_string();
    }
    std::env::var("HI_KAFKA_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET.to_string())
}

/// 扩展版本
#[php_function]
pub fn hi_kafka_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// 显式启动 worker（如果还没在跑）。命中缓存时零开销直接返回。
#[php_function]
pub fn hi_kafka_ensure_worker(socket: Option<String>) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    worker_health::ensure(&socket)
        .map_err(|e| PhpException::default(format!("ensure_worker: {e}")))?;
    Ok(())
}

/// 注册或覆盖一个 Kafka 集群。
///
/// `$config` 必须包含 `bootstrap.servers`，其它键值原样透传给 librdkafka。
/// 同名集群配置会被覆盖（注意：已建立的连接不会立即重建）。
///
/// 业务侧的标准模式：在请求开始前注册所需集群，之后 produce/subscribe
/// 用 cluster 名引用。worker 端按集群独立维护客户端，多集群天然隔离。
#[php_function]
pub fn hi_kafka_register_cluster(
    cluster: &str,
    config: std::collections::HashMap<String, String>,
    socket: Option<String>,
    timeout_ms: Option<i64>,
) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(5000).max(1) as u64);
    let cfg_vec: Vec<(String, String)> = config.into_iter().collect();
    ipc::register_cluster(&socket, cluster, cfg_vec, timeout)
        .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(())
}

/// Fire-and-forget 全局函数 API。
///
/// 高级选项（全部可选，缺省 None 表示由 librdkafka 自动决定）：
/// - `$headers`：Kafka 消息头（关联数组）
/// - `$partition`：明确写入分区编号；`null`/缺省 = 走 partitioner（key hash）
/// - `$timestampMs`：消息时间戳（毫秒）；`null`/缺省 = librdkafka 当前时间
#[php_function]
pub fn hi_kafka_produce_fnf(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
    socket: Option<String>,
) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    let opts = build_options(headers, partition, timestamp_ms);
    ipc::produce_fnf(&socket, cluster, topic, key, value, opts)
        .map_err(|e| PhpException::default(e.to_string()))
}

/// 同步带 ack 全局函数 API。参数同 [`hi_kafka_produce_fnf`] + `timeout_ms`。
#[php_function]
pub fn hi_kafka_produce_sync(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
    timeout_ms: Option<i64>,
    socket: Option<String>,
) -> PhpResult<Zval> {
    let socket = resolve_socket(socket.as_deref());
    let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(5000).max(1) as u64);
    let opts = build_options(headers, partition, timestamp_ms);
    let resp = ipc::produce_sync(&socket, cluster, topic, key, value, opts, timeout)
        .map_err(|e| PhpException::default(e.to_string()))?;
    client::resp_to_zval(resp)
}

pub(crate) fn build_options(
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
) -> ipc::ProduceOptions {
    ipc::ProduceOptions {
        headers: headers
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, bytes::Bytes::from(v.into_bytes())))
            .collect(),
        partition: partition
            .map(|p| p.clamp(i32::MIN as i64, i32::MAX as i64) as i32)
            .unwrap_or(-1),
        timestamp_ms: timestamp_ms.unwrap_or(-1),
    }
}

// ============================================================================
// Consumer 全局函数 API
// ============================================================================

/// 创建订阅。返回 virtual subscription_id（int），后续 poll/commit/unsubscribe 都用它。
///
/// **自愈语义**：返回的 ID 是扩展层维护的 virtual ID。worker 崩溃重启后，
/// poll/commit 时会透明重订阅（real_id 在底层换新，virtual_id 不变）。
/// 业务侧 `$sub` 句柄全程稳定，未提交消息按 Kafka at-least-once 语义重派发。
///
/// `$topics` 字符串数组；`$config` 关联数组（可选，consumer 级别配置如
/// `auto.offset.reset`、`session.timeout.ms`）。
#[php_function]
pub fn hi_kafka_subscribe(
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Option<std::collections::HashMap<String, String>>,
    socket: Option<String>,
    timeout_ms: Option<i64>,
) -> PhpResult<i64> {
    let socket = resolve_socket(socket.as_deref());
    let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(5000).max(1) as u64);
    let cfg_vec: Vec<(String, String)> = config.unwrap_or_default().into_iter().collect();
    let id = subscription::subscribe(&socket, cluster, group_id, topics, cfg_vec, timeout)
        .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(id as i64)
}

/// 拉一批消息。命中底层 subscription not found 时透明重订阅 + 重试。
#[php_function]
pub fn hi_kafka_poll(
    subscription_id: i64,
    max_messages: i64,
    timeout_ms: i64,
) -> PhpResult<Zval> {
    let messages = subscription::poll(
        subscription_id as u64,
        max_messages.max(1) as u32,
        timeout_ms.max(0) as u32,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    messages_to_zval(messages)
}

/// 同步提交 offset。命中底层 subscription not found 时透明重订阅 + 重试。
#[php_function]
pub fn hi_kafka_commit(subscription_id: i64, timeout_ms: Option<i64>) -> PhpResult<()> {
    let timeout = std::time::Duration::from_millis(timeout_ms.unwrap_or(5000).max(1) as u64);
    subscription::commit(subscription_id as u64, timeout)
        .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(())
}

/// 退订。幂等，对已不存在的 virtual_id 直接返回。
#[php_function]
pub fn hi_kafka_unsubscribe(subscription_id: i64) -> PhpResult<()> {
    subscription::unsubscribe(subscription_id as u64)
        .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(())
}

/// 扩展端 consumer 自愈重订阅统计。
#[php_function]
pub fn hi_kafka_resubscribe_stats() -> PhpResult<Zval> {
    let s = subscription::resubscribe_stats();
    let mut ht = ZendHashTable::new();
    ht.insert("attempts", s.attempts as i64)
        .map_err(|e| PhpException::default(format!("attempts: {e}")))?;
    ht.insert("successes", s.successes as i64)
        .map_err(|e| PhpException::default(format!("successes: {e}")))?;
    ht.insert("failures", s.failures as i64)
        .map_err(|e| PhpException::default(format!("failures: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 把三个平行数组组合成 `Vec<(String, i32, i64)>`。长度必须一致。
pub(crate) fn build_offset_targets(
    topics: Vec<String>,
    partitions: Vec<i64>,
    offsets: Vec<i64>,
) -> Result<Vec<(String, i32, i64)>, String> {
    if topics.len() != partitions.len() || topics.len() != offsets.len() {
        return Err(format!(
            "topics({}), partitions({}), offsets({}) 长度必须一致",
            topics.len(),
            partitions.len(),
            offsets.len()
        ));
    }
    Ok(topics
        .into_iter()
        .zip(partitions.into_iter())
        .zip(offsets.into_iter())
        .map(|((t, p), o)| (t, p as i32, o))
        .collect())
}

/// 把两个平行数组组合成 binary headers `Vec<(String, Bytes)>`。
/// 头部 name UTF-8（Kafka 协议要求），value 任意字节。两数组长度必须一致。
pub(crate) fn build_binary_headers(
    names: Vec<String>,
    values: Vec<ext_php_rs::binary::Binary<u8>>,
) -> Result<Vec<(String, bytes::Bytes)>, String> {
    if names.len() != values.len() {
        return Err(format!(
            "header names({}) 和 values({}) 长度必须一致",
            names.len(),
            values.len()
        ));
    }
    Ok(names
        .into_iter()
        .zip(values.into_iter())
        .map(|(n, v)| (n, bytes::Bytes::from(Vec::<u8>::from(v))))
        .collect())
}

/// 把两个平行数组组合成 `Vec<(String, i32)>`。空数组合法（应用到全部当前 assignment）。
pub(crate) fn build_partition_specs(
    topics: Vec<String>,
    partitions: Vec<i64>,
) -> Result<Vec<(String, i32)>, String> {
    if topics.len() != partitions.len() {
        return Err(format!(
            "topics({}) 和 partitions({}) 长度必须一致",
            topics.len(),
            partitions.len(),
        ));
    }
    Ok(topics
        .into_iter()
        .zip(partitions.into_iter())
        .map(|(t, p)| (t, p as i32))
        .collect())
}

pub(crate) fn rebalance_events_to_zval(
    events: Vec<hi_kafka_proto::RebalanceEvent>,
) -> PhpResult<Zval> {
    let mut top = ZendHashTable::new();
    for e in events {
        let mut inner = ZendHashTable::new();
        match e {
            hi_kafka_proto::RebalanceEvent::Assign { partitions } => {
                inner
                    .insert("type", "assign")
                    .map_err(|e| PhpException::default(format!("type: {e}")))?;
                inner
                    .insert("partitions", partitions_to_zval(&partitions)?)
                    .map_err(|e| PhpException::default(format!("partitions: {e}")))?;
            }
            hi_kafka_proto::RebalanceEvent::Revoke { partitions } => {
                inner
                    .insert("type", "revoke")
                    .map_err(|e| PhpException::default(format!("type: {e}")))?;
                inner
                    .insert("partitions", partitions_to_zval(&partitions)?)
                    .map_err(|e| PhpException::default(format!("partitions: {e}")))?;
            }
            hi_kafka_proto::RebalanceEvent::Error { message } => {
                inner
                    .insert("type", "error")
                    .map_err(|e| PhpException::default(format!("type: {e}")))?;
                inner
                    .insert("message", message.as_str())
                    .map_err(|e| PhpException::default(format!("message: {e}")))?;
            }
        }
        let inner_zval = inner
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("inner: {e}")))?;
        top.push(inner_zval)
            .map_err(|e| PhpException::default(format!("push: {e}")))?;
    }
    top.into_zval(false)
        .map_err(|e| PhpException::default(format!("top: {e}")))
}

fn partitions_to_zval(parts: &[(String, i32)]) -> PhpResult<Zval> {
    let mut arr = ZendHashTable::new();
    for (topic, partition) in parts {
        let mut entry = ZendHashTable::new();
        entry
            .insert("topic", topic.as_str())
            .map_err(|e| PhpException::default(format!("topic: {e}")))?;
        entry
            .insert("partition", *partition as i64)
            .map_err(|e| PhpException::default(format!("partition: {e}")))?;
        let z = entry
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("entry: {e}")))?;
        arr.push(z)
            .map_err(|e| PhpException::default(format!("push: {e}")))?;
    }
    arr.into_zval(false)
        .map_err(|e| PhpException::default(format!("arr: {e}")))
}

pub(crate) fn messages_to_zval(messages: Vec<hi_kafka_proto::ConsumerMessage>) -> PhpResult<Zval> {
    let mut top = ZendHashTable::new();
    for m in messages {
        let mut inner = ZendHashTable::new();
        inner
            .insert("topic", m.topic.as_str())
            .map_err(|e| PhpException::default(format!("topic: {e}")))?;
        inner
            .insert("partition", m.partition as i64)
            .map_err(|e| PhpException::default(format!("partition: {e}")))?;
        inner
            .insert("offset", m.offset)
            .map_err(|e| PhpException::default(format!("offset: {e}")))?;
        inner
            .insert("timestamp_ms", m.timestamp_ms)
            .map_err(|e| PhpException::default(format!("timestamp_ms: {e}")))?;
        let key_str = bytes_to_php_string(&m.key);
        inner
            .insert("key", key_str.as_str())
            .map_err(|e| PhpException::default(format!("key: {e}")))?;
        let val_str = bytes_to_php_string(&m.value);
        inner
            .insert("value", val_str.as_str())
            .map_err(|e| PhpException::default(format!("value: {e}")))?;

        // Headers 关联数组：name → binary value
        let mut headers_ht = ZendHashTable::new();
        for (name, value) in &m.headers {
            let v_str = bytes_to_php_string(value);
            headers_ht
                .insert(name.as_str(), v_str.as_str())
                .map_err(|e| PhpException::default(format!("header {name}: {e}")))?;
        }
        let headers_zval = headers_ht
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("headers: {e}")))?;
        inner
            .insert("headers", headers_zval)
            .map_err(|e| PhpException::default(format!("insert headers: {e}")))?;

        let inner_zval = inner
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("inner: {e}")))?;
        top.push(inner_zval)
            .map_err(|e| PhpException::default(format!("push: {e}")))?;
    }
    top.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 返回扩展进程内所有 socket 路径的连接池统计。
///
/// 数组形如：
///
/// ```text
/// [
///   "/var/run/hi-kafka/worker.sock" => [
///     "max_idle" => 16,
///     "idle"     => 3,
///     "acquires" => 100,
///     "hits"     => 97,
///     "misses"   => 3,
///     "closed"   => 0,
///     "poisoned" => 0,
///   ],
/// ]
/// ```
#[php_function]
pub fn hi_kafka_pool_stats() -> PhpResult<Zval> {
    let mut top = ZendHashTable::new();
    for (path, stats, idle, max_idle) in pool::all_stats() {
        let mut inner = ZendHashTable::new();
        inner
            .insert("max_idle", max_idle as i64)
            .map_err(|e| PhpException::default(format!("max_idle: {e}")))?;
        inner
            .insert("idle", idle as i64)
            .map_err(|e| PhpException::default(format!("idle: {e}")))?;
        inner
            .insert("acquires", stats.acquires_total as i64)
            .map_err(|e| PhpException::default(format!("acquires: {e}")))?;
        inner
            .insert("hits", stats.hits_total as i64)
            .map_err(|e| PhpException::default(format!("hits: {e}")))?;
        inner
            .insert("misses", stats.misses_total as i64)
            .map_err(|e| PhpException::default(format!("misses: {e}")))?;
        inner
            .insert("closed", stats.closed_total as i64)
            .map_err(|e| PhpException::default(format!("closed: {e}")))?;
        inner
            .insert("poisoned", stats.poisoned_total as i64)
            .map_err(|e| PhpException::default(format!("poisoned: {e}")))?;
        let inner_zval = inner
            .into_zval(false)
            .map_err(|e| PhpException::default(format!("inner: {e}")))?;
        top.insert(path.to_string_lossy().as_ref(), inner_zval)
            .map_err(|e| PhpException::default(format!("top: {e}")))?;
    }
    top.into_zval(false)
        .map_err(|e| PhpException::default(format!("top into_zval: {e}")))
}

/// 扩展端自动重试统计。worker 进程崩了之后业务调用的恢复次数。
///
/// 返回 `['attempts' => int, 'successes' => int, 'failures' => int]`：
/// - `attempts`：触发重试的次数（≈ worker 死亡且被 IPC 命中的次数）
/// - `successes`：重试成功的次数（业务无感）
/// - `failures`：重试也失败的次数（业务层看到错误）
#[php_function]
pub fn hi_kafka_retry_stats() -> PhpResult<Zval> {
    let s = ipc::retry_stats();
    let mut ht = ZendHashTable::new();
    ht.insert("attempts", s.attempts as i64)
        .map_err(|e| PhpException::default(format!("attempts: {e}")))?;
    ht.insert("successes", s.successes as i64)
        .map_err(|e| PhpException::default(format!("successes: {e}")))?;
    ht.insert("failures", s.failures as i64)
        .map_err(|e| PhpException::default(format!("failures: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 检测当前 PHP 加载了哪些已识别的协程运行时。
/// 返回字符串数组，例如 `["blocking"]` 或 `["blocking", "swoole"]`。
///
/// 检测策略：在 `EG(function_table)` 里查标志性函数是否注册。
/// - `swoole_version` → Swoole 已加载
/// - `Swow\Coroutine::*` 这类类方法不在函数表里，本期改查 `swow\\version`
///
/// 注：本期仍统一走阻塞 IO。返回结果**仅供观测**，不会改变扩展行为。
/// 真正的协程感知 driver 在 Phase 3。
#[php_function]
pub fn hi_kafka_runtime() -> Vec<String> {
    let mut runtimes = vec!["blocking".to_string()];
    if function_exists("swoole_version") {
        runtimes.push("swoole".to_string());
    }
    // Swow 的标志性函数（裸函数）
    if function_exists("swow\\version") {
        runtimes.push("swow".to_string());
    }
    runtimes
}

fn function_exists(name: &str) -> bool {
    use std::ffi::CString;
    let Ok(c) = CString::new(name) else {
        return false;
    };
    unsafe {
        // executor_globals.function_table 本身就是 *mut HashTable，
        // 直接读字段即可，不要再 &raw 一层
        let ft: *const ext_php_rs::ffi::HashTable =
            ext_php_rs::ffi::executor_globals.function_table;
        if ft.is_null() {
            return false;
        }
        // `_lc` 做小写匹配，符合 PHP 函数名大小写不敏感的语义
        let ptr = ext_php_rs::ffi::zend_hash_str_find_ptr_lc(ft, c.as_ptr(), name.len());
        !ptr.is_null()
    }
}

// === 协议编解码原语（给 PHP 层 Swoole/Swow driver 用） ==========================

/// 全进程单调自增 cid。
#[php_function]
pub fn hi_kafka_next_cid() -> i64 {
    protocol::next_cid() as i64
}

/// 协议帧头长度（常量 13）。便于 PHP driver 精确分两段 recv。
#[php_function]
pub fn hi_kafka_header_len() -> i64 {
    protocol::header_len() as i64
}

/// 编一帧 PRODUCE_FNF（fire-and-forget），返回完整帧字节串（PHP 字符串）。
#[php_function]
pub fn hi_kafka_encode_fnf_frame(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
) -> PhpResult<String> {
    let opts = build_options(headers, partition, timestamp_ms);
    let bytes = protocol::build_fnf_frame(
        cluster,
        topic,
        key.as_bytes(),
        value.as_bytes(),
        opts.headers,
        opts.partition,
        opts.timestamp_ms,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(bytes_to_php_string(&bytes))
}

/// 编一帧 PRODUCE_REQ，返回 `['cid' => int, 'frame' => binary]`。
#[php_function]
pub fn hi_kafka_encode_req_frame(
    cluster: &str,
    topic: &str,
    key: &str,
    value: &str,
    headers: Option<std::collections::HashMap<String, String>>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
) -> PhpResult<Zval> {
    let opts = build_options(headers, partition, timestamp_ms);
    let (cid, bytes) = protocol::build_req_frame(
        cluster,
        topic,
        key.as_bytes(),
        value.as_bytes(),
        opts.headers,
        opts.partition,
        opts.timestamp_ms,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    let mut ht = ZendHashTable::new();
    ht.insert("cid", cid as i64)
        .map_err(|e| PhpException::default(format!("cid: {e}")))?;
    ht.insert("frame", bytes_to_php_string(&bytes).as_str())
        .map_err(|e| PhpException::default(format!("frame: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 仅解析 13B 帧头，返回 `['kind' => int, 'cid' => int, 'payload_len' => int]`。
#[php_function]
pub fn hi_kafka_parse_header(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let h = protocol::parse_header_only(&bytes)
        .map_err(|e| PhpException::default(e.to_string()))?;
    let mut ht = ZendHashTable::new();
    ht.insert("kind", h.kind_byte as i64)
        .map_err(|e| PhpException::default(format!("kind: {e}")))?;
    ht.insert("cid", h.cid as i64)
        .map_err(|e| PhpException::default(format!("cid: {e}")))?;
    ht.insert("payload_len", h.payload_len as i64)
        .map_err(|e| PhpException::default(format!("payload_len: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 解析完整 PRODUCE_RESP 帧（含 header + payload）。
#[php_function]
pub fn hi_kafka_decode_resp_frame(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let parsed = protocol::parse_resp_frame(&bytes)
        .map_err(|e| PhpException::default(e.to_string()))?;
    let mut ht = ZendHashTable::new();
    match parsed {
        protocol::ParsedFrame::Resp { cid, resp } => {
            ht.insert("cid", cid as i64)
                .map_err(|e| PhpException::default(format!("cid: {e}")))?;
            match resp {
                hi_kafka_proto::ProduceResp::Ok(ack) => {
                    ht.insert("ok", true)
                        .map_err(|e| PhpException::default(format!("ok: {e}")))?;
                    ht.insert("partition", ack.partition as i64)
                        .map_err(|e| PhpException::default(format!("partition: {e}")))?;
                    ht.insert("offset", ack.offset)
                        .map_err(|e| PhpException::default(format!("offset: {e}")))?;
                }
                hi_kafka_proto::ProduceResp::Err(err) => {
                    ht.insert("ok", false)
                        .map_err(|e| PhpException::default(format!("ok: {e}")))?;
                    ht.insert("code", err.code as i64)
                        .map_err(|e| PhpException::default(format!("code: {e}")))?;
                    ht.insert("message", err.message.as_str())
                        .map_err(|e| PhpException::default(format!("message: {e}")))?;
                    ht.insert("retryable", err.retryable)
                        .map_err(|e| PhpException::default(format!("retryable: {e}")))?;
                }
            }
        }
        protocol::ParsedFrame::Other { kind, cid, .. } => {
            return Err(PhpException::default(format!(
                "unexpected frame kind {kind:?} cid={cid}"
            )));
        }
    }
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 字节流转 PHP 字符串。PHP 字符串本身二进制安全；Rust String 仅为内存表示。
fn bytes_to_php_string(bytes: &[u8]) -> String {
    unsafe { String::from_utf8_unchecked(bytes.to_vec()) }
}

// === Consumer 协议原语 =====================================================

/// 编一帧 SUBSCRIBE_REQ。返回 `['cid' => int, 'frame' => binary]`。
#[php_function]
pub fn hi_kafka_encode_subscribe_frame(
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Option<std::collections::HashMap<String, String>>,
) -> PhpResult<Zval> {
    let cfg_vec: Vec<(String, String)> = config.unwrap_or_default().into_iter().collect();
    let (cid, bytes) =
        protocol::build_subscribe_frame(cluster, group_id, topics, cfg_vec)
            .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, &bytes)
}

/// 编一帧 POLL_REQ。
#[php_function]
pub fn hi_kafka_encode_poll_frame(
    subscription_id: i64,
    max_messages: i64,
    timeout_ms: i64,
) -> PhpResult<Zval> {
    let (cid, bytes) = protocol::build_poll_frame(
        subscription_id as u64,
        max_messages.max(1) as u32,
        timeout_ms.max(0) as u32,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, &bytes)
}

/// 编一帧 COMMIT_REQ。
#[php_function]
pub fn hi_kafka_encode_commit_frame(subscription_id: i64) -> PhpResult<Zval> {
    let (cid, bytes) = protocol::build_commit_frame(subscription_id as u64)
        .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, &bytes)
}

/// 编一帧 UNSUBSCRIBE（无响应，cid=0）。
#[php_function]
pub fn hi_kafka_encode_unsubscribe_frame(subscription_id: i64) -> PhpResult<String> {
    let bytes = protocol::build_unsubscribe_frame(subscription_id as u64)
        .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(bytes_to_php_string(&bytes))
}

/// 编一帧 REGISTER_CLUSTER_REQ。
#[php_function]
pub fn hi_kafka_encode_register_cluster_frame(
    cluster: &str,
    config: std::collections::HashMap<String, String>,
) -> PhpResult<Zval> {
    let cfg_vec: Vec<(String, String)> = config.into_iter().collect();
    let (cid, bytes) = protocol::build_register_cluster_frame(cluster, cfg_vec)
        .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, &bytes)
}

/// 解析任意 consumer 响应帧（SUBSCRIBE_RESP / POLL_RESP / COMMIT_RESP），按 kind 分发。
///
/// 返回结构：
/// - SubscribeResp Ok：`['kind' => 'subscribe', 'cid' => int, 'ok' => true, 'subscription_id' => int]`
/// - SubscribeResp Err：`['kind' => 'subscribe', 'cid' => int, 'ok' => false, 'message' => str]`
/// - PollResp Ok：`['kind' => 'poll', 'cid' => int, 'ok' => true, 'messages' => array]`
/// - PollResp Err：`['kind' => 'poll', 'cid' => int, 'ok' => false, 'message' => str]`
/// - CommitResp Ok：`['kind' => 'commit', 'cid' => int, 'ok' => true]`
/// - CommitResp Err：`['kind' => 'commit', 'cid' => int, 'ok' => false, 'message' => str]`
#[php_function]
pub fn hi_kafka_decode_consumer_resp(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let parsed = protocol::parse_consumer_resp_frame(&bytes)
        .map_err(|e| PhpException::default(e.to_string()))?;

    let mut ht = ZendHashTable::new();
    match parsed {
        protocol::ConsumerResp::SubscribeOk {
            cid,
            subscription_id,
        } => {
            put(&mut ht, "kind", "subscribe")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
            put(&mut ht, "subscription_id", subscription_id as i64)?;
        }
        protocol::ConsumerResp::SubscribeErr { cid, message } => {
            put(&mut ht, "kind", "subscribe")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::PollOk { cid, messages } => {
            put(&mut ht, "kind", "poll")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
            let msgs_zval = messages_to_zval(messages)?;
            ht.insert("messages", msgs_zval)
                .map_err(|e| PhpException::default(format!("messages: {e}")))?;
        }
        protocol::ConsumerResp::PollErr { cid, message } => {
            put(&mut ht, "kind", "poll")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::CommitOk { cid } => {
            put(&mut ht, "kind", "commit")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::CommitErr { cid, message } => {
            put(&mut ht, "kind", "commit")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
        protocol::ConsumerResp::RegisterClusterOk { cid } => {
            put(&mut ht, "kind", "register_cluster")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", true)?;
        }
        protocol::ConsumerResp::RegisterClusterErr { cid, message } => {
            put(&mut ht, "kind", "register_cluster")?;
            put(&mut ht, "cid", cid as i64)?;
            put(&mut ht, "ok", false)?;
            put(&mut ht, "message", message.as_str())?;
        }
    }
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

fn cid_frame_zval(cid: u64, bytes: &[u8]) -> PhpResult<Zval> {
    let mut ht = ZendHashTable::new();
    ht.insert("cid", cid as i64)
        .map_err(|e| PhpException::default(format!("cid: {e}")))?;
    ht.insert("frame", bytes_to_php_string(bytes).as_str())
        .map_err(|e| PhpException::default(format!("frame: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

fn put<V: IntoZval>(ht: &mut ZendHashTable, key: &str, value: V) -> PhpResult<()> {
    ht.insert(key, value)
        .map_err(|e| PhpException::default(format!("{key}: {e}")))
}

/// MINIT 钩子：注册 hi_kafka.* 三项 ini 给运维侧用。
extern "C" fn module_startup(_type: i32, module_number: i32) -> i32 {
    ini_config::register(module_number);
    0
}

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module.startup_function(module_startup)
}
