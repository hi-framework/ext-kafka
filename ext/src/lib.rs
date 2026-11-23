// 业务 API（produce/consume + 完整 options）天然多参数；保留可读签名
// 而不是塞一个 Options struct 让 PHP 调用方更难用。
#![allow(clippy::too_many_arguments)]
// 数组组合 helpers 用 `.iter()/.into_iter()/.zip()/.collect()` 链式可读性比
// "fold + 显式 IntoIterator" 强，且 PHP 调用层的代码生成器倾向 explicit。
#![allow(clippy::useless_conversion)]
// ext-php-rs 的 ZendHashTable::insert 在错误时只能透传错误链，map_err 到
// PhpException 是标准用法——切到 inspect_err 反而丢类型。
#![allow(
    clippy::manual_map,
    clippy::manual_ok_err,
    clippy::manual_inspect,
    clippy::manual_flatten,
    clippy::redundant_pattern_matching
)]

mod client;
mod client_interface;
mod cluster_replay;
mod consumer_stream;
mod consumer_stream_interface;
mod ini_config;
mod ipc;
mod kafka_exception;
mod lifecycle;
mod pool;
mod protocol;
mod spawn;
mod uds;
mod worker_entry;
mod worker_health;

/// `cargo test --lib` 的可执行文件里没有 PHP 运行时。ext-php-rs 的 C wrapper 一旦被
/// 链进来，就会引用 Zend 的**数据**符号，而数据符号必须在加载时就能解析。这里只为
/// 单测提供占位定义；纯 Rust 单测从不读写它们。函数符号由 build.rs 改为惰性绑定。
#[cfg(test)]
#[allow(non_upper_case_globals)]
mod zend_test_symbols {
    #[no_mangle]
    static mut executor_globals: [u64; 1024] = [0; 1024];
    #[no_mangle]
    static mut zend_ce_traversable: usize = 0;
}

use ext_php_rs::binary::Binary;
use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendClassObject, ZendHashTable, Zval};

pub use client::Client;
pub use consumer_stream::{ConsumerRecord, ConsumerStream};

/// 把 IPC 错误转成 PHP 异常：worker 回的结构化错误（`Error` 帧）→ 带 kind + **真实调用栈**
/// 的 `Hi\Kafka\KafkaException`（见 [`kafka_exception`]）；其余（本地 IO / spawn / 协议
/// 错乱）→ 通用 `PhpException`。
///
/// 分类以公开属性暴露：`$e->kind` / `$e->kind_name` / `$e->retryable` / `$e->native_code`
/// （`$e->getCode()` 亦 = kind）。
pub(crate) fn ipc_err_to_php(e: ipc::IpcError) -> PhpException {
    use hi_kafka_proto::{ErrorKind, OperationOutcome};
    use std::io::ErrorKind as Io;

    // 传输层故障同样以结构化 KafkaException 抛出，业务可以按 retryable 统一处理；
    // 此前它们是普通 Exception，「worker 重启 / 读超时」无法与编程错误区分。
    let (kind, retryable, outcome, native_code, message) = match &e {
        ipc::IpcError::Worker {
            kind,
            retryable,
            outcome,
            native_code,
            message,
        } => (*kind, *retryable, *outcome, *native_code, message.clone()),
        ipc::IpcError::Io(io) if matches!(io.kind(), Io::TimedOut | Io::WouldBlock) => (
            ErrorKind::Timeout,
            true,
            OperationOutcome::Unknown,
            0,
            e.to_string(),
        ),
        // 请求可能已发出：结果未知。
        ipc::IpcError::Io(_) => (
            ErrorKind::WorkerUnavailable,
            true,
            OperationOutcome::Unknown,
            0,
            e.to_string(),
        ),
        // 拿不到连接或 worker 起不来：请求一定没有发出。
        ipc::IpcError::Pool(_) | ipc::IpcError::EnsureWorker(_) => (
            ErrorKind::WorkerUnavailable,
            true,
            OperationOutcome::NotApplied,
            0,
            e.to_string(),
        ),
        _ => return PhpException::default(e.to_string()),
    };
    if let Some(zv) = kafka_exception::build_zval(
        &message,
        kind.as_u16() as i64,
        kind.as_str(),
        retryable,
        outcome.as_str(),
        native_code as i64,
    ) {
        // set_object 会让 throw 走 throw_object(zv)，所以这里的 ce 仅作占位。
        let mut ph = PhpException::new(
            message,
            kind.as_u16() as i32,
            ext_php_rs::zend::ce::exception(),
        );
        ph.set_object(Some(zv));
        return ph;
    }
    PhpException::default(e.to_string())
}

/// 默认 Unix socket 路径。所有 `socket` 参数缺省时使用此值。
/// 可通过环境变量 `HI_KAFKA_SOCKET` 覆盖（仅在扩展首次解析时读取）。
pub(crate) const DEFAULT_SOCKET: &str = "/tmp/hi-kafka-v2.sock";

pub(crate) fn resolve_socket(socket: Option<&str>) -> String {
    if let Some(s) = socket.filter(|s| !s.is_empty()) {
        return s.to_string();
    }
    std::env::var("HI_KAFKA_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET.to_string())
}

pub(crate) fn require_positive_u64(name: &str, value: i64) -> PhpResult<u64> {
    if value <= 0 {
        return Err(PhpException::default(format!(
            "{name} must be a positive integer, got {value}"
        )));
    }
    Ok(value as u64)
}

/// 严格要求正数（≥ 1）且不超 `u32::MAX`；负值 / 0 / 超上限都报错。
///
/// 适用于 `timeout_ms` 这类"传 0 或负数说明业务侧 bug"的语义敏感字段——
/// 之前 `require_u32_at_least("timeout_ms", -1, 1)` 会静默变成 1ms 一路撞超时，
/// 业务侧看到 `TIMEOUT` 却不知道根因是传了负值；现在直接 InvalidArgument。
pub(crate) fn require_positive_u32(name: &str, value: i64) -> PhpResult<u32> {
    if value < 1 {
        return Err(PhpException::default(format!(
            "{name} must be a positive integer (>= 1), got {value}"
        )));
    }
    if value > u32::MAX as i64 {
        return Err(PhpException::default(format!(
            "{name} must be <= {}, got {value}",
            u32::MAX
        )));
    }
    Ok(value as u32)
}

pub(crate) fn require_i32(name: &str, value: i64) -> PhpResult<i32> {
    if value < i32::MIN as i64 || value > i32::MAX as i64 {
        return Err(PhpException::default(format!(
            "{name} must be between {} and {}, got {value}",
            i32::MIN,
            i32::MAX
        )));
    }
    Ok(value as i32)
}

/// 扩展版本
#[php_function]
pub fn hi_kafka_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// 显式启动 worker（如果还没在跑）。
/// L: 业务显式调用时**强制 fresh probe**——invalidate 内部缓存后 ensure 会调
/// spawn::ensure_worker（内部 worker_alive 是 ~50μs UDS probe），死了就重新 spawn
/// + 重放 cluster；活着就直接通过。这保证"业务调完 ensureWorker 后下一行 produce
/// 一定不会再触发 worker 自愈 retry"语义成立。频繁 produce 的 hot path 仍走
/// `ipc::ensure` 自己的 cache 快路径，不受影响。
#[php_function]
pub fn hi_kafka_ensure_worker(socket: Option<String>) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    worker_health::invalidate(&socket);
    ipc::ensure(&socket).map_err(|e| PhpException::default(format!("ensure_worker: {e}")))?;
    Ok(())
}

/// 注册或覆盖一个 Kafka 集群。
///
/// `$config` 必须包含 `bootstrap.servers`，其它键值原样透传给 librdkafka。
/// 同名集群配置会被覆盖（注意：已建立的连接不会立即重建）。
///
/// 业务侧的标准模式：在请求开始前注册所需集群，之后 produce/consume
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
    ipc::register_cluster(&socket, cluster, cfg_vec, timeout).map_err(ipc_err_to_php)?;
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
    key: BinarySlice<u8>,
    value: BinarySlice<u8>,
    headers: Option<&ZendHashTable>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
    socket: Option<String>,
) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    let opts = build_options(headers, partition, timestamp_ms)?;
    ipc::produce_fnf_bin(&socket, cluster, topic, &key, &value, opts).map_err(ipc_err_to_php)
}

/// 同步带 ack 全局函数 API。参数同 [`hi_kafka_produce_fnf`] + `timeout_ms`。
#[php_function]
pub fn hi_kafka_produce_sync(
    cluster: &str,
    topic: &str,
    key: BinarySlice<u8>,
    value: BinarySlice<u8>,
    headers: Option<&ZendHashTable>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
    timeout_ms: Option<i64>,
    socket: Option<String>,
) -> PhpResult<Zval> {
    let socket = resolve_socket(socket.as_deref());
    let timeout = std::time::Duration::from_millis(require_positive_u32(
        "timeout_ms",
        timeout_ms.unwrap_or(5000),
    )? as u64);
    let opts = build_options(headers, partition, timestamp_ms)?;
    let resp = ipc::produce_sync_bin(&socket, cluster, topic, &key, &value, opts, timeout)
        .map_err(ipc_err_to_php)?;
    client::resp_to_zval(resp)
}

pub(crate) fn build_options(
    headers: Option<&ZendHashTable>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
) -> PhpResult<ipc::ProduceOptions> {
    Ok(ipc::ProduceOptions {
        headers: parse_headers(headers)?,
        partition: partition
            .map(|p| require_i32("partition", p))
            .transpose()?
            .unwrap_or(-1),
        timestamp_ms: timestamp_ms.unwrap_or(-1),
    })
}

/// 把两个平行数组组合成 binary headers `Vec<(String, Bytes)>`。
/// 头部 name UTF-8（Kafka 协议要求），value 任意字节。两数组长度必须一致。
/// 解析 `array<string, string>` 消息头。header 值是任意字节（不要求 UTF-8），
/// `null` 值被跳过，整数 / 浮点值按 PHP 字符串形式写入。
pub(crate) fn parse_headers(
    headers: Option<&ZendHashTable>,
) -> PhpResult<Vec<(String, bytes::Bytes)>> {
    let Some(headers) = headers else {
        return Ok(Vec::new());
    };
    let mut parsed = Vec::with_capacity(headers.len());
    for (name, value) in headers {
        if value.is_null() {
            continue;
        }
        let bytes = if let Some(value) = value.zend_str() {
            bytes::Bytes::copy_from_slice(value.as_bytes())
        } else if let Some(value) = value.long() {
            bytes::Bytes::from(value.to_string())
        } else if let Some(value) = value.double() {
            bytes::Bytes::from(value.to_string())
        } else {
            return Err(PhpException::default(format!(
                "header '{name}' must be a string"
            )));
        };
        parsed.push((name.to_string(), bytes));
    }
    Ok(parsed)
}

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
/// 协程感知由 composer 包内的 Swoole/Swow driver 提供。
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

/// `Error` 帧的帧类型字节（`0x40`）。PHP driver 用 `hi_kafka_parse_header` 拿到 kind
/// 后与它比较，判断该帧是否要走 `hi_kafka_decode_error_frame` + 抛 KafkaException。
#[php_function]
pub fn hi_kafka_error_frame_kind() -> i64 {
    hi_kafka_proto::FrameType::Error as u8 as i64
}

/// 解析完整 `Error` 帧 →
/// `['kind'=>int, 'kind_name'=>str, 'retryable'=>bool, 'outcome'=>str, 'native_code'=>int, 'message'=>str]`。
#[php_function]
pub fn hi_kafka_decode_error_frame(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let pe =
        protocol::parse_error_frame(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
    let mut ht = ZendHashTable::new();
    put(&mut ht, "kind", pe.kind as i64)?;
    put(&mut ht, "kind_name", pe.kind_name)?;
    put(&mut ht, "retryable", pe.retryable)?;
    put(&mut ht, "outcome", pe.outcome.as_str())?;
    put(&mut ht, "native_code", pe.native_code as i64)?;
    put(&mut ht, "message", pe.message.as_str())?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

/// 编一帧 HELLO（协议握手）。role: 0=RPC, 1=ConsumerStream, 2=Lifecycle。
///
/// PHP 协程 driver 在新建 UDS 连接后必须先发它再发业务帧；worker 会用
/// HELLO RESP 回应，校验 `PROTOCOL_MAJOR` 是否一致。
#[php_function]
pub fn hi_kafka_encode_hello_frame(role: Option<i64>) -> PhpResult<Binary<u8>> {
    let role = match role.unwrap_or(0) {
        0 => hi_kafka_proto::ConnectionRole::Rpc,
        1 => hi_kafka_proto::ConnectionRole::ConsumerStream,
        2 => hi_kafka_proto::ConnectionRole::Lifecycle,
        value => {
            return Err(PhpException::default(format!(
                "invalid HELLO connection role {value}"
            )))
        }
    };
    let bytes =
        protocol::build_hello_frame_for(role).map_err(|e| PhpException::default(e.to_string()))?;
    Ok(Binary::from(bytes))
}

/// 校验 HELLO RESP 帧；版本不匹配 / 帧格式不对 → 抛错。
#[php_function]
pub fn hi_kafka_verify_hello_resp(bytes: BinarySlice<u8>) -> PhpResult<()> {
    protocol::parse_hello_resp(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
    Ok(())
}

/// 编一帧 PRODUCE_FNF（fire-and-forget），返回完整帧字节串（PHP binary string）。
#[php_function]
pub fn hi_kafka_encode_fnf_frame(
    cluster: &str,
    topic: &str,
    key: BinarySlice<u8>,
    value: BinarySlice<u8>,
    headers: Option<&ZendHashTable>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
) -> PhpResult<Binary<u8>> {
    let opts = build_options(headers, partition, timestamp_ms)?;
    let bytes = protocol::build_fnf_frame(
        cluster,
        topic,
        &key,
        &value,
        opts.headers,
        opts.partition,
        opts.timestamp_ms,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(Binary::from(bytes))
}

/// 编一帧 PRODUCE_REQ，返回 `['cid' => int, 'frame' => binary]`。
#[php_function]
pub fn hi_kafka_encode_req_frame(
    cluster: &str,
    topic: &str,
    key: BinarySlice<u8>,
    value: BinarySlice<u8>,
    headers: Option<&ZendHashTable>,
    partition: Option<i64>,
    timestamp_ms: Option<i64>,
    timeout_ms: Option<i64>,
) -> PhpResult<Zval> {
    let opts = build_options(headers, partition, timestamp_ms)?;
    let timeout_ms = require_positive_u32("timeout_ms", timeout_ms.unwrap_or(5000))?;
    let (cid, bytes) = protocol::build_req_frame(
        cluster,
        topic,
        &key,
        &value,
        opts.headers,
        opts.partition,
        opts.timestamp_ms,
        timeout_ms,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// 仅解析 13B 帧头，返回 `['kind' => int, 'cid' => int, 'payload_len' => int]`。
#[php_function]
pub fn hi_kafka_parse_header(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let h =
        protocol::parse_header_only(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
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
    let parsed =
        protocol::parse_resp_frame(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
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

// === Control-plane protocol primitives =====================================

/// 编一帧 REGISTER_CLUSTER_REQ。
#[php_function]
pub fn hi_kafka_encode_register_cluster_frame(
    cluster: &str,
    config: std::collections::HashMap<String, String>,
) -> PhpResult<Zval> {
    let cfg_vec: Vec<(String, String)> = config.into_iter().collect();
    let (cid, bytes) = protocol::build_register_cluster_frame(cluster, cfg_vec)
        .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

/// Decode a control-plane response frame.
#[php_function]
pub fn hi_kafka_decode_control_resp(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let parsed = protocol::parse_control_resp_frame(&bytes)
        .map_err(|e| PhpException::default(e.to_string()))?;
    let mut ht = ZendHashTable::new();
    let (kind, cid, ok, message) = match parsed {
        protocol::ControlResp::RegisterCluster { cid, ok, message } => {
            ("register_cluster", cid, ok, message)
        }
        protocol::ControlResp::OAuth { cid, ok, message } => ("set_oauth_token", cid, ok, message),
    };
    put(&mut ht, "kind", kind)?;
    put(&mut ht, "cid", cid as i64)?;
    put(&mut ht, "ok", ok)?;
    if let Some(message) = message {
        put(&mut ht, "message", message.as_str())?;
    }
    ht.into_zval(false)
        .map_err(|e| PhpException::default(e.to_string()))
}

// === Registration replay ====================================================

/// 记录一次已由协程 driver 成功完成的集群注册。
///
/// 阻塞 `Client` 在 Rust 内部注册时自动记录；Swoole/Swow driver 自己走协程 IO，
/// 必须在收到成功响应后调用本函数，worker 重启后扩展才能把它重放到新 worker。
#[php_function]
pub fn hi_kafka_remember_cluster(
    cluster: &str,
    config: std::collections::HashMap<String, String>,
    socket: Option<String>,
) {
    let socket = resolve_socket(socket.as_deref());
    cluster_replay::record_cluster(&socket, cluster, config.into_iter().collect());
}

/// 记录一次已由协程 driver 成功推送的 OAuth token，语义同 [`hi_kafka_remember_cluster`]。
#[php_function]
pub fn hi_kafka_remember_oauth_token(
    cluster: &str,
    token: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Option<std::collections::HashMap<String, String>>,
    socket: Option<String>,
) {
    let socket = resolve_socket(socket.as_deref());
    cluster_replay::record_oauth_token(
        &socket,
        cluster,
        cluster_replay::OAuthToken {
            token_value: token.to_string(),
            lifetime_ms,
            principal_name: principal_name.to_string(),
            extensions: extensions.unwrap_or_default().into_iter().collect(),
        },
    );
}

/// 确保 worker 存活，并把本进程记录的全部集群注册重放给它。
///
/// worker 被其它进程重启后，本进程的集群不在新 worker 中；driver 收到
/// `CLUSTER_NOT_REGISTERED` 时调用本函数后再重试一次。
#[php_function]
pub fn hi_kafka_replay_clusters(socket: Option<String>) -> PhpResult<()> {
    let socket = resolve_socket(socket.as_deref());
    ipc::replay(&socket).map_err(ipc_err_to_php)
}

// === Control-plane request encoders ========================================

/// 编一帧 SET_OAUTH_BEARER_TOKEN_REQ。
#[php_function]
pub fn hi_kafka_encode_set_oauth_token_frame(
    cluster: &str,
    token: &str,
    lifetime_ms: i64,
    principal_name: &str,
    extensions: Option<std::collections::HashMap<String, String>>,
) -> PhpResult<Zval> {
    let ext_vec: Vec<(String, String)> = extensions.unwrap_or_default().into_iter().collect();
    let (cid, bytes) =
        protocol::build_set_oauth_token_frame(cluster, token, lifetime_ms, principal_name, ext_vec)
            .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, bytes)
}

// === Consumer stream codecs ================================================

#[php_function]
pub fn hi_kafka_encode_consume_open_frame(
    cluster: &str,
    group_id: &str,
    topics: Vec<String>,
    config: Option<std::collections::HashMap<String, String>>,
    initial_message_credit: i64,
    initial_byte_credit: i64,
    max_in_flight_messages: i64,
    max_in_flight_bytes: i64,
) -> PhpResult<Zval> {
    let initial_messages = u32::try_from(initial_message_credit)
        .map_err(|_| PhpException::default("invalid initial_message_credit".into()))?;
    let initial_bytes = u64::try_from(initial_byte_credit)
        .map_err(|_| PhpException::default("invalid initial_byte_credit".into()))?;
    let max_messages = require_positive_u32("max_in_flight_messages", max_in_flight_messages)?;
    let max_bytes = require_positive_u64("max_in_flight_bytes", max_in_flight_bytes)?;
    let (cid, frame) = protocol::build_consume_open_frame(
        cluster,
        group_id,
        topics,
        config.unwrap_or_default().into_iter().collect(),
        initial_messages,
        initial_bytes,
        max_messages,
        max_bytes,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, frame)
}

#[php_function]
pub fn hi_kafka_encode_consume_resume_frame(
    subscription_id: i64,
    previous_epoch: i64,
    token: BinarySlice<u8>,
    message_credit: i64,
    byte_credit: i64,
) -> PhpResult<Zval> {
    let (cid, frame) = protocol::build_consume_resume_frame(
        require_positive_u64("subscription_id", subscription_id)?,
        require_positive_u64("previous_epoch", previous_epoch)?,
        &token,
        u32::try_from(message_credit)
            .map_err(|_| PhpException::default("invalid message_credit".into()))?,
        u64::try_from(byte_credit)
            .map_err(|_| PhpException::default("invalid byte_credit".into()))?,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, frame)
}

#[php_function]
pub fn hi_kafka_encode_consume_ack_frame(
    subscription_id: i64,
    epoch: i64,
    delivery_id: i64,
    message_credit: Option<i64>,
    byte_credit: Option<i64>,
) -> PhpResult<Binary<u8>> {
    let frame = protocol::build_consume_ack_frame(
        require_positive_u64("subscription_id", subscription_id)?,
        require_positive_u64("epoch", epoch)?,
        require_positive_u64("delivery_id", delivery_id)?,
        u32::try_from(message_credit.unwrap_or(0))
            .map_err(|_| PhpException::default("invalid message_credit".into()))?,
        u64::try_from(byte_credit.unwrap_or(0))
            .map_err(|_| PhpException::default("invalid byte_credit".into()))?,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    Ok(frame.into())
}

#[php_function]
pub fn hi_kafka_encode_consume_nack_frame(
    subscription_id: i64,
    epoch: i64,
    delivery_id: i64,
    reason: Option<String>,
) -> PhpResult<Binary<u8>> {
    Ok(protocol::build_consume_nack_frame(
        require_positive_u64("subscription_id", subscription_id)?,
        require_positive_u64("epoch", epoch)?,
        require_positive_u64("delivery_id", delivery_id)?,
        reason.unwrap_or_default(),
    )
    .map_err(|e| PhpException::default(e.to_string()))?
    .into())
}

#[php_function]
pub fn hi_kafka_encode_flow_frame(
    subscription_id: i64,
    epoch: i64,
    message_credit: i64,
    byte_credit: i64,
) -> PhpResult<Binary<u8>> {
    Ok(protocol::build_flow_frame(
        require_positive_u64("subscription_id", subscription_id)?,
        require_positive_u64("epoch", epoch)?,
        u32::try_from(message_credit)
            .map_err(|_| PhpException::default("invalid message_credit".into()))?,
        u64::try_from(byte_credit)
            .map_err(|_| PhpException::default("invalid byte_credit".into()))?,
    )
    .map_err(|e| PhpException::default(e.to_string()))?
    .into())
}

#[php_function]
pub fn hi_kafka_encode_consume_commit_frame(subscription_id: i64, epoch: i64) -> PhpResult<Zval> {
    let (cid, frame) = protocol::build_consume_commit_frame(
        require_positive_u64("subscription_id", subscription_id)?,
        require_positive_u64("epoch", epoch)?,
    )
    .map_err(|e| PhpException::default(e.to_string()))?;
    cid_frame_zval(cid, frame)
}

#[php_function]
pub fn hi_kafka_encode_consume_close_frame(
    subscription_id: i64,
    epoch: i64,
) -> PhpResult<Binary<u8>> {
    Ok(protocol::build_consume_close_frame(
        require_positive_u64("subscription_id", subscription_id)?,
        require_positive_u64("epoch", epoch)?,
    )
    .map_err(|e| PhpException::default(e.to_string()))?
    .into())
}

#[php_function]
pub fn hi_kafka_encode_consume_pause_frame(
    subscription_id: i64,
    epoch: i64,
    partitions: &ZendHashTable,
) -> PhpResult<Binary<u8>> {
    encode_consumer_partition_control(
        hi_kafka_proto::FrameType::ConsumePause,
        subscription_id,
        epoch,
        partitions,
    )
}

#[php_function]
pub fn hi_kafka_encode_consume_partition_resume_frame(
    subscription_id: i64,
    epoch: i64,
    partitions: &ZendHashTable,
) -> PhpResult<Binary<u8>> {
    encode_consumer_partition_control(
        hi_kafka_proto::FrameType::ConsumePartitionResume,
        subscription_id,
        epoch,
        partitions,
    )
}

fn encode_consumer_partition_control(
    kind: hi_kafka_proto::FrameType,
    subscription_id: i64,
    epoch: i64,
    partitions: &ZendHashTable,
) -> PhpResult<Binary<u8>> {
    Ok(protocol::build_consume_partition_frame(
        kind,
        require_positive_u64("subscription_id", subscription_id)?,
        require_positive_u64("epoch", epoch)?,
        consumer_stream::parse_partitions(partitions)?,
    )
    .map_err(|e| PhpException::default(e.to_string()))?
    .into())
}

#[php_function]
pub fn hi_kafka_decode_stream_frame(bytes: BinarySlice<u8>) -> PhpResult<Zval> {
    let frame =
        protocol::parse_stream_frame(&bytes).map_err(|e| PhpException::default(e.to_string()))?;
    let mut result = ZendHashTable::new();
    match frame {
        protocol::StreamFrame::Ready { cid, value } => {
            put(&mut result, "kind", "ready")?;
            put(&mut result, "cid", cid as i64)?;
            put(&mut result, "subscription_id", value.subscription_id as i64)?;
            put(&mut result, "epoch", value.subscription_epoch as i64)?;
            put(
                &mut result,
                "resume_token",
                Binary::<u8>::from(value.resume_token.0.to_vec()),
            )?;
            put(
                &mut result,
                "max_in_flight_messages",
                value.max_in_flight_messages as i64,
            )?;
            put(
                &mut result,
                "max_in_flight_bytes",
                value.max_in_flight_bytes as i64,
            )?;
        }
        protocol::StreamFrame::Message(value) => {
            put(&mut result, "kind", "message")?;
            let object = ZendClassObject::new(ConsumerRecord::from(value))
                .into_zval(false)
                .map_err(|e| PhpException::default(e.to_string()))?;
            result
                .insert("message", object)
                .map_err(|e| PhpException::default(e.to_string()))?;
        }
        protocol::StreamFrame::Committed { cid, value } => {
            put(&mut result, "kind", "committed")?;
            put(&mut result, "cid", cid as i64)?;
            put(&mut result, "subscription_id", value.subscription_id as i64)?;
            put(&mut result, "epoch", value.subscription_epoch as i64)?;
        }
        protocol::StreamFrame::Assign(value) => {
            put(&mut result, "kind", "assign")?;
            put(&mut result, "subscription_id", value.subscription_id as i64)?;
            put(&mut result, "epoch", value.subscription_epoch as i64)?;
        }
        protocol::StreamFrame::Revoke(value) => {
            put(&mut result, "kind", "revoke")?;
            put(&mut result, "subscription_id", value.subscription_id as i64)?;
            put(&mut result, "epoch", value.subscription_epoch as i64)?;
        }
        protocol::StreamFrame::Close(value) => {
            put(&mut result, "kind", "close")?;
            put(&mut result, "subscription_id", value.subscription_id as i64)?;
            put(&mut result, "epoch", value.subscription_epoch as i64)?;
        }
    }
    result
        .into_zval(false)
        .map_err(|e| PhpException::default(e.to_string()))
}

/// 把（cid, frame bytes）打包成 `['cid' => int, 'frame' => binary]`。
/// 接 `Vec<u8>` 而非 `&[u8]` 是为了直接 move 进 Binary，避免一次 to_vec 拷贝。
fn cid_frame_zval(cid: u64, bytes: Vec<u8>) -> PhpResult<Zval> {
    let mut ht = ZendHashTable::new();
    ht.insert("cid", cid as i64)
        .map_err(|e| PhpException::default(format!("cid: {e}")))?;
    ht.insert("frame", Binary::<u8>::from(bytes))
        .map_err(|e| PhpException::default(format!("frame: {e}")))?;
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}

fn put<V: IntoZval>(ht: &mut ZendHashTable, key: &str, value: V) -> PhpResult<()> {
    ht.insert(key, value)
        .map_err(|e| PhpException::default(format!("{key}: {e}")))
}

/// MINIT 前置钩子：注册 runtime interface、异常类和 hi_kafka.* INI 项。
///
/// **顺序约束**：`client_interface::register()` 必须在 `#[php_class]` 类注册之前
/// 完成。ext-php-rs 0.14+ 的 `#[php(startup = ...)]` 包装器先调用本函数，随后才
/// 注册 `ModuleBuilder` 中声明的类，因此类解析 `get_ce()` 时 interface 已存在。
fn module_startup(_type: i32, module_number: i32) -> i32 {
    client_interface::register();
    consumer_stream_interface::register();
    kafka_exception::register();
    ini_config::register(module_number);
    0
}

/// MSHUTDOWN 钩子：PHP 进程退出时主动通知所用过的 worker 自退（见 [`lifecycle`]）。
///
/// 不与 `#[php_class]` 的自动 class 注册冲突——那套逻辑挂在 module **startup**，
/// 这里只设 module **shutdown**（`module_shutdown_func` 默认 None，无被覆盖之虞）。
///
/// `extern "C"`：跨 FFI 边界 panic 是 UB，用 `catch_unwind` 把任何意外 unwind 兜在
/// Rust 侧；清理本就是 best-effort，失败无所谓。返回 PHP 约定的 SUCCESS(0)。
extern "C" fn module_shutdown(_type: i32, _module_number: i32) -> i32 {
    let _ = std::panic::catch_unwind(lifecycle::on_module_shutdown);
    0
}

#[php_module]
#[php(startup = "module_startup")]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    // ext-php-rs 0.14+ 不再通过全局 inventory 隐式发现导出项；这里是扩展公开
    // PHP API 的唯一注册清单，新增 `#[php_function]` / `#[php_class]` 时必须同步加入。
    module
        .function(wrap_function!(hi_kafka_version))
        .function(wrap_function!(hi_kafka_ensure_worker))
        .function(wrap_function!(hi_kafka_register_cluster))
        .function(wrap_function!(hi_kafka_remember_cluster))
        .function(wrap_function!(hi_kafka_remember_oauth_token))
        .function(wrap_function!(hi_kafka_replay_clusters))
        .function(wrap_function!(hi_kafka_produce_fnf))
        .function(wrap_function!(hi_kafka_produce_sync))
        .function(wrap_function!(hi_kafka_pool_stats))
        .function(wrap_function!(hi_kafka_retry_stats))
        .function(wrap_function!(hi_kafka_runtime))
        .function(wrap_function!(hi_kafka_next_cid))
        .function(wrap_function!(hi_kafka_header_len))
        .function(wrap_function!(hi_kafka_error_frame_kind))
        .function(wrap_function!(hi_kafka_decode_error_frame))
        .function(wrap_function!(hi_kafka_encode_hello_frame))
        .function(wrap_function!(hi_kafka_verify_hello_resp))
        .function(wrap_function!(hi_kafka_encode_fnf_frame))
        .function(wrap_function!(hi_kafka_encode_req_frame))
        .function(wrap_function!(hi_kafka_parse_header))
        .function(wrap_function!(hi_kafka_decode_resp_frame))
        .function(wrap_function!(hi_kafka_encode_register_cluster_frame))
        .function(wrap_function!(hi_kafka_decode_control_resp))
        .function(wrap_function!(hi_kafka_encode_set_oauth_token_frame))
        .function(wrap_function!(hi_kafka_encode_consume_open_frame))
        .function(wrap_function!(hi_kafka_encode_consume_resume_frame))
        .function(wrap_function!(hi_kafka_encode_consume_ack_frame))
        .function(wrap_function!(hi_kafka_encode_consume_nack_frame))
        .function(wrap_function!(hi_kafka_encode_flow_frame))
        .function(wrap_function!(hi_kafka_encode_consume_commit_frame))
        .function(wrap_function!(hi_kafka_encode_consume_close_frame))
        .function(wrap_function!(hi_kafka_encode_consume_pause_frame))
        .function(wrap_function!(
            hi_kafka_encode_consume_partition_resume_frame
        ))
        .function(wrap_function!(hi_kafka_decode_stream_frame))
        .class::<Client>()
        .class::<ConsumerRecord>()
        .class::<ConsumerStream>()
        .shutdown_function(module_shutdown)
}
