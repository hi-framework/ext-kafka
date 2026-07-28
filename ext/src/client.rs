//! PHP 端 `Hi\Kafka\Client` 类的实现。
//!
//! 把全局的 `hi_kafka_produce_*` 函数封装成对象方法，`socket` 路径作为对象状态，
//! 避免每次调用都传一遍。
//!
//! **参数命名**：方法参数刻意用 **camelCase**（`timeoutMs` / `groupId` / …）。
//! ext-php-rs 直接把 Rust 参数标识符当 PHP 参数名（只对方法名做 snake→camel），
//! 而 PHP 是公开 API、应遵循 camelCase 惯例并与 `ClientInterface` / `SwooleClient` /
//! `SwowClient` 及命名参数调用方一致——故这里让 Rust 侧非惯例、换取 PHP 侧地道。
#![allow(non_snake_case)]

use crate::ipc;
use crate::worker_health;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use hi_kafka_proto::ProduceResp;

#[php_class(name = "Hi\\Kafka\\Client")]
#[implements(crate::client_interface::get_ce())]
#[derive(Debug)]
pub struct Client {
    socket: String,
}

#[php_impl]
impl Client {
    /// 创建 client。
    ///
    /// `$socket` 可选；不指定时按 `HI_KAFKA_SOCKET` 环境变量，
    /// 兜底为 `/tmp/hi-kafka-v2.sock`。
    pub fn __construct(socket: Option<String>) -> Self {
        Self {
            socket: super::resolve_socket(socket.as_deref()),
        }
    }

    /// 返回当前 socket 路径。
    pub fn socket(&self) -> String {
        self.socket.clone()
    }

    /// 注册或覆盖一个 Kafka 集群。`$config` 须含 `bootstrap.servers`。
    pub fn register_cluster(
        &self,
        cluster: &str,
        config: std::collections::HashMap<String, String>,
        timeoutMs: Option<i64>,
    ) -> PhpResult<()> {
        let timeout = std::time::Duration::from_millis(timeoutMs.unwrap_or(5000).max(1) as u64);
        let cfg_vec: Vec<(String, String)> = config.into_iter().collect();
        ipc::register_cluster(&self.socket, cluster, cfg_vec, timeout)
            .map_err(super::ipc_err_to_php)
    }

    /// 显式拉起 worker（如果还没在跑）。
    /// L: 强制 fresh probe（invalidate + ensure），死了就 spawn + replay cluster；
    /// 活着就快速通过。保证"调完 ensureWorker 后下一行业务 RPC 不会再触发自愈"。
    pub fn ensure_worker(&self) -> PhpResult<()> {
        worker_health::invalidate(&self.socket);
        ipc::ensure(&self.socket)
            .map_err(|e| PhpException::default(format!("ensure_worker: {e}")))?;
        Ok(())
    }

    /// Fire-and-forget 生产。`$headers` 关联数组（`[]` = 无 headers）；
    /// `$partition` 与 `$timestampMs` 可选，`null` / 缺省 = auto。
    pub fn produce_fnf(
        &self,
        cluster: &str,
        topic: &str,
        key: &str,
        value: &str,
        headers: Option<std::collections::HashMap<String, String>>,
        partition: Option<i64>,
        timestampMs: Option<i64>,
    ) -> PhpResult<()> {
        let opts = super::build_options(headers, partition, timestampMs)?;
        ipc::produce_fnf(&self.socket, cluster, topic, key, value, opts)
            .map_err(super::ipc_err_to_php)
    }

    /// 同步生产，等 broker ack。返回 `['ok' => bool, ...]` 数组。
    pub fn produce_sync(
        &self,
        cluster: &str,
        topic: &str,
        key: &str,
        value: &str,
        headers: Option<std::collections::HashMap<String, String>>,
        partition: Option<i64>,
        timestampMs: Option<i64>,
        timeoutMs: Option<i64>,
    ) -> PhpResult<Zval> {
        let timeout = std::time::Duration::from_millis(super::require_positive_u32(
            "timeoutMs",
            timeoutMs.unwrap_or(5000),
        )? as u64);
        let opts = super::build_options(headers, partition, timestampMs)?;
        let resp = ipc::produce_sync(&self.socket, cluster, topic, key, value, opts, timeout)
            .map_err(super::ipc_err_to_php)?;
        resp_to_zval(resp)
    }

    /// **Binary-safe** fire-and-forget。`$key` / `$value` 接 PHP binary string
    /// （含 NUL 字节也安全）；`$headerNames` 是 UTF-8 名（Kafka 协议要求），
    /// `$headerValues` 平行数组，元素是 PHP binary string。
    ///
    /// 用于 protobuf / msgpack / 加密 payload 等二进制场景。
    /// 文本场景仍然可以用 `produceFnf`（HashMap<string, string>，更短）。
    pub fn produce_fnf_bin(
        &self,
        cluster: &str,
        topic: &str,
        key: ext_php_rs::binary::Binary<u8>,
        value: ext_php_rs::binary::Binary<u8>,
        headerNames: Vec<String>,
        headerValues: Vec<ext_php_rs::binary::Binary<u8>>,
        partition: Option<i64>,
        timestampMs: Option<i64>,
    ) -> PhpResult<()> {
        let key_vec: Vec<u8> = key.into();
        let value_vec: Vec<u8> = value.into();
        let headers = super::build_binary_headers(headerNames, headerValues)
            .map_err(PhpException::default)?;
        let opts = ipc::ProduceOptions {
            headers,
            partition: partition
                .map(|p| super::require_i32("partition", p))
                .transpose()?
                .unwrap_or(-1),
            timestamp_ms: timestampMs.unwrap_or(-1),
        };
        ipc::produce_fnf_bin(&self.socket, cluster, topic, &key_vec, &value_vec, opts)
            .map_err(super::ipc_err_to_php)
    }

    /// Binary-safe 同步生产。参数同 `produceFnfBin` + `$timeoutMs`。
    pub fn produce_sync_bin(
        &self,
        cluster: &str,
        topic: &str,
        key: ext_php_rs::binary::Binary<u8>,
        value: ext_php_rs::binary::Binary<u8>,
        headerNames: Vec<String>,
        headerValues: Vec<ext_php_rs::binary::Binary<u8>>,
        partition: Option<i64>,
        timestampMs: Option<i64>,
        timeoutMs: Option<i64>,
    ) -> PhpResult<Zval> {
        let timeout = std::time::Duration::from_millis(super::require_positive_u32(
            "timeoutMs",
            timeoutMs.unwrap_or(5000),
        )? as u64);
        let key_vec: Vec<u8> = key.into();
        let value_vec: Vec<u8> = value.into();
        let headers = super::build_binary_headers(headerNames, headerValues)
            .map_err(PhpException::default)?;
        let opts = ipc::ProduceOptions {
            headers,
            partition: partition
                .map(|p| super::require_i32("partition", p))
                .transpose()?
                .unwrap_or(-1),
            timestamp_ms: timestampMs.unwrap_or(-1),
        };
        let resp = ipc::produce_sync_bin(
            &self.socket,
            cluster,
            topic,
            &key_vec,
            &value_vec,
            opts,
            timeout,
        )
        .map_err(super::ipc_err_to_php)?;
        resp_to_zval(resp)
    }

    /// Open a dedicated protocol-v2 Consumer stream.  The returned object is
    /// not backed by the RPC pool and yields exactly one ConsumerRecord per next().
    pub fn consume(
        &self,
        cluster: &str,
        groupId: &str,
        topics: Vec<String>,
        config: Option<std::collections::HashMap<String, String>>,
        timeoutMs: Option<i64>,
    ) -> PhpResult<
        ext_php_rs::boxed::ZBox<
            ext_php_rs::types::ZendClassObject<crate::consumer_stream::ConsumerStream>,
        >,
    > {
        let timeout = std::time::Duration::from_millis(timeoutMs.unwrap_or(5000).max(1) as u64);
        let stream = crate::consumer_stream::ConsumerStream::connect(
            &self.socket,
            cluster,
            groupId,
            topics,
            config.unwrap_or_default().into_iter().collect(),
            timeout,
        )
        .map_err(super::ipc_err_to_php)?;
        Ok(ext_php_rs::types::ZendClassObject::new(stream))
    }

    /// 为指定 cluster 推送 SASL/OAUTHBEARER token。
    ///
    /// 设计：PHP 端负责拉 token（HTTP/STS/k8s secret/Cloud SDK 等），调本方法
    /// 写到 worker 内部 per-cluster slot；librdkafka 触发 token refresh 回调时
    /// worker 直接从 slot 读返回。token 缺失时 librdkafka 按其退避策略重试。
    ///
    /// 业务侧典型刷新策略：
    /// - 定时器：每 `lifetimeMs - now - 5min` 触发一次再拉一次推回去
    /// - 监听：订阅自家 token 服务的 webhook，token rotate 时立即推
    ///
    /// `$extensions` 为 SASL extension key/value（预留，rdkafka 0.38 还没透传给
    /// librdkafka——worker 会存但暂时不用，等 rdkafka 上游开支持自动生效）。
    pub fn set_o_auth_bearer_token(
        &self,
        cluster: &str,
        token: &str,
        lifetimeMs: i64,
        principalName: &str,
        extensions: Option<std::collections::HashMap<String, String>>,
        timeoutMs: Option<i64>,
    ) -> PhpResult<()> {
        let timeout = std::time::Duration::from_millis(timeoutMs.unwrap_or(5000).max(1) as u64);
        let ext_vec: Vec<(String, String)> = extensions.unwrap_or_default().into_iter().collect();
        ipc::set_oauth_bearer_token(
            &self.socket,
            cluster,
            token,
            lifetimeMs,
            principalName,
            ext_vec,
            timeout,
        )
        .map_err(super::ipc_err_to_php)
    }
}

pub fn resp_to_zval(resp: ProduceResp) -> PhpResult<Zval> {
    let mut ht = ZendHashTable::new();
    match resp {
        ProduceResp::Ok(ack) => {
            ht.insert("ok", true)
                .map_err(|e| PhpException::default(format!("insert ok: {e}")))?;
            ht.insert("partition", ack.partition as i64)
                .map_err(|e| PhpException::default(format!("insert partition: {e}")))?;
            ht.insert("offset", ack.offset)
                .map_err(|e| PhpException::default(format!("insert offset: {e}")))?;
        }
        ProduceResp::Err(err) => {
            ht.insert("ok", false)
                .map_err(|e| PhpException::default(format!("insert ok: {e}")))?;
            ht.insert("code", err.code as i64)
                .map_err(|e| PhpException::default(format!("insert code: {e}")))?;
            ht.insert("message", err.message.as_str())
                .map_err(|e| PhpException::default(format!("insert message: {e}")))?;
            ht.insert("retryable", err.retryable)
                .map_err(|e| PhpException::default(format!("insert retryable: {e}")))?;
        }
    }
    ht.into_zval(false)
        .map_err(|e| PhpException::default(format!("into_zval: {e}")))
}
