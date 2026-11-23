//! 集群注册表 —— 多 Kafka 集群配置的运行时单一源。
//!
//! PHP 端通过 `REGISTER_CLUSTER_REQ` 帧把每个集群的 librdkafka 配置
//! 写入注册表；Producer 和 Consumer 从同一份注册表惰性创建客户端。
//!
//! 每个集群还附带一个 SASL/OAUTHBEARER **token slot**（[`OAuthTokenSlot`]）：
//! PHP 端通过 `SET_OAUTH_BEARER_TOKEN_REQ` 推 token，librdkafka 触发 token
//! refresh 回调时 worker 的 `ClientContext::generate_oauth_bearer_token`
//! 直接读 slot 返回。token 缺失时 librdkafka 会按其自身的退避策略重试。
//!
//! 不存在启动参数或环境变量 broker fallback；业务必须通过协议显式注册，
//! Cluster Registry 是唯一权威配置源。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use tokio::sync::RwLock;

pub type ClusterConfig = HashMap<String, String>;

/// worker 为每个 librdkafka 客户端（producer 与 consumer）设置的默认值；业务在集群
/// 配置中显式设置同名键时以业务为准。
///
/// `statistics.interval.ms`：驱动 broker 列表兜底（见 `broker_watchdog`）。
pub const CLIENT_DEFAULTS: &[(&str, &str)] = &[("statistics.interval.ms", "5000")];

/// 单个集群存储的 OAuth token 完整描述。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredOAuthToken {
    pub token_value: String,
    /// Token 失效时间（unix epoch ms）。librdkafka 用这个决定下次 refresh 时机。
    pub lifetime_ms: i64,
    pub principal_name: String,
    pub extensions: Vec<(String, String)>,
}

/// Per-cluster token 共享槽位。`Arc<Mutex<Option<...>>>` 让：
/// - PHP 端经 server.rs 写入 → 取 lock 直接 replace
/// - librdkafka token_refresh callback 读取 → 取 lock 读 clone
///
/// 用 `std::sync::Mutex`（而非 tokio::Mutex），因为 librdkafka 回调跑在
/// 它自己的同步线程里，**不能 await**。
pub type OAuthTokenSlot = Arc<StdMutex<Option<StoredOAuthToken>>>;

/// 单集群的所有可变状态。
#[derive(Debug, Default)]
struct ClusterEntry {
    config: ClusterConfig,
    token: OAuthTokenSlot,
    /// P1 #7：config 版本号，register 覆盖时 +1。Producer/Consumer 在自己的
    /// per-cluster 缓存里记录创建时的 version，每次取用前比对，发现 stale
    /// 自动重建 librdkafka 客户端。原先注释「覆盖**不会**自动重建」是已知坑，
    /// 现在闭环了。
    version: u64,
}

/// [`ClusterRegistry::register`] 的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterOutcome {
    Added,
    Unchanged,
    Updated,
}

#[derive(Debug, Default)]
pub struct ClusterRegistry {
    clusters: RwLock<HashMap<String, ClusterEntry>>,
}

impl ClusterRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// 注册或覆盖一个集群（幂等 upsert）。
    ///
    /// - 新集群：分配空 token slot，version = 1。
    /// - 配置与现有**完全相同**：不做任何改变，version 不变。每个 PHP 进程启动时都会
    ///   重复注册同一份配置，若这里也递增 version，会让共享 Producer 被反复重建。
    /// - 配置有变化：替换配置并递增 version；token slot 保留同一个 Arc，已持有它的
    ///   librdkafka 客户端仍能读到 token。Producer 发现 version 变化后切换到新实例，
    ///   旧实例排干后释放；存量 Consumer subscription 继续使用创建时的配置。
    pub async fn register(&self, name: String, config: ClusterConfig) -> RegisterOutcome {
        let mut map = self.clusters.write().await;
        match map.get_mut(&name) {
            Some(entry) if entry.config == config => RegisterOutcome::Unchanged,
            Some(entry) => {
                entry.config = config;
                entry.version = entry.version.wrapping_add(1);
                tracing::info!(
                    %name,
                    version = entry.version,
                    "cluster config changed; producer will switch generation on next use"
                );
                RegisterOutcome::Updated
            }
            None => {
                map.insert(
                    name,
                    ClusterEntry {
                        config,
                        token: Arc::new(StdMutex::new(None)),
                        version: 1,
                    },
                );
                RegisterOutcome::Added
            }
        }
    }

    pub async fn get(&self, name: &str) -> Option<ClusterConfig> {
        self.clusters
            .read()
            .await
            .get(name)
            .map(|e| e.config.clone())
    }

    /// P1 #7：同时取回当前 config 和 version。Producer/Consumer 用 version
    /// 做缓存键比对，stale 时 rebuild。
    pub async fn get_with_version(&self, name: &str) -> Option<(ClusterConfig, u64)> {
        self.clusters
            .read()
            .await
            .get(name)
            .map(|e| (e.config.clone(), e.version))
    }

    pub async fn exists(&self, name: &str) -> bool {
        self.clusters.read().await.contains_key(name)
    }

    pub async fn names(&self) -> Vec<String> {
        self.clusters.read().await.keys().cloned().collect()
    }

    /// 拿到集群的 OAuth token slot 句柄（Arc clone）。
    /// 供 Producer / Consumer 在创建 librdkafka 客户端时把句柄塞进 ClientContext。
    pub async fn token_slot_for(&self, name: &str) -> Option<OAuthTokenSlot> {
        self.clusters
            .read()
            .await
            .get(name)
            .map(|e| e.token.clone())
    }

    /// 写入或覆盖某 cluster 的 OAuth token。返回该 slot 的 Arc 句柄
    /// 供调用方记录到日志或后续比对。集群未注册时返回 `Err`。
    pub async fn set_oauth_token(&self, name: &str, token: StoredOAuthToken) -> Result<(), String> {
        let map = self.clusters.read().await;
        let entry = map
            .get(name)
            .ok_or_else(|| format!("cluster '{name}' not registered"))?;
        let mut slot = entry.token.lock().unwrap_or_else(|e| e.into_inner());
        *slot = Some(token);
        Ok(())
    }

    /// 测试 / 调试用
    #[allow(dead_code)]
    pub async fn len(&self) -> usize {
        self.clusters.read().await.len()
    }

    #[allow(dead_code)]
    pub async fn is_empty(&self) -> bool {
        self.clusters.read().await.is_empty()
    }
}

pub type ClusterRegistryHandle = Arc<ClusterRegistry>;

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(servers: &str) -> ClusterConfig {
        let mut m = HashMap::new();
        m.insert("bootstrap.servers".into(), servers.into());
        m
    }

    fn token(v: &str) -> StoredOAuthToken {
        StoredOAuthToken {
            token_value: v.into(),
            lifetime_ms: 1_000_000_000,
            principal_name: "p".into(),
            extensions: vec![],
        }
    }

    #[tokio::test]
    async fn test_register_then_get() {
        let r = ClusterRegistry::new();
        let outcome = r.register("main".into(), cfg("kafka:9092")).await;
        assert_eq!(outcome, RegisterOutcome::Added);

        let got = r.get("main").await.unwrap();
        assert_eq!(got.get("bootstrap.servers").unwrap(), "kafka:9092");
    }

    #[tokio::test]
    async fn test_register_overwrite() {
        let r = ClusterRegistry::new();
        r.register("c".into(), cfg("a:9092")).await;
        let outcome = r.register("c".into(), cfg("b:9092")).await;
        assert_eq!(outcome, RegisterOutcome::Updated);

        assert_eq!(
            r.get("c").await.unwrap().get("bootstrap.servers").unwrap(),
            "b:9092"
        );
    }

    #[tokio::test]
    async fn test_register_identical_config_keeps_version() {
        let r = ClusterRegistry::new();
        r.register("c".into(), cfg("a:9092")).await;
        let (_, v1) = r.get_with_version("c").await.unwrap();

        let outcome = r.register("c".into(), cfg("a:9092")).await;
        assert_eq!(outcome, RegisterOutcome::Unchanged);
        let (_, v2) = r.get_with_version("c").await.unwrap();
        assert_eq!(
            v1, v2,
            "identical re-registration must not rebuild the producer"
        );

        r.register("c".into(), cfg("b:9092")).await;
        let (_, v3) = r.get_with_version("c").await.unwrap();
        assert_eq!(v3, v2 + 1);
    }

    #[tokio::test]
    async fn test_multiple_clusters_isolated() {
        let r = ClusterRegistry::new();
        r.register("main".into(), [("k".into(), "v1".into())].into())
            .await;
        r.register("audit".into(), [("k".into(), "v2".into())].into())
            .await;
        assert_eq!(r.len().await, 2);
        assert_eq!(r.get("main").await.unwrap().get("k").unwrap(), "v1");
        assert_eq!(r.get("audit").await.unwrap().get("k").unwrap(), "v2");
    }

    #[tokio::test]
    async fn test_oauth_token_set_and_read() {
        let r = ClusterRegistry::new();
        r.register("msk".into(), cfg("msk:9094")).await;

        let slot = r.token_slot_for("msk").await.unwrap();
        assert!(slot.lock().unwrap().is_none(), "新 slot 应为空");

        r.set_oauth_token("msk", token("jwt-1")).await.unwrap();

        let got = slot.lock().unwrap().clone().unwrap();
        assert_eq!(got.token_value, "jwt-1");
    }

    #[tokio::test]
    async fn test_oauth_token_overwrite() {
        let r = ClusterRegistry::new();
        r.register("msk".into(), cfg("msk:9094")).await;
        r.set_oauth_token("msk", token("jwt-1")).await.unwrap();
        r.set_oauth_token("msk", token("jwt-2")).await.unwrap();

        let got = r.token_slot_for("msk").await.unwrap();
        assert_eq!(got.lock().unwrap().as_ref().unwrap().token_value, "jwt-2");
    }

    #[tokio::test]
    async fn test_oauth_token_unknown_cluster() {
        let r = ClusterRegistry::new();
        let err = r.set_oauth_token("nope", token("x")).await.unwrap_err();
        assert!(err.contains("not registered"));
    }

    #[tokio::test]
    async fn test_register_overwrite_keeps_token_slot() {
        // token slot 是 per-cluster 共享 Arc；config 覆盖不应让已注入到 librdkafka
        // 的 slot 失效。验证：同一 cluster name 再 register 后，token_slot 是同一 Arc。
        let r = ClusterRegistry::new();
        r.register("c".into(), cfg("a:9092")).await;
        let slot1 = r.token_slot_for("c").await.unwrap();

        r.register("c".into(), cfg("b:9092")).await;
        let slot2 = r.token_slot_for("c").await.unwrap();

        assert!(
            Arc::ptr_eq(&slot1, &slot2),
            "token slot 应在 config 覆盖后保留"
        );

        // 通过任一句柄写入，另一句柄都能看到
        r.set_oauth_token("c", token("jwt-x")).await.unwrap();
        assert_eq!(slot1.lock().unwrap().as_ref().unwrap().token_value, "jwt-x");
    }
}
