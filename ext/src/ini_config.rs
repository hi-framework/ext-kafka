//! `php.ini` 配置入口。
//!
//! 注册 4 个 ini 项给运维侧用，并在 fork worker 之前把它们解析出来：
//!
//! | ini 名                       | 类型   | 默认值  | 等价 env                    |
//! |------------------------------|--------|---------|-----------------------------|
//! | `hi_kafka.log_level`         | string | `info`  | `HI_KAFKA_LOG_LEVEL`        |
//! | `hi_kafka.log_file`          | string | (空)    | `HI_KAFKA_LOG_FILE`         |
//! | `hi_kafka.drain_timeout_ms`  | int    | `10000` | `HI_KAFKA_DRAIN_TIMEOUT_MS` |
//! | `hi_kafka.metrics_addr`      | string | (空=关) | `HI_KAFKA_METRICS_ADDR`     |
//!
//! 优先级：**env > ini > 内置默认**。env 显式给值时一律生效（兼容旧部署 & 方便
//! 容器/k8s 覆盖），否则用 ini，最后兜底默认。
//!
//! 权限：全部 `IniEntryPermission::All`（PHP_INI_ALL），允许业务通过 `-d` 或
//! `ini_set()` 改——但生效时机仅在 MINIT/worker fork 阶段读取，运行中改无效。

use ext_php_rs::flags::IniEntryPermission;
use ext_php_rs::zend::{ExecutorGlobals, IniEntryDef};

pub const INI_LOG_LEVEL: &str = "hi_kafka.log_level";
pub const INI_LOG_FILE: &str = "hi_kafka.log_file";
pub const INI_DRAIN_TIMEOUT_MS: &str = "hi_kafka.drain_timeout_ms";
pub const INI_METRICS_ADDR: &str = "hi_kafka.metrics_addr";

pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const DEFAULT_LOG_FILE: &str = "";
pub const DEFAULT_DRAIN_TIMEOUT_MS: u64 = 10_000;
pub const DEFAULT_METRICS_ADDR: &str = "";

/// MINIT 期注册三个 ini 项。
pub fn register(module_number: i32) {
    IniEntryDef::register(
        vec![
            IniEntryDef::new(
                INI_LOG_LEVEL.into(),
                DEFAULT_LOG_LEVEL.into(),
                IniEntryPermission::All,
            ),
            IniEntryDef::new(
                INI_LOG_FILE.into(),
                DEFAULT_LOG_FILE.into(),
                IniEntryPermission::All,
            ),
            IniEntryDef::new(
                INI_DRAIN_TIMEOUT_MS.into(),
                DEFAULT_DRAIN_TIMEOUT_MS.to_string(),
                IniEntryPermission::All,
            ),
            IniEntryDef::new(
                INI_METRICS_ADDR.into(),
                DEFAULT_METRICS_ADDR.into(),
                IniEntryPermission::All,
            ),
        ],
        module_number,
    );
}

/// 取一个 ini 项的字符串值。未注册或空字符串 → `None`。
fn ini_str(name: &str) -> Option<String> {
    let map = ExecutorGlobals::get().ini_values();
    map.get(name)
        .and_then(|v| v.clone())
        .filter(|s| !s.is_empty())
}

/// env > ini > default 的字符串解析。
fn resolve_str(env: &str, ini: &str, default: &str) -> String {
    if let Ok(v) = std::env::var(env) {
        if !v.is_empty() {
            return v;
        }
    }
    ini_str(ini).unwrap_or_else(|| default.to_string())
}

/// env > ini > default 的可选字符串（None 表示既未配置也无默认）。
fn resolve_opt_str(env: &str, ini: &str) -> Option<String> {
    if let Ok(v) = std::env::var(env) {
        if !v.is_empty() {
            return Some(v);
        }
    }
    ini_str(ini)
}

pub fn log_level() -> String {
    resolve_str("HI_KAFKA_LOG_LEVEL", INI_LOG_LEVEL, DEFAULT_LOG_LEVEL)
}

pub fn log_file() -> Option<std::path::PathBuf> {
    resolve_opt_str("HI_KAFKA_LOG_FILE", INI_LOG_FILE).map(std::path::PathBuf::from)
}

pub fn drain_timeout_ms() -> u64 {
    if let Ok(v) = std::env::var("HI_KAFKA_DRAIN_TIMEOUT_MS") {
        if let Ok(n) = v.parse::<u64>() {
            return n;
        }
    }
    ini_str(INI_DRAIN_TIMEOUT_MS)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_DRAIN_TIMEOUT_MS)
}

/// Prometheus `/metrics` 监听地址。默认关闭：env 与 ini 都为空时返回 `None`，
/// worker 不启动 metrics endpoint；显式给一个 `host:port` 才开。
pub fn metrics_addr() -> Option<std::net::SocketAddr> {
    resolve_opt_str("HI_KAFKA_METRICS_ADDR", INI_METRICS_ADDR).and_then(|s| s.parse().ok())
}
