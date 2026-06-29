//! `Hi\Kafka\ClientInterface` —— 给业务侧统一 type-hint 的 **marker interface**。
//!
//! 设计取舍：注册成**空 interface**（仅 `ClassFlags::Interface`，不挂任何 abstract method），
//! `Hi\Kafka\Client` 通过 `#[implements]` 真挂关系。这样三件事同时成立：
//!
//! 1. PHP 端 `$x instanceof \Hi\Kafka\ClientInterface` 在扩展类实例上 **真返回 true**
//!    —— 业务侧 `KafkaManager` / `AbstractProducer` / `AbstractConsumer` 能用接口作 type hint。
//! 2. PHP-side 实现 `Hi\Kafka\SwooleClient` / `SwowClient` 同样挂 `implements` 即可——
//!    扩展端不挂 abstract method，runtime 不强校验方法签名，三个实现各自的
//!    `$timeoutMs` 默认值 / 参数顺序差异**不会触发 LSP 兼容错误**。
//! 3. IDE / PHPStan / Psalm 走 PHP 侧桩 `php-driver/src/Hi/Kafka/ClientInterface.php`
//!    —— 那里写完整方法签名供静态分析，runtime 侧用 `interface_exists(..., false)`
//!    guard 让位给扩展注册的 interface。
//!
//! 注册时机：必须在 `Hi\Kafka\Client` 类注册**之前**完成，因为 `#[implements]`
//! 在 startup 期间引用本模块导出的 `get_ce()`。`lib.rs` 用
//! `#[php_startup(before)]` 把 `register()` 调用放到 ext-php-rs 自动注册类**之前**。

use ext_php_rs::builders::ClassBuilder;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::zend::ClassEntry;
use std::sync::OnceLock;

/// Newtype 包 `&'static ClassEntry` 加 `Sync` —— Zend ClassEntry 含 raw pointer
/// （bindgen union 推不出 `Sync`），但 PHP 类 entry 在 MINIT 完成后**只读**、
/// 生命周期等于模块本身（即 `'static`），跨线程读取是安全的。
struct CeRef(&'static ClassEntry);
// SAFETY: see CeRef doc
unsafe impl Sync for CeRef {}
unsafe impl Send for CeRef {}

static IFACE_CE: OnceLock<CeRef> = OnceLock::new();

/// 返回已注册的 `ClientInterface` ClassEntry。
///
/// **必须**在 `register()` 之后才有意义；在 `#[implements]` 表达式里用，
/// 由 ext-php-rs 在 module startup 阶段 evaluate（此时 `register()` 已跑过）。
///
/// # Panics
///
/// 如果在 `register()` 调用前被求值会 panic——但在正常 startup 路径下
/// （`#[php_startup(before)]` → `register()` → class 注册）不会发生。
pub fn get_ce() -> &'static ClassEntry {
    IFACE_CE
        .get()
        .map(|r| r.0)
        .expect("ClientInterface CE accessed before register()")
}

/// MINIT 阶段注册接口。幂等：重复调用只生效一次。
pub fn register() {
    if IFACE_CE.get().is_some() {
        return;
    }
    let ce = ClassBuilder::new("Hi\\Kafka\\ClientInterface")
        .flags(ClassFlags::Interface)
        .build()
        .expect("failed to register Hi\\Kafka\\ClientInterface");
    let _ = IFACE_CE.set(CeRef(ce));
}
