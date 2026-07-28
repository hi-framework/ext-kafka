//! Runtime marker interface for all protocol-v2 consumer stream drivers.
//!
//! The complete PHP signatures live in php-driver. The extension registers the
//! marker before its internal ConsumerStream class so native and coroutine
//! implementations share one stable type identity without Zend LSP issues.

use ext_php_rs::builders::ClassBuilder;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::zend::ClassEntry;
use std::sync::OnceLock;

struct CeRef(&'static ClassEntry);
// SAFETY: a class entry is immutable after MINIT and lives for the module.
unsafe impl Sync for CeRef {}
unsafe impl Send for CeRef {}

static IFACE_CE: OnceLock<CeRef> = OnceLock::new();

pub fn get_ce() -> &'static ClassEntry {
    IFACE_CE
        .get()
        .map(|entry| entry.0)
        .expect("ConsumerStreamInterface CE accessed before register()")
}

pub fn register() {
    if IFACE_CE.get().is_some() {
        return;
    }
    ClassBuilder::new("Hi\\Kafka\\ConsumerStreamInterface")
        .flags(ClassFlags::Interface)
        .registration(|ce| {
            let _ = IFACE_CE.set(CeRef(ce));
        })
        .register()
        .expect("failed to register Hi\\Kafka\\ConsumerStreamInterface");
}
