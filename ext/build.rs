fn main() {
    // PHP extension symbols (emalloc, zend_*, etc.) are resolved at runtime
    // when PHP `dlopen`s the shared library. The linker must be told to
    // allow undefined symbols.
    //
    // `cargo test --lib` 会把 crate 链成可执行文件，那里没有 PHP 运行时：
    // - 允许未解析符号，并关闭 chained fixups，让 Zend 函数恢复惰性绑定——纯 Rust
    //   单测永远不会调用它们；
    // - Zend 数据符号在加载时必须存在，由 `lib.rs` 的 `zend_test_symbols`（仅 cfg(test)）
    //   提供占位定义。
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    match target_os.as_str() {
        "macos" => {
            // `rustc-link-arg` 同时作用于 cdylib 与 lib 单测可执行文件
            // （`rustc-link-arg-tests` 只覆盖集成测试目标）。
            println!("cargo:rustc-link-arg=-Wl,-undefined,dynamic_lookup");
            println!("cargo:rustc-link-arg=-Wl,-no_fixup_chains");
        }
        "linux" => {
            // 共享库默认允许未解析符号；测试可执行文件需要显式放开。
            println!("cargo:rustc-link-arg=-Wl,--unresolved-symbols=ignore-all");
        }
        _ => {}
    }
}
