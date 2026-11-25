//! Worker 自启动：扩展首次需要 worker 时拉起一个节点级守护进程。worker 代码链接在
//! 本 .so 里，不需要额外的可执行文件。
//!
//! fork + exec：孙进程 fork 后立即 exec 一个只加载本扩展的 PHP CLI
//! （`php -n -d extension=<本 .so>`），新进程在 MINIT 中进入 worker（见
//! [`crate::worker_entry::exec_config_from_env`]）。fork 与 exec 之间只有
//! async-signal-safe 调用，exec 换掉整个地址空间，fork 时其它线程持有的锁随之作废：
//! Swoole / Swow / Workerman / ReactPHP / FPM 等运行时在任何时刻懒加载都是安全的，
//! 不需要业务在「启动线程之前」调用 ensureWorker。
//!
//! 回退：找不到 PHP CLI（可用 `hi_kafka.php_binary` 指定）或 exec 失败时，孙进程直接
//! 在 fork 出的进程中运行 worker；此时多线程进程中仍有继承锁死锁的风险，由下面的
//! 就绪超时 SIGKILL 兜底。
//!
//! 并发控制：`flock(LOCK_EX | LOCK_NB)` 锁住 `<socket>.spawn-lock` 文件。
//! 同 pod 内多个 PHP 进程并发到这一步时只有一个能拿到锁、负责 fork，
//! 其它等 socket 出现。
//!
//! 进程模型：double fork。中间进程只调用 async-signal-safe 的 `setsid` / `fork` /
//! `write` / `_exit`，经 pipe 回报孙进程 PID 后立即退出并被 spawner `waitpid` 回收；
//! worker（孙进程）从一开始就被 init 收养，不会成为 PHP-FPM / Swoole manager 的
//! 子进程（不留僵尸、不干扰它们的 SIGCHLD 处理）。worker 启动后先关闭全部继承的 fd。
//! exec 保持 PID 不变，中间进程回报的 PID 就是最终 worker 的 PID。
//!
//! 就绪判定：HELLO 握手成功才算就绪（connect 成功不够：可能是旧版本、半启动或
//! 卡死的 worker）。超时未就绪的孙进程会被 SIGKILL：保证最坏情况是一次有界的启动
//! 失败，而不是一个永远不就绪、却占着 socket namespace 的僵死进程。

use crate::protocol;
use crate::worker_entry::{self, ChildConfig, Launch};
use hi_kafka_proto::HEADER_LEN;
use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("fork failed: {0}")]
    ForkFailed(std::io::Error),

    #[error("worker did not become ready in {timeout_ms}ms")]
    NotReady { timeout_ms: u64 },

    #[error("create spawn lock file: {0}")]
    CreateLockFile(std::io::Error),

    /// 无法 exec（`reason`）而进程已有 {count} 个线程：不 exec 的子进程可能继承其它
    /// 线程持有的锁而死锁。仅在 `HI_KAFKA_STRICT_FORK=1` 时拒绝。
    #[error(
        "refusing to run the worker in a forked copy of a multi-threaded process \
         (threads={count}, HI_KAFKA_STRICT_FORK=1): exec unavailable: {reason}. \
         Set hi_kafka.php_binary to a PHP CLI binary"
    )]
    MultiThreadedFork { count: usize, reason: String },
}

/// 启动配置
pub struct SpawnConfig {
    pub socket: PathBuf,
    pub log_level: String,
    pub log_file: Option<PathBuf>,
    /// worker 收到 SIGTERM 后排干 in-flight 连接的超时
    pub drain_timeout: Duration,
    /// 等待 socket 就绪的总超时
    pub ready_timeout: Duration,
}

impl SpawnConfig {
    /// 解析配置：先读 env / php.ini（env > ini > default），fork 前父进程一次性算出来。
    pub fn from_env(socket: PathBuf) -> Self {
        Self {
            socket,
            log_level: crate::ini_config::log_level(),
            log_file: crate::ini_config::log_file(),
            drain_timeout: Duration::from_millis(crate::ini_config::drain_timeout_ms()),
            ready_timeout: Duration::from_secs(5),
        }
    }

    fn to_child_config(&self, launch: Launch) -> ChildConfig {
        ChildConfig {
            socket: self.socket.clone(),
            log_level: self.log_level.clone(),
            log_file: self.log_file.clone(),
            drain_timeout: self.drain_timeout,
            metrics_addr: crate::ini_config::metrics_addr(),
            launch,
        }
    }
}

/// L: 区分 worker 已活 vs 本次刚 spawn，让上层决定是否要重放 cluster 注册。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpawnOutcome {
    AlreadyAlive,
    JustSpawned,
}

/// 单次探测的 IO 上限。worker 正常时 HELLO 是亚毫秒级。
const PROBE_TIMEOUT: Duration = Duration::from_millis(300);

/// 检测 worker 是否可用：connect 并完成 protocol HELLO。
pub fn worker_alive(socket: &Path) -> bool {
    handshake(socket, PROBE_TIMEOUT).is_some()
}

/// 直连 socket 并完成 HELLO 握手（RPC 角色）。任一步失败返回 `None`。
pub fn handshake(socket: &Path, timeout: Duration) -> Option<UnixStream> {
    handshake_with_capabilities(socket, timeout).map(|(stream, _)| stream)
}

/// 同 [`handshake`]，另返回 worker 声明的 capabilities。
pub fn handshake_with_capabilities(socket: &Path, timeout: Duration) -> Option<(UnixStream, u64)> {
    let mut stream = crate::uds::connect(socket, timeout).ok()?;
    stream.set_read_timeout(Some(timeout)).ok()?;
    stream.set_write_timeout(Some(timeout)).ok()?;

    let hello = protocol::build_hello_frame().ok()?;
    stream.write_all(&hello).ok()?;

    let mut header = [0u8; HEADER_LEN];
    stream.read_exact(&mut header).ok()?;
    let payload_len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
    // HELLO RESP payload 只有几个字节；异常大说明对端不是本协议的 worker。
    if payload_len > 64 {
        return None;
    }
    let mut full = vec![0u8; HEADER_LEN + payload_len];
    full[..HEADER_LEN].copy_from_slice(&header);
    stream.read_exact(&mut full[HEADER_LEN..]).ok()?;
    let capabilities = protocol::parse_hello_resp(&full).ok()?;
    Some((stream, capabilities))
}

/// 主入口：保证 socket 后面有一个能响应的 worker。
pub fn ensure_worker(cfg: &SpawnConfig) -> Result<SpawnOutcome, SpawnError> {
    // Fast path
    if worker_alive(&cfg.socket) {
        return Ok(SpawnOutcome::AlreadyAlive);
    }

    // 准备 lock 文件路径
    let lock_path = lock_path_for(&cfg.socket);
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent).map_err(SpawnError::CreateLockFile)?;
    }
    let lock_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(SpawnError::CreateLockFile)?;

    // 非阻塞独占锁
    let got_lock = try_flock_ex_nb(&lock_file);

    let deadline = Instant::now() + cfg.ready_timeout;
    if got_lock {
        // 拿锁后再探测一次：可能在我们等锁之前别的进程刚完成启动。
        if worker_alive(&cfg.socket) {
            return Ok(SpawnOutcome::AlreadyAlive);
        }
        let pid = spawn_worker(cfg)?;
        if let Err(error) = wait_for_socket(&cfg.socket, deadline) {
            // 未就绪的 worker 可能已经死锁；杀掉它，让下一次 ensure 干净地重试。
            if pid > 0 {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
            return Err(error);
        }
    } else {
        // 别的进程在 spawn，等 socket 就绪
        wait_for_socket(&cfg.socket, deadline)?;
    }

    drop(lock_file);
    Ok(SpawnOutcome::JustSpawned)
}

fn lock_path_for(socket: &Path) -> PathBuf {
    let mut p = socket.to_path_buf();
    let stem = p.file_name().map(|s| s.to_os_string()).unwrap_or_default();
    let mut new_name = stem;
    new_name.push(".spawn-lock");
    p.set_file_name(new_name);
    p
}

fn try_flock_ex_nb(file: &File) -> bool {
    let fd = file.as_raw_fd();
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    rc == 0
}

/// double fork 出 worker，返回 worker（孙进程）的 PID；无法获知时返回 0。
///
/// 父：回收中间进程后返回，由调用方等待就绪
/// 孙：exec PHP CLI 进入 worker；exec 不可用或失败时直接跑
///     [`worker_entry::run_in_child`]。**永不返回**
fn spawn_worker(cfg: &SpawnConfig) -> Result<libc::pid_t, SpawnError> {
    // fork 之后的子进程不做任何分配：exec 参数、回退配置都在这里准备好。
    let plan = ExecPlan::prepare(cfg);
    let mut child_config = cfg.to_child_config(match &plan {
        Ok(_) => Launch::Exec,
        Err(reason) => Launch::InProcess {
            reason: reason.clone(),
        },
    });

    if let Err(reason) = &plan {
        // 不 exec 的子进程会在 fork 出的地址空间里运行 worker：多线程进程中其它线程
        // 持有的锁（malloc 等）可能永久不释放。默认允许并依赖「就绪超时即 SIGKILL」
        // 把最坏情况限定为一次有界失败；`HI_KAFKA_STRICT_FORK=1` 时严格拒绝。
        let n_threads = current_thread_count();
        if n_threads > 1 {
            let strict = std::env::var("HI_KAFKA_STRICT_FORK")
                .map(|v| v != "0" && !v.is_empty())
                .unwrap_or(false);
            if strict {
                return Err(SpawnError::MultiThreadedFork {
                    count: n_threads,
                    reason: reason.clone(),
                });
            }
            tracing::warn!(
                threads = n_threads,
                reason = %reason,
                "cannot exec a PHP CLI for the worker; running it in a forked copy of a \
                 multi-threaded process. A worker that inherits a held lock is killed after \
                 the ready timeout. Set hi_kafka.php_binary to enable exec."
            );
        }
    }

    let mut pipe_fds = [0 as libc::c_int; 2];
    if unsafe { libc::pipe(pipe_fds.as_mut_ptr()) } != 0 {
        return Err(SpawnError::ForkFailed(std::io::Error::last_os_error()));
    }
    let [read_fd, write_fd] = pipe_fds;

    let intermediate = unsafe { libc::fork() };
    if intermediate < 0 {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
        return Err(SpawnError::ForkFailed(error));
    }

    if intermediate == 0 {
        // === 中间进程：只允许 async-signal-safe 调用 ===
        unsafe {
            libc::close(read_fd);
            libc::setsid();
            let worker = libc::fork();
            if worker == 0 {
                libc::close(write_fd);
                if let Ok(plan) = &plan {
                    // 成功则不返回；fd 清理、信号复位由新进程的 run_in_child 完成。
                    libc::execve(plan.path.as_ptr(), plan.argv.as_ptr(), plan.envp.as_ptr());
                    child_config.launch = Launch::ExecFailed {
                        errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                    };
                }
                // 进入 worker，永不返回
                worker_entry::run_in_child(child_config);
            }
            let bytes = worker.to_ne_bytes();
            libc::write(write_fd, bytes.as_ptr().cast(), bytes.len());
            libc::_exit(if worker > 0 { 0 } else { 1 });
        }
    }

    // === 父进程 ===
    unsafe { libc::close(write_fd) };
    let mut status = 0;
    while unsafe { libc::waitpid(intermediate, &mut status, 0) } < 0
        && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
    {}
    let mut bytes = [0u8; std::mem::size_of::<libc::pid_t>()];
    let mut file = unsafe { <File as std::os::fd::FromRawFd>::from_raw_fd(read_fd) };
    let worker = match file.read_exact(&mut bytes) {
        Ok(()) => libc::pid_t::from_ne_bytes(bytes),
        Err(_) => 0,
    };
    if worker < 0 {
        return Err(SpawnError::ForkFailed(std::io::Error::other(
            "intermediate process failed to fork the worker",
        )));
    }
    Ok(worker)
}

/// fork 之前准备好的 execve 参数。指针数组引用本结构持有的 `CString`，fork 后的子进程
/// 拥有同一份内存拷贝，因此可以直接使用、无需分配。
struct ExecPlan {
    path: CString,
    argv: Vec<*const libc::c_char>,
    envp: Vec<*const libc::c_char>,
    _strings: Vec<CString>,
}

impl ExecPlan {
    /// 解析 PHP CLI 与本 .so 的路径并构造 argv / envp。失败返回原因（用于回退日志）。
    fn prepare(cfg: &SpawnConfig) -> Result<Self, String> {
        let php = php_cli_binary()?;
        let extension = extension_path()?;
        let socket = cfg.socket.as_os_str();

        let mut extension_arg = OsString::from("extension=");
        extension_arg.push(extension.as_os_str());
        // argv[0] 让 ps 中能认出 worker；`-r ''` 只在 MINIT 未接管时生效（直接退出）；
        // `--` 之后附上 socket 路径，便于按 socket 路径 pgrep（scripts/cleanup-ghosts.sh）。
        let args: Vec<&OsStr> = vec![
            OsStr::new("hi-kafka-worker"),
            OsStr::new("-n"),
            OsStr::new("-d"),
            extension_arg.as_os_str(),
            OsStr::new("-r"),
            OsStr::new(""),
            OsStr::new("--"),
            socket,
        ];

        let metrics = crate::ini_config::metrics_addr().map(|addr| addr.to_string());
        let overrides = [
            (
                worker_entry::EXEC_SOCKET_ENV,
                Some(socket.as_bytes().to_vec()),
            ),
            (
                "HI_KAFKA_LOG_LEVEL",
                Some(cfg.log_level.clone().into_bytes()),
            ),
            (
                "HI_KAFKA_LOG_FILE",
                cfg.log_file
                    .as_ref()
                    .map(|path| path.as_os_str().as_bytes().to_vec()),
            ),
            (
                "HI_KAFKA_DRAIN_TIMEOUT_MS",
                Some(cfg.drain_timeout.as_millis().to_string().into_bytes()),
            ),
            ("HI_KAFKA_METRICS_ADDR", metrics.map(String::into_bytes)),
        ];
        let env = worker_env(std::env::vars_os(), &overrides);

        let path = cstring(php.as_os_str().as_bytes())?;
        let mut strings = Vec::with_capacity(args.len() + env.len());
        let mut argv = Vec::with_capacity(args.len() + 1);
        for arg in args {
            let arg = cstring(arg.as_bytes())?;
            argv.push(arg.as_ptr());
            strings.push(arg);
        }
        argv.push(std::ptr::null());
        let mut envp = Vec::with_capacity(env.len() + 1);
        for entry in env {
            envp.push(entry.as_ptr());
            strings.push(entry);
        }
        envp.push(std::ptr::null());
        Ok(Self {
            path,
            argv,
            envp,
            _strings: strings,
        })
    }
}

fn cstring(bytes: &[u8]) -> Result<CString, String> {
    CString::new(bytes).map_err(|_| "exec argument contains a NUL byte".to_string())
}

/// worker 的环境：继承父进程环境（librdkafka / TLS / 分配器相关变量都需要），并用
/// 父进程已解析好的配置覆盖同名键；值为 `None` 的键被移除。
fn worker_env(
    inherited: impl IntoIterator<Item = (OsString, OsString)>,
    overrides: &[(&str, Option<Vec<u8>>)],
) -> Vec<CString> {
    let mut env: Vec<CString> = inherited
        .into_iter()
        .filter(|(key, _)| {
            !overrides
                .iter()
                .any(|(name, _)| key.as_bytes() == name.as_bytes())
        })
        .filter_map(|(key, value)| {
            let mut entry = key.as_bytes().to_vec();
            entry.push(b'=');
            entry.extend_from_slice(value.as_bytes());
            CString::new(entry).ok()
        })
        .collect();
    for (name, value) in overrides {
        if let Some(value) = value {
            let mut entry = name.as_bytes().to_vec();
            entry.push(b'=');
            entry.extend_from_slice(value);
            if let Ok(entry) = CString::new(entry) {
                env.push(entry);
            }
        }
    }
    env
}

/// 用来 exec worker 的 PHP CLI：显式配置优先；否则当前进程本身是 PHP CLI 时用它自己
/// （与当前扩展同一构建，ABI 一定匹配）；FPM 等其它 SAPI 时找同一安装下的 `php`。
fn php_cli_binary() -> Result<PathBuf, String> {
    if let Some(path) = crate::ini_config::php_binary() {
        return if is_executable(&path) {
            Ok(path)
        } else {
            Err(format!(
                "hi_kafka.php_binary {} is not an executable file",
                path.display()
            ))
        };
    }
    let exe = std::env::current_exe().map_err(|e| format!("resolve current executable: {e}"))?;
    php_cli_candidates(&exe)
        .into_iter()
        .find(|path| is_executable(path))
        .ok_or_else(|| {
            format!(
                "no PHP CLI binary found for {}; set hi_kafka.php_binary",
                exe.display()
            )
        })
}

fn php_cli_candidates(exe: &Path) -> Vec<PathBuf> {
    if exe
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(is_php_cli_name)
    {
        return vec![exe.to_path_buf()];
    }
    let mut candidates = Vec::new();
    if let Some(dir) = exe.parent() {
        // 同目录（/usr/bin/php-fpm8.2 旁的 php）与同前缀的 bin（/usr/local/sbin/php-fpm
        // 对应 /usr/local/bin/php）。
        candidates.push(dir.join("php"));
        if let Some(prefix) = dir.parent() {
            candidates.push(prefix.join("bin").join("php"));
        }
    }
    candidates
}

/// `php`、`php8.2`、`php82` 是 CLI；`php-fpm`、`php-cgi`、`phpdbg` 不是。
fn is_php_cli_name(name: &str) -> bool {
    name.strip_prefix("php")
        .is_some_and(|rest| rest.chars().all(|c| c.is_ascii_digit() || c == '.'))
}

fn is_executable(path: &Path) -> bool {
    let Ok(c_path) = CString::new(path.as_os_str().as_bytes()) else {
        return false;
    };
    path.is_file() && unsafe { libc::access(c_path.as_ptr(), libc::X_OK) } == 0
}

/// 本扩展 .so 的绝对路径（`dladdr` 查询本模块内一个函数的地址）。
fn extension_path() -> Result<PathBuf, String> {
    let mut info: libc::Dl_info = unsafe { std::mem::zeroed() };
    let address = extension_path as *const () as *const libc::c_void;
    if unsafe { libc::dladdr(address, &mut info) } == 0 || info.dli_fname.is_null() {
        return Err("dladdr could not locate the extension".to_string());
    }
    let path = OsStr::from_bytes(unsafe { CStr::from_ptr(info.dli_fname) }.to_bytes());
    std::fs::canonicalize(path)
        .map_err(|e| format!("resolve extension path {}: {e}", Path::new(path).display()))
}

/// 当前进程的线程数。各 OS 拿数据来源不同：
/// - Linux: `/proc/self/status` 的 `Threads:` 行
/// - macOS: mach `task_threads()` 取 thread port 数组长度
/// - 其他 / 失败：返回 1（按"单线程"处理，不阻塞）
fn current_thread_count() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                if let Some(rest) = line.strip_prefix("Threads:") {
                    if let Ok(n) = rest.trim().parse::<usize>() {
                        return n.max(1);
                    }
                }
            }
        }
        1
    }
    #[cfg(target_os = "macos")]
    {
        // 用 libproc.h::proc_pidinfo(PROC_PIDTASKINFO) 拿 pti_threadnum。
        // 这是 libSystem 公开符号，链接稳定，不依赖 mach 内部 ABI。
        unsafe extern "C" {
            fn proc_pidinfo(
                pid: libc::pid_t,
                flavor: libc::c_int,
                arg: u64,
                buffer: *mut libc::c_void,
                buffersize: libc::c_int,
            ) -> libc::c_int;
        }
        const PROC_PIDTASKINFO: libc::c_int = 4;
        #[repr(C)]
        #[derive(Default)]
        struct ProcTaskInfo {
            virtual_size: u64,
            resident_size: u64,
            total_user: u64,
            total_system: u64,
            threads_user: u64,
            threads_system: u64,
            policy: i32,
            faults: i32,
            pageins: i32,
            cow_faults: i32,
            messages_sent: i32,
            messages_received: i32,
            syscalls_mach: i32,
            syscalls_unix: i32,
            csw: i32,
            threadnum: i32,
            numrunning: i32,
            priority: i32,
        }
        let mut info = ProcTaskInfo::default();
        let size = std::mem::size_of::<ProcTaskInfo>() as libc::c_int;
        let r = unsafe {
            proc_pidinfo(
                libc::getpid(),
                PROC_PIDTASKINFO,
                0,
                &mut info as *mut _ as *mut libc::c_void,
                size,
            )
        };
        if r > 0 {
            (info.threadnum as usize).max(1)
        } else {
            1
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        1
    }
}

fn wait_for_socket(socket: &Path, deadline: Instant) -> Result<(), SpawnError> {
    while Instant::now() < deadline {
        if worker_alive(socket) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(30));
    }
    Err(SpawnError::NotReady {
        timeout_ms: deadline.elapsed().as_millis() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_path_for() {
        let p = lock_path_for(Path::new("/tmp/hi-kafka-v2.sock"));
        assert_eq!(p, PathBuf::from("/tmp/hi-kafka-v2.sock.spawn-lock"));
    }

    #[test]
    fn php_cli_names() {
        for name in ["php", "php8.2", "php82"] {
            assert!(is_php_cli_name(name), "{name}");
        }
        for name in [
            "php-fpm",
            "php-fpm8.2",
            "php-cgi",
            "phpdbg",
            "hi-kafka-worker",
        ] {
            assert!(!is_php_cli_name(name), "{name}");
        }
    }

    #[test]
    fn php_cli_candidates_prefer_self_then_same_install() {
        assert_eq!(
            php_cli_candidates(Path::new("/usr/local/bin/php")),
            vec![PathBuf::from("/usr/local/bin/php")]
        );
        assert_eq!(
            php_cli_candidates(Path::new("/usr/local/sbin/php-fpm")),
            vec![
                PathBuf::from("/usr/local/sbin/php"),
                PathBuf::from("/usr/local/bin/php"),
            ]
        );
    }

    #[test]
    fn worker_env_overrides_and_removes_keys() {
        let inherited = vec![
            (OsString::from("PATH"), OsString::from("/usr/bin")),
            (
                OsString::from("HI_KAFKA_LOG_LEVEL"),
                OsString::from("trace"),
            ),
            (
                OsString::from("HI_KAFKA_LOG_FILE"),
                OsString::from("/old.log"),
            ),
        ];
        let env = worker_env(
            inherited,
            &[
                ("HI_KAFKA_LOG_LEVEL", Some(b"warn".to_vec())),
                ("HI_KAFKA_LOG_FILE", None),
                (worker_entry::EXEC_SOCKET_ENV, Some(b"/tmp/w.sock".to_vec())),
            ],
        );
        let env: Vec<&str> = env.iter().map(|e| e.to_str().unwrap()).collect();
        assert_eq!(
            env,
            vec![
                "PATH=/usr/bin",
                "HI_KAFKA_LOG_LEVEL=warn",
                "HI_KAFKA_INTERNAL_WORKER_SOCKET=/tmp/w.sock",
            ]
        );
    }

    #[test]
    fn extension_path_resolves_the_loaded_module() {
        // 单测里本模块链接在测试可执行文件中：dladdr 应返回一个存在的绝对路径。
        let path = extension_path().unwrap();
        assert!(path.is_absolute() && path.exists(), "{}", path.display());
    }

    #[test]
    fn test_worker_alive_false_for_missing() {
        assert!(!worker_alive(Path::new(
            "/tmp/definitely-does-not-exist.sock"
        )));
    }
}
