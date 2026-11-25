//! 本机其它用户的隔离。
//!
//! 默认 socket 在 `/tmp`，同一台机器上的任何用户都能连接或抢先占用这个路径：
//!
//! - worker 只服务与自己同 uid 的进程（以及 root）：其它用户不能借用本 worker 已
//!   注册集群的 Kafka 权限去生产或消费。
//! - 客户端只连接与自己同 uid 的 worker（以及 root 的 worker）：其它用户抢先在该
//!   路径上监听时，不会把 SASL 凭据和消息发给它。root 客户端信任任何 worker，
//!   兼容 root 在容器里执行命令、连接普通用户（例如 FPM 的 www-data）拉起的 worker。
//!
//! 不依赖 socket 文件的权限位：worker 继承拉起它的进程的 umask，权限位不可靠。

use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

fn euid() -> u32 {
    // SAFETY: geteuid 没有失败路径。
    unsafe { libc::geteuid() }
}

/// worker 是否接受 uid 为 `uid` 的客户端。
pub fn accepts_client(uid: u32) -> bool {
    uid == euid() || uid == 0
}

/// 客户端是否信任 uid 为 `uid` 的 worker。
pub fn trusts_worker(uid: u32) -> bool {
    let own = euid();
    uid == own || uid == 0 || own == 0
}

/// 已连接 Unix socket 对端进程的 uid。
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn peer_uid(fd: BorrowedFd<'_>) -> io::Result<u32> {
    // SAFETY: ucred 是纯 C 结构，全零是合法初值；len 与其大小一致。
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(cred.uid)
}

/// 已连接 Unix socket 对端进程的 uid。
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub fn peer_uid(fd: BorrowedFd<'_>) -> io::Result<u32> {
    let mut uid: libc::uid_t = 0;
    let mut gid: libc::gid_t = 0;
    // SAFETY: fd 有效，uid / gid 是可写的输出参数。
    if unsafe { libc::getpeereid(fd.as_raw_fd(), &mut uid, &mut gid) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(uid)
}

/// 客户端连接对端 worker 之前检查路径上 socket 文件的属主（给无法取对端凭据的
/// 协程 driver 用）。路径不存在或无法 stat 时放行，由随后的 connect 报错。
pub fn check_socket_owner(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if !trusts_worker(meta.uid()) => Err(untrusted(path, meta.uid())),
        _ => Ok(()),
    }
}

/// 对端 worker 不可信时返回的错误。
pub fn untrusted(path: &Path, uid: u32) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!(
            "{} is served by uid {uid}, not by this user (uid {}) or root; refusing to connect",
            path.display(),
            euid()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsFd;
    use std::os::unix::net::UnixStream;

    #[test]
    fn peer_uid_of_own_socket_pair_is_trusted() {
        let (a, _b) = UnixStream::pair().unwrap();
        let uid = peer_uid(a.as_fd()).unwrap();
        assert_eq!(uid, euid());
        assert!(accepts_client(uid));
        assert!(trusts_worker(uid));
    }

    #[test]
    fn root_is_trusted_both_ways() {
        assert!(accepts_client(0));
        assert!(trusts_worker(0));
    }

    #[test]
    fn other_users_are_rejected_unless_self_is_root() {
        let other = euid().wrapping_add(4242).max(1);
        assert!(!accepts_client(other));
        assert_eq!(trusts_worker(other), euid() == 0);
    }

    #[test]
    fn socket_owner_check() {
        let dir = std::env::temp_dir();
        let missing = dir.join(format!("hi-kafka-peer-missing-{}", std::process::id()));
        assert!(check_socket_owner(&missing).is_ok());
        let own = dir.join(format!("hi-kafka-peer-own-{}", std::process::id()));
        std::fs::write(&own, b"").unwrap();
        assert!(check_socket_owner(&own).is_ok());
        std::fs::remove_file(&own).unwrap();
    }
}
