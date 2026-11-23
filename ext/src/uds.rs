//! 带超时的 Unix domain socket 连接。
//!
//! `std::os::unix::net::UnixStream::connect` 没有超时：worker 冻结或卡死时不再
//! accept，监听队列满后 `connect()` 会无限期阻塞，PHP 进程（包括就绪探测）随之挂死。
//! 这里用非阻塞 connect：队列满时 Linux 返回 `EAGAIN`，在截止时间内重试；返回
//! `EINPROGRESS` 时 poll 可写后检查 `SO_ERROR`。

use std::io;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

/// 队列满时两次尝试之间的间隔。
const RETRY_INTERVAL: Duration = Duration::from_millis(5);

/// 在 `timeout` 内连接 `path`；超时返回 `ErrorKind::TimedOut`。返回的流为阻塞模式。
pub fn connect(path: &Path, timeout: Duration) -> io::Result<UnixStream> {
    let (addr, len) = socket_addr(path)?;
    let deadline = Instant::now() + timeout;
    loop {
        // SAFETY: socket() 返回新的 fd，立即交给 UnixStream 管理生命周期。
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let stream = unsafe { UnixStream::from_raw_fd(fd) };
        // 不让 fork 出的子进程（例如 PHP 的 proc_open）继承连接。
        unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
        stream.set_nonblocking(true)?;

        // SAFETY: addr / len 由 socket_addr 按 sockaddr_un 布局构造。
        let rc = unsafe {
            libc::connect(
                fd,
                (&addr as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
                len,
            )
        };
        if rc == 0 {
            stream.set_nonblocking(false)?;
            return Ok(stream);
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINPROGRESS) => {
                wait_writable(&stream, deadline)?;
                stream.set_nonblocking(false)?;
                return Ok(stream);
            }
            // 监听队列已满：worker 没有在 accept。截止时间内用新 socket 重试。
            Some(libc::EAGAIN) => {
                if Instant::now() >= deadline {
                    return Err(timed_out(path));
                }
                std::thread::sleep(RETRY_INTERVAL);
            }
            _ => return Err(error),
        }
    }
}

fn wait_writable(stream: &UnixStream, deadline: Instant) -> io::Result<()> {
    let mut poll = libc::pollfd {
        fd: stream.as_raw_fd(),
        events: libc::POLLOUT,
        revents: 0,
    };
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "unix socket connect timed out",
            ));
        }
        let millis = remaining.as_millis().clamp(1, i32::MAX as u128) as libc::c_int;
        // SAFETY: 单个有效 pollfd。
        let rc = unsafe { libc::poll(&mut poll, 1, millis) };
        if rc < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if rc == 0 {
            continue;
        }
        return match stream.take_error()? {
            None => Ok(()),
            Some(error) => Err(error),
        };
    }
}

fn socket_addr(path: &Path) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    // SAFETY: sockaddr_un 是纯 C 结构，全零是合法初值。
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    let bytes = path.as_os_str().as_bytes();
    if bytes.len() >= addr.sun_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unix socket path is too long",
        ));
    }
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, src) in addr.sun_path.iter_mut().zip(bytes) {
        *dst = *src as libc::c_char;
    }
    let path_offset = std::mem::size_of::<libc::sockaddr_un>() - addr.sun_path.len();
    let len = (path_offset + bytes.len() + 1) as libc::socklen_t;
    Ok((addr, len))
}

fn timed_out(path: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "connect {} timed out: listen queue is full (worker not accepting)",
            path.display()
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("hi-kafka-uds-{}-{name}.sock", std::process::id()))
    }

    #[test]
    fn connects_to_an_accepting_listener() {
        let path = temp_path("ok");
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let stream = connect(&path, Duration::from_secs(1)).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        drop((stream, accepted));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn missing_socket_fails_fast() {
        let error = connect(&temp_path("missing"), Duration::from_secs(1)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    /// 监听方从不 accept 时，队列填满后的连接必须在超时内失败，而不是永久阻塞。
    #[test]
    fn full_listen_queue_times_out() {
        let path = temp_path("full");
        let _ = std::fs::remove_file(&path);
        let _listener = UnixListener::bind(&path).unwrap();
        let mut held = Vec::new();
        let started = Instant::now();
        let outcome = loop {
            match connect(&path, Duration::from_millis(200)) {
                Ok(stream) => held.push(stream),
                Err(error) => break error,
            }
            assert!(held.len() < 100_000, "listen queue never filled");
        };
        // macOS 在队列满时直接返回 ECONNREFUSED；Linux 返回 EAGAIN 并由这里转为超时。
        assert!(
            matches!(
                outcome.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::ConnectionRefused
            ),
            "unexpected error {outcome:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(30));
        let _ = std::fs::remove_file(&path);
    }
}
