//! 同一 socket 路径上只能有一个 worker：残留的 socket 文件可以清理，正在服务的
//! socket 绝不能被第二个 worker 抢占。

use std::path::PathBuf;
use std::time::Duration;

fn temp_socket(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("hi-kafka-bind-{}-{name}.sock", std::process::id()))
}

#[tokio::test]
async fn live_socket_is_never_taken_over() {
    let socket = temp_socket("live");
    let _ = std::fs::remove_file(&socket);
    let first = hi_kafka_worker::Server::bind(&socket).await.unwrap();
    tokio::spawn(first.run(Duration::from_secs(1)));

    let second = hi_kafka_worker::Server::bind(&socket).await;
    assert!(second.is_err(), "second worker must refuse a live socket");
    assert!(
        tokio::net::UnixStream::connect(&socket).await.is_ok(),
        "the original worker must keep serving"
    );
    let _ = std::fs::remove_file(&socket);
}

#[tokio::test]
async fn stale_socket_file_is_replaced() {
    let socket = temp_socket("stale");
    let _ = std::fs::remove_file(&socket);
    // 绑定后立即丢弃监听者：只留下无人监听的 socket 文件。
    drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
    assert!(socket.exists());

    let server = hi_kafka_worker::Server::bind(&socket).await;
    assert!(
        server.is_ok(),
        "stale socket must be cleaned up: {:?}",
        server.err()
    );
    let _ = std::fs::remove_file(&socket);
}

/// 旧 worker 排干退出时只能删除自己创建的 socket 文件：drain 期间新 worker 可能已经
/// 在同一路径重新 bind，误删会让新 worker 不可达并被重复拉起。
#[tokio::test]
async fn exiting_worker_only_removes_its_own_socket_file() {
    let socket = temp_socket("owned");
    let _ = std::fs::remove_file(&socket);
    let old = hi_kafka_worker::Server::bind(&socket).await.unwrap();
    let old_identity = old.socket_identity();
    drop(old); // 旧 worker 停止监听（drain 中）

    let replacement = hi_kafka_worker::Server::bind(&socket).await.unwrap();
    assert!(!hi_kafka_worker::server::remove_socket_if_owned(
        &socket,
        old_identity
    ));
    assert!(socket.exists(), "replacement worker's socket must survive");

    assert!(hi_kafka_worker::server::remove_socket_if_owned(
        &socket,
        replacement.socket_identity()
    ));
    assert!(!socket.exists());
}
