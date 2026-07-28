use bytes::BytesMut;
use hi_kafka_proto::{
    codec, encode_frame, ConnectionRole, ConsumeAck, ConsumeOpen, ConsumeReady, ConsumeResume,
    ConsumerPartitions, ErrorKind, ErrorResp, FrameType, HelloReq, ResumeToken, CAP_ALL,
    HEADER_LEN, PROTOCOL_MAJOR,
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

static SOCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_socket() -> PathBuf {
    let id = SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "hi-kafka-consumer-v2-{}-{id}.sock",
        std::process::id()
    ))
}

async fn send<T>(
    stream: &mut UnixStream,
    kind: FrameType,
    cid: u64,
    encode: impl FnOnce(&mut BytesMut) -> Result<T, hi_kafka_proto::PayloadError>,
) {
    let mut payload = BytesMut::new();
    encode(&mut payload).unwrap();
    let mut frame = BytesMut::new();
    encode_frame(kind, cid, &payload, &mut frame).unwrap();
    stream.write_all(&frame).await.unwrap();
}

async fn receive(stream: &mut UnixStream) -> (FrameType, u64, Vec<u8>) {
    let mut bytes = [0; HEADER_LEN];
    stream.read_exact(&mut bytes).await.unwrap();
    let header = codec::decode_header(&bytes).unwrap();
    let mut payload = vec![0; header.payload_len as usize];
    stream.read_exact(&mut payload).await.unwrap();
    (header.kind, header.cid, payload)
}

async fn connect(socket: &PathBuf) -> UnixStream {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    let hello = HelloReq {
        major: PROTOCOL_MAJOR,
        role: ConnectionRole::ConsumerStream,
        capabilities: CAP_ALL,
    };
    send(&mut stream, FrameType::Hello, 1, |b| hello.encode(b)).await;
    assert_eq!(receive(&mut stream).await.0, FrameType::Hello);
    stream
}

#[tokio::test]
async fn dedicated_stream_controls_resume_and_rejects_stale_epoch() {
    let socket = temp_socket();
    let server = hi_kafka_worker::Server::bind(&socket).await.unwrap();
    tokio::spawn(server.run(Duration::from_secs(30)));

    let mut stream = connect(&socket).await;
    let open = ConsumeOpen {
        cluster: "logging".into(),
        group_id: "group".into(),
        topics: vec!["events".into()],
        config: vec![],
        initial_message_credit: 1,
        initial_byte_credit: 1024,
        max_in_flight_messages: 1,
        max_in_flight_bytes: 1024,
        client_id: "test".into(),
    };
    send(&mut stream, FrameType::ConsumeOpen, 2, |b| open.encode(b)).await;
    let (kind, cid, payload) = receive(&mut stream).await;
    assert_eq!((kind, cid), (FrameType::ConsumeReady, 2));
    let ready = ConsumeReady::decode(&payload).unwrap();

    let partitions = ConsumerPartitions {
        subscription_id: ready.subscription_id,
        subscription_epoch: ready.subscription_epoch,
        partitions: vec![],
    };
    send(&mut stream, FrameType::ConsumePause, 0, |b| {
        partitions.encode(b)
    })
    .await;
    send(&mut stream, FrameType::ConsumePartitionResume, 0, |b| {
        partitions.encode(b)
    })
    .await;
    drop(stream);
    tokio::time::sleep(Duration::from_millis(30)).await;

    let mut resumed = connect(&socket).await;
    let resume = ConsumeResume {
        subscription_id: ready.subscription_id,
        previous_epoch: ready.subscription_epoch,
        resume_token: ResumeToken(ready.resume_token.0),
        initial_message_credit: 1,
        initial_byte_credit: 1024,
    };
    send(&mut resumed, FrameType::ConsumeResume, 3, |b| {
        resume.encode(b)
    })
    .await;
    let (kind, cid, payload) = receive(&mut resumed).await;
    assert_eq!((kind, cid), (FrameType::ConsumeReady, 3));
    let resumed_ready = ConsumeReady::decode(&payload).unwrap();
    assert_eq!(
        resumed_ready.subscription_epoch,
        ready.subscription_epoch + 1
    );

    let stale = ConsumeAck {
        subscription_id: ready.subscription_id,
        subscription_epoch: ready.subscription_epoch,
        delivery_id: 1,
        message_credit: 0,
        byte_credit: 0,
    };
    send(&mut resumed, FrameType::ConsumeAck, 4, |b| stale.encode(b)).await;
    let (kind, cid, payload) = receive(&mut resumed).await;
    assert_eq!((kind, cid), (FrameType::Error, 4));
    assert_eq!(
        ErrorResp::decode(&payload).unwrap().kind,
        ErrorKind::StaleEpoch
    );
}
