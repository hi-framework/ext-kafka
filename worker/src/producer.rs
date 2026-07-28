//! Kafka 生产者抽象。
//!
//! 默认编译只提供 [`LoggingProducer`]（dry-run，便于无 Kafka 环境开发）；
//! 启用 `kafka` feature 时编译 [`KafkaProducer`]，真正投递到 broker。

use hi_kafka_proto::{ProduceFnf, ProduceResp};
use std::sync::Arc;
use tracing::info;

#[cfg(feature = "kafka")]
mod real;

#[async_trait::async_trait]
pub trait Producer: Send + Sync {
    /// Fire-and-forget：成功入队即视为成功，不等 broker ack。
    async fn produce_fnf(&self, msg: ProduceFnf) -> anyhow::Result<()>;

    /// 同步带 ack：等 broker delivery report 后返回带 partition/offset 的 ProduceResp。
    /// 失败时返回 `ProduceResp::Err`，不抛 anyhow 错误（broker 错误是业务可处理的）；
    /// 只有 IPC 或配置层面的不可恢复错误才走 `Err(anyhow)`。
    async fn produce_ack(
        &self,
        msg: ProduceFnf,
        timeout: std::time::Duration,
    ) -> anyhow::Result<ProduceResp>;

    /// 阻塞至所有 in-flight 消息发送完成或超时。优雅停机用。
    async fn flush(&self, timeout: std::time::Duration) -> anyhow::Result<()>;
}

/// 把消息记录到日志，伪造一个固定的 partition/offset 返回。
pub struct LoggingProducer;

#[async_trait::async_trait]
impl Producer for LoggingProducer {
    async fn produce_fnf(&self, msg: ProduceFnf) -> anyhow::Result<()> {
        info!(
            cluster = %msg.cluster,
            topic = %msg.topic,
            key_len = msg.key.len(),
            value_len = msg.value.len(),
            "[LoggingProducer] PRODUCE_FNF (dry-run, kafka feature disabled)"
        );
        Ok(())
    }

    async fn produce_ack(
        &self,
        msg: ProduceFnf,
        _timeout: std::time::Duration,
    ) -> anyhow::Result<ProduceResp> {
        info!(
            cluster = %msg.cluster,
            topic = %msg.topic,
            key_len = msg.key.len(),
            value_len = msg.value.len(),
            "[LoggingProducer] PRODUCE_REQ (dry-run) → fake ACK"
        );
        Ok(ProduceResp::Ok(hi_kafka_proto::DeliveryAck {
            partition: 0,
            offset: -1,
        }))
    }

    async fn flush(&self, _timeout: std::time::Duration) -> anyhow::Result<()> {
        Ok(())
    }
}

pub type ProducerHandle = Arc<dyn Producer>;

pub fn logging() -> ProducerHandle {
    Arc::new(LoggingProducer)
}

#[cfg(feature = "kafka")]
pub use real::KafkaProducer;
