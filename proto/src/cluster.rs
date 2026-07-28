//! Cluster registry control payloads.

use crate::payload::PayloadError;
use bytes::{Buf, BufMut, BytesMut};

const STATUS_OK: u8 = 0;
const STATUS_ERR: u8 = 1;
const MAX_CONFIG_ENTRIES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterClusterReq {
    pub cluster: String,
    pub config: Vec<(String, String)>,
}

impl RegisterClusterReq {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        write_str(&self.cluster, buf)?;
        if self.config.len() > MAX_CONFIG_ENTRIES {
            return Err(PayloadError::FieldTooLarge(self.config.len()));
        }
        buf.put_u16(self.config.len() as u16);
        for (key, value) in &self.config {
            write_str(key, buf)?;
            write_str(value, buf)?;
        }
        Ok(())
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        let cluster = read_str(&mut buf, "cluster")?;
        if buf.remaining() < 2 {
            return Err(PayloadError::Truncated);
        }
        let count = buf.get_u16() as usize;
        if count > MAX_CONFIG_ENTRIES {
            return Err(PayloadError::FieldTooLarge(count));
        }
        let mut config = Vec::with_capacity(count);
        for _ in 0..count {
            config.push((
                read_str(&mut buf, "config_key")?,
                read_str(&mut buf, "config_value")?,
            ));
        }
        Ok(Self { cluster, config })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterClusterResp {
    Ok,
    Err { message: String },
}

impl RegisterClusterResp {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), PayloadError> {
        match self {
            Self::Ok => buf.put_u8(STATUS_OK),
            Self::Err { message } => {
                buf.put_u8(STATUS_ERR);
                write_str(message, buf)?;
            }
        }
        Ok(())
    }
    pub fn decode(mut buf: &[u8]) -> Result<Self, PayloadError> {
        if !buf.has_remaining() {
            return Err(PayloadError::Truncated);
        }
        match buf.get_u8() {
            STATUS_OK => Ok(Self::Ok),
            STATUS_ERR => Ok(Self::Err {
                message: read_str(&mut buf, "message")?,
            }),
            tag => Err(PayloadError::InvalidTag {
                tag,
                field: "register_cluster_resp",
            }),
        }
    }
}

fn write_str(value: &str, buf: &mut BytesMut) -> Result<(), PayloadError> {
    if value.len() > u16::MAX as usize {
        return Err(PayloadError::FieldTooLarge(value.len()));
    }
    buf.put_u16(value.len() as u16);
    buf.put_slice(value.as_bytes());
    Ok(())
}

fn read_str(buf: &mut &[u8], field: &'static str) -> Result<String, PayloadError> {
    if buf.remaining() < 2 {
        return Err(PayloadError::Truncated);
    }
    let len = buf.get_u16() as usize;
    if buf.remaining() < len {
        return Err(PayloadError::Truncated);
    }
    String::from_utf8(buf.copy_to_bytes(len).to_vec())
        .map_err(|_| PayloadError::InvalidUtf8 { field })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip() {
        let value = RegisterClusterReq {
            cluster: "main".into(),
            config: vec![("bootstrap.servers".into(), "kafka:9092".into())],
        };
        let mut bytes = BytesMut::new();
        value.encode(&mut bytes).unwrap();
        assert_eq!(RegisterClusterReq::decode(&bytes).unwrap(), value);
    }
}
