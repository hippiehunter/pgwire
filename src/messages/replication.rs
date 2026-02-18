//! Replication protocol message types carried inside `CopyData` payloads.
//!
//! These types represent the streaming-level messages used during PostgreSQL
//! replication. They do NOT implement the top-level `Message` trait since they
//! travel inside `CopyData` messages. Instead, they have their own encode/decode
//! using single-byte discriminators.

use std::fmt;
use std::str::FromStr;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{PgWireError, PgWireResult};

/// Log Sequence Number — a 64-bit position in the WAL stream.
///
/// Displays in PostgreSQL's `"X/X"` hex format (e.g., `"0/16B3748"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const INVALID: Lsn = Lsn(0);

    pub fn new(value: u64) -> Self {
        Lsn(value)
    }

    pub fn is_valid(self) -> bool {
        self.0 != 0
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hi = (self.0 >> 32) as u32;
        let lo = self.0 as u32;
        write!(f, "{hi:X}/{lo:X}")
    }
}

impl FromStr for Lsn {
    type Err = PgWireError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi_str, lo_str) = s
            .split_once('/')
            .ok_or_else(|| PgWireError::InvalidLsnFormat(s.to_owned()))?;
        let hi = u32::from_str_radix(hi_str, 16)
            .map_err(|_| PgWireError::InvalidLsnFormat(s.to_owned()))?;
        let lo = u32::from_str_radix(lo_str, 16)
            .map_err(|_| PgWireError::InvalidLsnFormat(s.to_owned()))?;
        Ok(Lsn(((hi as u64) << 32) | lo as u64))
    }
}

impl From<u64> for Lsn {
    fn from(value: u64) -> Self {
        Lsn(value)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> u64 {
        lsn.0
    }
}

// Discriminator bytes for replication messages
const XLOG_DATA_TAG: u8 = b'w';
const PRIMARY_KEEPALIVE_TAG: u8 = b'k';
const STANDBY_STATUS_UPDATE_TAG: u8 = b'r';
const HOT_STANDBY_FEEDBACK_TAG: u8 = b'h';

/// Server→client WAL data message.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct XLogData {
    /// Starting point of the WAL data in this message.
    pub start_lsn: Lsn,
    /// Current end of WAL on the server.
    pub end_lsn: Lsn,
    /// Server's system clock at time of transmission (microseconds since
    /// 2000-01-01 UTC).
    pub send_time: i64,
    /// The WAL data payload.
    pub data: Bytes,
}

/// Server→client heartbeat message.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, new)]
pub struct PrimaryKeepalive {
    /// Current end of WAL on the server.
    pub wal_end: Lsn,
    /// Server's system clock (microseconds since 2000-01-01 UTC).
    pub send_time: i64,
    /// If true, the client should reply with a StandbyStatusUpdate ASAP.
    pub reply_requested: bool,
}

/// Client→server position report.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, new)]
pub struct StandbyStatusUpdate {
    /// The location of the last WAL byte + 1 received and written to disk.
    pub write_pos: Lsn,
    /// The location of the last WAL byte + 1 flushed to disk.
    pub flush_pos: Lsn,
    /// The location of the last WAL byte + 1 applied.
    pub apply_pos: Lsn,
    /// Client's system clock (microseconds since 2000-01-01 UTC).
    pub client_time: i64,
    /// If true, the client requests the server to reply immediately.
    pub reply_requested: bool,
}

/// Client→server xmin report for hot standby feedback.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, new)]
pub struct HotStandbyFeedback {
    /// Client's system clock (microseconds since 2000-01-01 UTC).
    pub client_time: i64,
    /// Standby's current global xmin.
    pub global_xmin: u32,
    /// Epoch of global_xmin.
    pub global_xmin_epoch: u32,
    /// Standby's current catalog xmin.
    pub catalog_xmin: u32,
    /// Epoch of catalog_xmin.
    pub catalog_xmin_epoch: u32,
}

/// A replication message carried inside a `CopyData` payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMessage {
    XLogData(XLogData),
    PrimaryKeepalive(PrimaryKeepalive),
    StandbyStatusUpdate(StandbyStatusUpdate),
    HotStandbyFeedback(HotStandbyFeedback),
}

impl ReplicationMessage {
    /// Encode this message into bytes suitable for a `CopyData` payload.
    pub fn encode_to_bytes(&self) -> PgWireResult<Bytes> {
        let mut buf = BytesMut::new();
        match self {
            ReplicationMessage::XLogData(msg) => {
                buf.put_u8(XLOG_DATA_TAG);
                buf.put_u64(msg.start_lsn.0);
                buf.put_u64(msg.end_lsn.0);
                buf.put_i64(msg.send_time);
                buf.put_slice(&msg.data);
            }
            ReplicationMessage::PrimaryKeepalive(msg) => {
                buf.put_u8(PRIMARY_KEEPALIVE_TAG);
                buf.put_u64(msg.wal_end.0);
                buf.put_i64(msg.send_time);
                buf.put_u8(u8::from(msg.reply_requested));
            }
            ReplicationMessage::StandbyStatusUpdate(msg) => {
                buf.put_u8(STANDBY_STATUS_UPDATE_TAG);
                buf.put_u64(msg.write_pos.0);
                buf.put_u64(msg.flush_pos.0);
                buf.put_u64(msg.apply_pos.0);
                buf.put_i64(msg.client_time);
                buf.put_u8(u8::from(msg.reply_requested));
            }
            ReplicationMessage::HotStandbyFeedback(msg) => {
                buf.put_u8(HOT_STANDBY_FEEDBACK_TAG);
                buf.put_i64(msg.client_time);
                buf.put_u32(msg.global_xmin);
                buf.put_u32(msg.global_xmin_epoch);
                buf.put_u32(msg.catalog_xmin);
                buf.put_u32(msg.catalog_xmin_epoch);
            }
        }
        Ok(buf.freeze())
    }

    /// Decode a replication message from a `CopyData` payload.
    pub fn decode_from_bytes(mut buf: Bytes) -> PgWireResult<Self> {
        if buf.is_empty() {
            return Err(PgWireError::InvalidReplicationMessage(
                "empty replication message".to_owned(),
            ));
        }

        let tag = buf[0];
        buf.advance(1);

        match tag {
            XLOG_DATA_TAG => {
                if buf.remaining() < 24 {
                    return Err(PgWireError::InvalidReplicationMessage(
                        "XLogData too short".to_owned(),
                    ));
                }
                let start_lsn = Lsn(buf.get_u64());
                let end_lsn = Lsn(buf.get_u64());
                let send_time = buf.get_i64();
                let data = buf;
                Ok(ReplicationMessage::XLogData(XLogData {
                    start_lsn,
                    end_lsn,
                    send_time,
                    data,
                }))
            }
            PRIMARY_KEEPALIVE_TAG => {
                if buf.remaining() < 17 {
                    return Err(PgWireError::InvalidReplicationMessage(
                        "PrimaryKeepalive too short".to_owned(),
                    ));
                }
                let wal_end = Lsn(buf.get_u64());
                let send_time = buf.get_i64();
                let reply_requested = buf.get_u8() != 0;
                Ok(ReplicationMessage::PrimaryKeepalive(PrimaryKeepalive {
                    wal_end,
                    send_time,
                    reply_requested,
                }))
            }
            STANDBY_STATUS_UPDATE_TAG => {
                if buf.remaining() < 33 {
                    return Err(PgWireError::InvalidReplicationMessage(
                        "StandbyStatusUpdate too short".to_owned(),
                    ));
                }
                let write_pos = Lsn(buf.get_u64());
                let flush_pos = Lsn(buf.get_u64());
                let apply_pos = Lsn(buf.get_u64());
                let client_time = buf.get_i64();
                let reply_requested = buf.get_u8() != 0;
                Ok(ReplicationMessage::StandbyStatusUpdate(
                    StandbyStatusUpdate {
                        write_pos,
                        flush_pos,
                        apply_pos,
                        client_time,
                        reply_requested,
                    },
                ))
            }
            HOT_STANDBY_FEEDBACK_TAG => {
                if buf.remaining() < 24 {
                    return Err(PgWireError::InvalidReplicationMessage(
                        "HotStandbyFeedback too short".to_owned(),
                    ));
                }
                let client_time = buf.get_i64();
                let global_xmin = buf.get_u32();
                let global_xmin_epoch = buf.get_u32();
                let catalog_xmin = buf.get_u32();
                let catalog_xmin_epoch = buf.get_u32();
                Ok(ReplicationMessage::HotStandbyFeedback(
                    HotStandbyFeedback {
                        client_time,
                        global_xmin,
                        global_xmin_epoch,
                        catalog_xmin,
                        catalog_xmin_epoch,
                    },
                ))
            }
            other => Err(PgWireError::InvalidReplicationMessage(format!(
                "unknown replication message tag: 0x{other:02X}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_display_and_parse() {
        let lsn = Lsn(0x0000_0001_6B3_7480);
        let s = lsn.to_string();
        assert_eq!(s, "0/16B37480");
        let parsed: Lsn = s.parse().unwrap();
        assert_eq!(parsed, lsn);
    }

    #[test]
    fn test_lsn_display_zero() {
        let lsn = Lsn(0);
        assert_eq!(lsn.to_string(), "0/0");
        assert_eq!("0/0".parse::<Lsn>().unwrap(), Lsn::INVALID);
    }

    #[test]
    fn test_lsn_display_high_bits() {
        let lsn = Lsn(0xABCD_0000_1234_5678);
        assert_eq!(lsn.to_string(), "ABCD0000/12345678");
        assert_eq!("ABCD0000/12345678".parse::<Lsn>().unwrap(), lsn);
    }

    #[test]
    fn test_lsn_parse_error() {
        assert!("invalid".parse::<Lsn>().is_err());
        assert!("not/hex".parse::<Lsn>().is_err());
        assert!("/123".parse::<Lsn>().is_err());
    }

    #[test]
    fn test_lsn_ordering() {
        let a = Lsn(100);
        let b = Lsn(200);
        assert!(a < b);
    }

    #[test]
    fn test_xlog_data_roundtrip() {
        let msg = ReplicationMessage::XLogData(XLogData {
            start_lsn: Lsn(0x0000_0000_0100_0000),
            end_lsn: Lsn(0x0000_0000_0100_0100),
            send_time: 737_000_000_000_000,
            data: Bytes::from_static(b"hello wal data"),
        });
        let encoded = msg.encode_to_bytes().unwrap();
        let decoded = ReplicationMessage::decode_from_bytes(encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_primary_keepalive_roundtrip() {
        let msg = ReplicationMessage::PrimaryKeepalive(PrimaryKeepalive {
            wal_end: Lsn(0x0000_0000_0200_0000),
            send_time: 737_000_000_000_000,
            reply_requested: true,
        });
        let encoded = msg.encode_to_bytes().unwrap();
        let decoded = ReplicationMessage::decode_from_bytes(encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_primary_keepalive_no_reply() {
        let msg = ReplicationMessage::PrimaryKeepalive(PrimaryKeepalive {
            wal_end: Lsn(0x0000_0000_0200_0000),
            send_time: 0,
            reply_requested: false,
        });
        let encoded = msg.encode_to_bytes().unwrap();
        let decoded = ReplicationMessage::decode_from_bytes(encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_standby_status_update_roundtrip() {
        let msg = ReplicationMessage::StandbyStatusUpdate(StandbyStatusUpdate {
            write_pos: Lsn(0x0100_0000),
            flush_pos: Lsn(0x00F0_0000),
            apply_pos: Lsn(0x00E0_0000),
            client_time: 737_000_000_000_000,
            reply_requested: false,
        });
        let encoded = msg.encode_to_bytes().unwrap();
        let decoded = ReplicationMessage::decode_from_bytes(encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_hot_standby_feedback_roundtrip() {
        let msg = ReplicationMessage::HotStandbyFeedback(HotStandbyFeedback {
            client_time: 737_000_000_000_000,
            global_xmin: 1000,
            global_xmin_epoch: 0,
            catalog_xmin: 900,
            catalog_xmin_epoch: 0,
        });
        let encoded = msg.encode_to_bytes().unwrap();
        let decoded = ReplicationMessage::decode_from_bytes(encoded).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_xlog_data_known_bytes() {
        // Hand-crafted XLogData: tag 'w', start_lsn=0/1000000, end_lsn=0/1000100,
        // send_time=0, data=b"WAL"
        let mut buf = BytesMut::new();
        buf.put_u8(b'w');
        buf.put_u64(0x0000_0000_0100_0000); // start_lsn
        buf.put_u64(0x0000_0000_0100_0100); // end_lsn
        buf.put_i64(0); // send_time
        buf.put_slice(b"WAL");

        let msg = ReplicationMessage::decode_from_bytes(buf.freeze()).unwrap();
        match msg {
            ReplicationMessage::XLogData(xlog) => {
                assert_eq!(xlog.start_lsn, Lsn(0x0100_0000));
                assert_eq!(xlog.end_lsn, Lsn(0x0100_0100));
                assert_eq!(xlog.send_time, 0);
                assert_eq!(xlog.data.as_ref(), b"WAL");
            }
            _ => panic!("expected XLogData"),
        }
    }

    #[test]
    fn test_primary_keepalive_known_bytes() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'k');
        buf.put_u64(0x0000_0000_0200_0000); // wal_end
        buf.put_i64(1_000_000); // send_time
        buf.put_u8(1); // reply_requested

        let msg = ReplicationMessage::decode_from_bytes(buf.freeze()).unwrap();
        match msg {
            ReplicationMessage::PrimaryKeepalive(ka) => {
                assert_eq!(ka.wal_end, Lsn(0x0200_0000));
                assert_eq!(ka.send_time, 1_000_000);
                assert!(ka.reply_requested);
            }
            _ => panic!("expected PrimaryKeepalive"),
        }
    }

    #[test]
    fn test_standby_status_update_known_bytes() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'r');
        buf.put_u64(0x0100_0000); // write_pos
        buf.put_u64(0x00F0_0000); // flush_pos
        buf.put_u64(0x00E0_0000); // apply_pos
        buf.put_i64(500_000); // client_time
        buf.put_u8(0); // reply_requested

        let msg = ReplicationMessage::decode_from_bytes(buf.freeze()).unwrap();
        match msg {
            ReplicationMessage::StandbyStatusUpdate(ssu) => {
                assert_eq!(ssu.write_pos, Lsn(0x0100_0000));
                assert_eq!(ssu.flush_pos, Lsn(0x00F0_0000));
                assert_eq!(ssu.apply_pos, Lsn(0x00E0_0000));
                assert_eq!(ssu.client_time, 500_000);
                assert!(!ssu.reply_requested);
            }
            _ => panic!("expected StandbyStatusUpdate"),
        }
    }

    #[test]
    fn test_hot_standby_feedback_known_bytes() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'h');
        buf.put_i64(999_999); // client_time
        buf.put_u32(42); // global_xmin
        buf.put_u32(0); // global_xmin_epoch
        buf.put_u32(41); // catalog_xmin
        buf.put_u32(0); // catalog_xmin_epoch

        let msg = ReplicationMessage::decode_from_bytes(buf.freeze()).unwrap();
        match msg {
            ReplicationMessage::HotStandbyFeedback(hsf) => {
                assert_eq!(hsf.client_time, 999_999);
                assert_eq!(hsf.global_xmin, 42);
                assert_eq!(hsf.global_xmin_epoch, 0);
                assert_eq!(hsf.catalog_xmin, 41);
                assert_eq!(hsf.catalog_xmin_epoch, 0);
            }
            _ => panic!("expected HotStandbyFeedback"),
        }
    }

    #[test]
    fn test_empty_message_error() {
        let result = ReplicationMessage::decode_from_bytes(Bytes::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_tag_error() {
        let buf = Bytes::from_static(&[b'Z', 0, 0, 0, 0]);
        let result = ReplicationMessage::decode_from_bytes(buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_truncated_xlog_data_error() {
        let mut buf = BytesMut::new();
        buf.put_u8(b'w');
        buf.put_u64(0); // start_lsn only, missing rest
        let result = ReplicationMessage::decode_from_bytes(buf.freeze());
        assert!(result.is_err());
    }
}
