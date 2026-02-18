//! Logical replication event messages that live inside `XLogData.data` payloads.
//!
//! These types represent the protocol-level logical replication messages produced
//! by PostgreSQL's logical decoding output plugins (e.g., pgoutput). Each message
//! type is dispatched by a single-byte tag.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::replication::Lsn;
use crate::error::{PgWireError, PgWireResult};

/// Replica identity setting for a relation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    /// Default (primary key or unique index).
    Default,
    /// Nothing — no old tuple information.
    Nothing,
    /// Full — entire old tuple is logged.
    Full,
    /// Index — a specific index's columns identify the row.
    Index,
}

impl ReplicaIdentity {
    fn from_byte(b: u8) -> PgWireResult<Self> {
        match b {
            b'd' => Ok(ReplicaIdentity::Default),
            b'n' => Ok(ReplicaIdentity::Nothing),
            b'f' => Ok(ReplicaIdentity::Full),
            b'i' => Ok(ReplicaIdentity::Index),
            _ => Err(PgWireError::InvalidReplicationMessage(format!(
                "unknown replica identity: 0x{b:02X}"
            ))),
        }
    }

    fn to_byte(self) -> u8 {
        match self {
            ReplicaIdentity::Default => b'd',
            ReplicaIdentity::Nothing => b'n',
            ReplicaIdentity::Full => b'f',
            ReplicaIdentity::Index => b'i',
        }
    }
}

/// A column in a `RelationBody` message.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct Column {
    /// Column flags (bit 0 = part of key).
    pub flags: u8,
    /// Column name.
    pub name: String,
    /// OID of the column's data type.
    pub type_id: u32,
    /// Type modifier (e.g., for varchar(N)).
    pub type_modifier: i32,
}

/// A column value within a `TupleData`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleDataColumn {
    /// NULL value.
    Null,
    /// Unchanged TOASTed column (only sent for old tuples).
    Unchanged,
    /// Text representation of the value.
    Text(Bytes),
    /// Binary representation of the value.
    Binary(Bytes),
}

/// A tuple (row) of column data.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub columns: Vec<TupleDataColumn>,
}

impl TupleData {
    pub fn new(columns: Vec<TupleDataColumn>) -> Self {
        TupleData { columns }
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_i16(self.columns.len() as i16);
        for col in &self.columns {
            match col {
                TupleDataColumn::Null => {
                    buf.put_u8(b'n');
                }
                TupleDataColumn::Unchanged => {
                    buf.put_u8(b'u');
                }
                TupleDataColumn::Text(data) => {
                    buf.put_u8(b't');
                    buf.put_i32(data.len() as i32);
                    buf.put_slice(data);
                }
                TupleDataColumn::Binary(data) => {
                    buf.put_u8(b'b');
                    buf.put_i32(data.len() as i32);
                    buf.put_slice(data);
                }
            }
        }
    }

    fn decode(buf: &mut Bytes) -> PgWireResult<Self> {
        if buf.remaining() < 2 {
            return Err(PgWireError::InvalidReplicationMessage(
                "TupleData too short".to_owned(),
            ));
        }
        let n_cols = buf.get_i16() as usize;
        let mut columns = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            if buf.remaining() < 1 {
                return Err(PgWireError::InvalidReplicationMessage(
                    "TupleData column truncated".to_owned(),
                ));
            }
            let tag = buf.get_u8();
            match tag {
                b'n' => columns.push(TupleDataColumn::Null),
                b'u' => columns.push(TupleDataColumn::Unchanged),
                b't' | b'b' => {
                    if buf.remaining() < 4 {
                        return Err(PgWireError::InvalidReplicationMessage(
                            "TupleData column data length truncated".to_owned(),
                        ));
                    }
                    let len = buf.get_i32() as usize;
                    if buf.remaining() < len {
                        return Err(PgWireError::InvalidReplicationMessage(
                            "TupleData column data truncated".to_owned(),
                        ));
                    }
                    let data = buf.split_to(len);
                    if tag == b't' {
                        columns.push(TupleDataColumn::Text(data));
                    } else {
                        columns.push(TupleDataColumn::Binary(data));
                    }
                }
                _ => {
                    return Err(PgWireError::InvalidReplicationMessage(format!(
                        "unknown tuple data column tag: 0x{tag:02X}"
                    )));
                }
            }
        }
        Ok(TupleData { columns })
    }
}

// Helper to read a null-terminated string from Bytes
fn get_cstring(buf: &mut Bytes) -> PgWireResult<String> {
    let pos = buf
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| {
            PgWireError::InvalidReplicationMessage("missing null terminator in string".to_owned())
        })?;
    let s = String::from_utf8(buf.split_to(pos).to_vec()).map_err(|_| {
        PgWireError::InvalidReplicationMessage("invalid UTF-8 in string".to_owned())
    })?;
    buf.advance(1); // skip null terminator
    Ok(s)
}

fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.put_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Begin body — start of a transaction.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, new)]
pub struct BeginBody {
    /// The final LSN of the transaction.
    pub final_lsn: Lsn,
    /// Commit timestamp (microseconds since 2000-01-01 UTC).
    pub commit_time: i64,
    /// Transaction ID.
    pub xid: u32,
}

/// Commit body — end of a transaction.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, new)]
pub struct CommitBody {
    /// Flags (currently unused).
    pub flags: u8,
    /// LSN of the commit.
    pub commit_lsn: Lsn,
    /// End LSN of the transaction.
    pub end_lsn: Lsn,
    /// Commit timestamp (microseconds since 2000-01-01 UTC).
    pub commit_time: i64,
}

/// Relation body — describes a table.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct RelationBody {
    /// OID of the relation.
    pub relation_id: u32,
    /// Namespace (schema name).
    pub namespace: String,
    /// Relation name.
    pub name: String,
    /// Replica identity setting.
    pub replica_identity: ReplicaIdentity,
    /// Column descriptions.
    pub columns: Vec<Column>,
}

/// Insert body.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct InsertBody {
    pub relation_id: u32,
    pub tuple: TupleData,
}

/// Update body.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct UpdateBody {
    pub relation_id: u32,
    /// Old tuple — present when replica identity is FULL or when the updated
    /// columns include key columns.
    pub old_tuple: Option<TupleData>,
    pub new_tuple: TupleData,
}

/// Delete body.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct DeleteBody {
    pub relation_id: u32,
    pub old_tuple: TupleData,
}

/// Truncate body.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct TruncateBody {
    /// Truncate option bits.
    pub options: u8,
    /// OIDs of the truncated relations.
    pub relation_ids: Vec<u32>,
}

/// Origin body.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct OriginBody {
    pub commit_lsn: Lsn,
    pub name: String,
}

/// Type body — describes a custom data type.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct TypeBody {
    pub type_id: u32,
    pub namespace: String,
    pub name: String,
}

/// Logical decoding message (from `pg_logical_emit_message()`).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct LogicalDecodingMessageBody {
    /// Flags (bit 0 = transactional).
    pub flags: u8,
    pub lsn: Lsn,
    pub prefix: String,
    pub content: Bytes,
}

/// Stream start body (protocol v2+).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, new)]
pub struct StreamStartBody {
    pub xid: u32,
    /// First stream segment = 1, otherwise 0.
    pub first_segment: u8,
}

/// Stream stop body (protocol v2+). Empty message.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StreamStopBody;

/// Stream commit body (protocol v2+).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, new)]
pub struct StreamCommitBody {
    pub xid: u32,
    pub flags: u8,
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub commit_time: i64,
}

/// Stream abort body (protocol v2+).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, new)]
pub struct StreamAbortBody {
    pub xid: u32,
    pub sub_xid: u32,
    pub abort_lsn: Lsn,
    pub abort_time: i64,
}

/// Begin prepare body (protocol v3+, two-phase).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct BeginPrepareBody {
    pub prepare_lsn: Lsn,
    pub end_lsn: Lsn,
    pub prepare_time: i64,
    pub xid: u32,
    pub gid: String,
}

/// Prepare body (protocol v3+, two-phase).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct PrepareBody {
    pub flags: u8,
    pub prepare_lsn: Lsn,
    pub end_lsn: Lsn,
    pub prepare_time: i64,
    pub xid: u32,
    pub gid: String,
}

/// Commit prepared body (protocol v3+, two-phase).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct CommitPreparedBody {
    pub flags: u8,
    pub commit_lsn: Lsn,
    pub end_lsn: Lsn,
    pub commit_time: i64,
    pub xid: u32,
    pub gid: String,
}

/// Rollback prepared body (protocol v3+, two-phase).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, new)]
pub struct RollbackPreparedBody {
    pub flags: u8,
    pub prepare_end_lsn: Lsn,
    pub rollback_end_lsn: Lsn,
    pub prepare_time: i64,
    pub rollback_time: i64,
    pub xid: u32,
    pub gid: String,
}

/// Top-level logical replication message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogicalReplicationMessage {
    Begin(BeginBody),
    Commit(CommitBody),
    Relation(RelationBody),
    Insert(InsertBody),
    Update(UpdateBody),
    Delete(DeleteBody),
    Truncate(TruncateBody),
    Origin(OriginBody),
    Type(TypeBody),
    LogicalDecodingMessage(LogicalDecodingMessageBody),
    StreamStart(StreamStartBody),
    StreamStop(StreamStopBody),
    StreamCommit(StreamCommitBody),
    StreamAbort(StreamAbortBody),
    BeginPrepare(BeginPrepareBody),
    Prepare(PrepareBody),
    CommitPrepared(CommitPreparedBody),
    RollbackPrepared(RollbackPreparedBody),
}

impl LogicalReplicationMessage {
    /// Encode this logical replication message into bytes.
    pub fn encode_to_bytes(&self) -> PgWireResult<Bytes> {
        let mut buf = BytesMut::new();
        match self {
            LogicalReplicationMessage::Begin(msg) => {
                buf.put_u8(b'B');
                buf.put_u64(msg.final_lsn.0);
                buf.put_i64(msg.commit_time);
                buf.put_u32(msg.xid);
            }
            LogicalReplicationMessage::Commit(msg) => {
                buf.put_u8(b'C');
                buf.put_u8(msg.flags);
                buf.put_u64(msg.commit_lsn.0);
                buf.put_u64(msg.end_lsn.0);
                buf.put_i64(msg.commit_time);
            }
            LogicalReplicationMessage::Relation(msg) => {
                buf.put_u8(b'R');
                buf.put_u32(msg.relation_id);
                put_cstring(&mut buf, &msg.namespace);
                put_cstring(&mut buf, &msg.name);
                buf.put_u8(msg.replica_identity.to_byte());
                buf.put_i16(msg.columns.len() as i16);
                for col in &msg.columns {
                    buf.put_u8(col.flags);
                    put_cstring(&mut buf, &col.name);
                    buf.put_u32(col.type_id);
                    buf.put_i32(col.type_modifier);
                }
            }
            LogicalReplicationMessage::Insert(msg) => {
                buf.put_u8(b'I');
                buf.put_u32(msg.relation_id);
                buf.put_u8(b'N'); // new tuple marker
                msg.tuple.encode(&mut buf);
            }
            LogicalReplicationMessage::Update(msg) => {
                buf.put_u8(b'U');
                buf.put_u32(msg.relation_id);
                if let Some(old) = &msg.old_tuple {
                    buf.put_u8(b'O'); // old tuple marker
                    old.encode(&mut buf);
                }
                buf.put_u8(b'N'); // new tuple marker
                msg.new_tuple.encode(&mut buf);
            }
            LogicalReplicationMessage::Delete(msg) => {
                buf.put_u8(b'D');
                buf.put_u32(msg.relation_id);
                buf.put_u8(b'O'); // old tuple marker
                msg.old_tuple.encode(&mut buf);
            }
            LogicalReplicationMessage::Truncate(msg) => {
                buf.put_u8(b'T');
                buf.put_i32(msg.relation_ids.len() as i32);
                buf.put_u8(msg.options);
                for &rid in &msg.relation_ids {
                    buf.put_u32(rid);
                }
            }
            LogicalReplicationMessage::Origin(msg) => {
                buf.put_u8(b'O');
                buf.put_u64(msg.commit_lsn.0);
                put_cstring(&mut buf, &msg.name);
            }
            LogicalReplicationMessage::Type(msg) => {
                buf.put_u8(b'Y');
                buf.put_u32(msg.type_id);
                put_cstring(&mut buf, &msg.namespace);
                put_cstring(&mut buf, &msg.name);
            }
            LogicalReplicationMessage::LogicalDecodingMessage(msg) => {
                buf.put_u8(b'M');
                buf.put_u8(msg.flags);
                buf.put_u64(msg.lsn.0);
                put_cstring(&mut buf, &msg.prefix);
                buf.put_u32(msg.content.len() as u32);
                buf.put_slice(&msg.content);
            }
            LogicalReplicationMessage::StreamStart(msg) => {
                buf.put_u8(b'S');
                buf.put_u32(msg.xid);
                buf.put_u8(msg.first_segment);
            }
            LogicalReplicationMessage::StreamStop(_) => {
                buf.put_u8(b'E');
            }
            LogicalReplicationMessage::StreamCommit(msg) => {
                buf.put_u8(b'c');
                buf.put_u32(msg.xid);
                buf.put_u8(msg.flags);
                buf.put_u64(msg.commit_lsn.0);
                buf.put_u64(msg.end_lsn.0);
                buf.put_i64(msg.commit_time);
            }
            LogicalReplicationMessage::StreamAbort(msg) => {
                buf.put_u8(b'A');
                buf.put_u32(msg.xid);
                buf.put_u32(msg.sub_xid);
                buf.put_u64(msg.abort_lsn.0);
                buf.put_i64(msg.abort_time);
            }
            LogicalReplicationMessage::BeginPrepare(msg) => {
                buf.put_u8(b'b');
                buf.put_u64(msg.prepare_lsn.0);
                buf.put_u64(msg.end_lsn.0);
                buf.put_i64(msg.prepare_time);
                buf.put_u32(msg.xid);
                put_cstring(&mut buf, &msg.gid);
            }
            LogicalReplicationMessage::Prepare(msg) => {
                buf.put_u8(b'P');
                buf.put_u8(msg.flags);
                buf.put_u64(msg.prepare_lsn.0);
                buf.put_u64(msg.end_lsn.0);
                buf.put_i64(msg.prepare_time);
                buf.put_u32(msg.xid);
                put_cstring(&mut buf, &msg.gid);
            }
            LogicalReplicationMessage::CommitPrepared(msg) => {
                buf.put_u8(b'K');
                buf.put_u8(msg.flags);
                buf.put_u64(msg.commit_lsn.0);
                buf.put_u64(msg.end_lsn.0);
                buf.put_i64(msg.commit_time);
                buf.put_u32(msg.xid);
                put_cstring(&mut buf, &msg.gid);
            }
            LogicalReplicationMessage::RollbackPrepared(msg) => {
                buf.put_u8(b'r');
                buf.put_u8(msg.flags);
                buf.put_u64(msg.prepare_end_lsn.0);
                buf.put_u64(msg.rollback_end_lsn.0);
                buf.put_i64(msg.prepare_time);
                buf.put_i64(msg.rollback_time);
                buf.put_u32(msg.xid);
                put_cstring(&mut buf, &msg.gid);
            }
        }
        Ok(buf.freeze())
    }

    /// Decode a logical replication message from the `XLogData.data` payload.
    pub fn decode_from_bytes(mut buf: Bytes) -> PgWireResult<Self> {
        if buf.is_empty() {
            return Err(PgWireError::InvalidReplicationMessage(
                "empty logical replication message".to_owned(),
            ));
        }

        let tag = buf.get_u8();
        match tag {
            b'B' => {
                ensure_remaining(&buf, 20, "BeginBody")?;
                let final_lsn = Lsn(buf.get_u64());
                let commit_time = buf.get_i64();
                let xid = buf.get_u32();
                Ok(LogicalReplicationMessage::Begin(BeginBody {
                    final_lsn,
                    commit_time,
                    xid,
                }))
            }
            b'C' => {
                ensure_remaining(&buf, 25, "CommitBody")?;
                let flags = buf.get_u8();
                let commit_lsn = Lsn(buf.get_u64());
                let end_lsn = Lsn(buf.get_u64());
                let commit_time = buf.get_i64();
                Ok(LogicalReplicationMessage::Commit(CommitBody {
                    flags,
                    commit_lsn,
                    end_lsn,
                    commit_time,
                }))
            }
            b'R' => {
                ensure_remaining(&buf, 4, "RelationBody")?;
                let relation_id = buf.get_u32();
                let namespace = get_cstring(&mut buf)?;
                let name = get_cstring(&mut buf)?;
                ensure_remaining(&buf, 3, "RelationBody columns")?;
                let replica_identity = ReplicaIdentity::from_byte(buf.get_u8())?;
                let n_cols = buf.get_i16() as usize;
                let mut columns = Vec::with_capacity(n_cols);
                for _ in 0..n_cols {
                    ensure_remaining(&buf, 1, "Column flags")?;
                    let flags = buf.get_u8();
                    let col_name = get_cstring(&mut buf)?;
                    ensure_remaining(&buf, 8, "Column type")?;
                    let type_id = buf.get_u32();
                    let type_modifier = buf.get_i32();
                    columns.push(Column {
                        flags,
                        name: col_name,
                        type_id,
                        type_modifier,
                    });
                }
                Ok(LogicalReplicationMessage::Relation(RelationBody {
                    relation_id,
                    namespace,
                    name,
                    replica_identity,
                    columns,
                }))
            }
            b'I' => {
                ensure_remaining(&buf, 5, "InsertBody")?;
                let relation_id = buf.get_u32();
                let _new_marker = buf.get_u8(); // 'N'
                let tuple = TupleData::decode(&mut buf)?;
                Ok(LogicalReplicationMessage::Insert(InsertBody {
                    relation_id,
                    tuple,
                }))
            }
            b'U' => {
                ensure_remaining(&buf, 5, "UpdateBody")?;
                let relation_id = buf.get_u32();
                let marker = buf.get_u8();
                let old_tuple = if marker == b'O' || marker == b'K' {
                    let old = TupleData::decode(&mut buf)?;
                    ensure_remaining(&buf, 1, "UpdateBody new marker")?;
                    let _new_marker = buf.get_u8(); // 'N'
                    Some(old)
                } else {
                    // marker was 'N' for new tuple directly
                    None
                };
                let new_tuple = TupleData::decode(&mut buf)?;
                Ok(LogicalReplicationMessage::Update(UpdateBody {
                    relation_id,
                    old_tuple,
                    new_tuple,
                }))
            }
            b'D' => {
                ensure_remaining(&buf, 5, "DeleteBody")?;
                let relation_id = buf.get_u32();
                let _old_marker = buf.get_u8(); // 'O' or 'K'
                let old_tuple = TupleData::decode(&mut buf)?;
                Ok(LogicalReplicationMessage::Delete(DeleteBody {
                    relation_id,
                    old_tuple,
                }))
            }
            b'T' => {
                ensure_remaining(&buf, 5, "TruncateBody")?;
                let n_relations = buf.get_i32() as usize;
                let options = buf.get_u8();
                let mut relation_ids = Vec::with_capacity(n_relations);
                for _ in 0..n_relations {
                    ensure_remaining(&buf, 4, "TruncateBody relation")?;
                    relation_ids.push(buf.get_u32());
                }
                Ok(LogicalReplicationMessage::Truncate(TruncateBody {
                    options,
                    relation_ids,
                }))
            }
            b'O' => {
                ensure_remaining(&buf, 8, "OriginBody")?;
                let commit_lsn = Lsn(buf.get_u64());
                let name = get_cstring(&mut buf)?;
                Ok(LogicalReplicationMessage::Origin(OriginBody {
                    commit_lsn,
                    name,
                }))
            }
            b'Y' => {
                ensure_remaining(&buf, 4, "TypeBody")?;
                let type_id = buf.get_u32();
                let namespace = get_cstring(&mut buf)?;
                let name = get_cstring(&mut buf)?;
                Ok(LogicalReplicationMessage::Type(TypeBody {
                    type_id,
                    namespace,
                    name,
                }))
            }
            b'M' => {
                ensure_remaining(&buf, 9, "LogicalDecodingMessageBody")?;
                let flags = buf.get_u8();
                let lsn = Lsn(buf.get_u64());
                let prefix = get_cstring(&mut buf)?;
                ensure_remaining(&buf, 4, "LogicalDecodingMessageBody content")?;
                let content_len = buf.get_u32() as usize;
                if buf.remaining() < content_len {
                    return Err(PgWireError::InvalidReplicationMessage(
                        "LogicalDecodingMessageBody content truncated".to_owned(),
                    ));
                }
                let content = buf.split_to(content_len);
                Ok(LogicalReplicationMessage::LogicalDecodingMessage(
                    LogicalDecodingMessageBody {
                        flags,
                        lsn,
                        prefix,
                        content,
                    },
                ))
            }
            b'S' => {
                ensure_remaining(&buf, 5, "StreamStartBody")?;
                let xid = buf.get_u32();
                let first_segment = buf.get_u8();
                Ok(LogicalReplicationMessage::StreamStart(StreamStartBody {
                    xid,
                    first_segment,
                }))
            }
            b'E' => Ok(LogicalReplicationMessage::StreamStop(StreamStopBody)),
            b'c' => {
                ensure_remaining(&buf, 29, "StreamCommitBody")?;
                let xid = buf.get_u32();
                let flags = buf.get_u8();
                let commit_lsn = Lsn(buf.get_u64());
                let end_lsn = Lsn(buf.get_u64());
                let commit_time = buf.get_i64();
                Ok(LogicalReplicationMessage::StreamCommit(StreamCommitBody {
                    xid,
                    flags,
                    commit_lsn,
                    end_lsn,
                    commit_time,
                }))
            }
            b'A' => {
                ensure_remaining(&buf, 20, "StreamAbortBody")?;
                let xid = buf.get_u32();
                let sub_xid = buf.get_u32();
                let abort_lsn = Lsn(buf.get_u64());
                let abort_time = buf.get_i64();
                Ok(LogicalReplicationMessage::StreamAbort(StreamAbortBody {
                    xid,
                    sub_xid,
                    abort_lsn,
                    abort_time,
                }))
            }
            b'b' => {
                ensure_remaining(&buf, 28, "BeginPrepareBody")?;
                let prepare_lsn = Lsn(buf.get_u64());
                let end_lsn = Lsn(buf.get_u64());
                let prepare_time = buf.get_i64();
                let xid = buf.get_u32();
                let gid = get_cstring(&mut buf)?;
                Ok(LogicalReplicationMessage::BeginPrepare(BeginPrepareBody {
                    prepare_lsn,
                    end_lsn,
                    prepare_time,
                    xid,
                    gid,
                }))
            }
            b'P' => {
                ensure_remaining(&buf, 29, "PrepareBody")?;
                let flags = buf.get_u8();
                let prepare_lsn = Lsn(buf.get_u64());
                let end_lsn = Lsn(buf.get_u64());
                let prepare_time = buf.get_i64();
                let xid = buf.get_u32();
                let gid = get_cstring(&mut buf)?;
                Ok(LogicalReplicationMessage::Prepare(PrepareBody {
                    flags,
                    prepare_lsn,
                    end_lsn,
                    prepare_time,
                    xid,
                    gid,
                }))
            }
            b'K' => {
                ensure_remaining(&buf, 29, "CommitPreparedBody")?;
                let flags = buf.get_u8();
                let commit_lsn = Lsn(buf.get_u64());
                let end_lsn = Lsn(buf.get_u64());
                let commit_time = buf.get_i64();
                let xid = buf.get_u32();
                let gid = get_cstring(&mut buf)?;
                Ok(LogicalReplicationMessage::CommitPrepared(
                    CommitPreparedBody {
                        flags,
                        commit_lsn,
                        end_lsn,
                        commit_time,
                        xid,
                        gid,
                    },
                ))
            }
            b'r' => {
                ensure_remaining(&buf, 37, "RollbackPreparedBody")?;
                let flags = buf.get_u8();
                let prepare_end_lsn = Lsn(buf.get_u64());
                let rollback_end_lsn = Lsn(buf.get_u64());
                let prepare_time = buf.get_i64();
                let rollback_time = buf.get_i64();
                let xid = buf.get_u32();
                let gid = get_cstring(&mut buf)?;
                Ok(LogicalReplicationMessage::RollbackPrepared(
                    RollbackPreparedBody {
                        flags,
                        prepare_end_lsn,
                        rollback_end_lsn,
                        prepare_time,
                        rollback_time,
                        xid,
                        gid,
                    },
                ))
            }
            _ => Err(PgWireError::InvalidReplicationMessage(format!(
                "unknown logical replication message tag: 0x{tag:02X}"
            ))),
        }
    }
}

fn ensure_remaining(buf: &Bytes, needed: usize, context: &str) -> PgWireResult<()> {
    if buf.remaining() < needed {
        Err(PgWireError::InvalidReplicationMessage(format!(
            "{context} message too short"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &LogicalReplicationMessage) {
        let encoded = msg.encode_to_bytes().unwrap();
        let decoded = LogicalReplicationMessage::decode_from_bytes(encoded).unwrap();
        assert_eq!(msg, &decoded);
    }

    #[test]
    fn test_begin_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Begin(BeginBody {
            final_lsn: Lsn(0x0100_0000),
            commit_time: 737_000_000_000_000,
            xid: 42,
        }));
    }

    #[test]
    fn test_commit_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Commit(CommitBody {
            flags: 0,
            commit_lsn: Lsn(0x0100_0000),
            end_lsn: Lsn(0x0100_0100),
            commit_time: 737_000_000_000_000,
        }));
    }

    #[test]
    fn test_relation_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Relation(RelationBody {
            relation_id: 16384,
            namespace: "public".to_owned(),
            name: "users".to_owned(),
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                Column {
                    flags: 1,
                    name: "id".to_owned(),
                    type_id: 23,
                    type_modifier: -1,
                },
                Column {
                    flags: 0,
                    name: "name".to_owned(),
                    type_id: 25,
                    type_modifier: -1,
                },
            ],
        }));
    }

    #[test]
    fn test_insert_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Insert(InsertBody {
            relation_id: 16384,
            tuple: TupleData::new(vec![
                TupleDataColumn::Text(Bytes::from("1")),
                TupleDataColumn::Text(Bytes::from("alice")),
            ]),
        }));
    }

    #[test]
    fn test_update_with_old_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Update(UpdateBody {
            relation_id: 16384,
            old_tuple: Some(TupleData::new(vec![
                TupleDataColumn::Text(Bytes::from("1")),
                TupleDataColumn::Text(Bytes::from("alice")),
            ])),
            new_tuple: TupleData::new(vec![
                TupleDataColumn::Text(Bytes::from("1")),
                TupleDataColumn::Text(Bytes::from("bob")),
            ]),
        }));
    }

    #[test]
    fn test_update_without_old_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Update(UpdateBody {
            relation_id: 16384,
            old_tuple: None,
            new_tuple: TupleData::new(vec![
                TupleDataColumn::Text(Bytes::from("1")),
                TupleDataColumn::Text(Bytes::from("bob")),
            ]),
        }));
    }

    #[test]
    fn test_delete_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Delete(DeleteBody {
            relation_id: 16384,
            old_tuple: TupleData::new(vec![TupleDataColumn::Text(Bytes::from("1"))]),
        }));
    }

    #[test]
    fn test_truncate_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Truncate(TruncateBody {
            options: 0x01,
            relation_ids: vec![16384, 16385],
        }));
    }

    #[test]
    fn test_origin_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Origin(OriginBody {
            commit_lsn: Lsn(0x0200_0000),
            name: "my_origin".to_owned(),
        }));
    }

    #[test]
    fn test_type_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Type(TypeBody {
            type_id: 100000,
            namespace: "public".to_owned(),
            name: "my_type".to_owned(),
        }));
    }

    #[test]
    fn test_logical_decoding_message_roundtrip() {
        roundtrip(&LogicalReplicationMessage::LogicalDecodingMessage(
            LogicalDecodingMessageBody {
                flags: 1,
                lsn: Lsn(0x0300_0000),
                prefix: "my_prefix".to_owned(),
                content: Bytes::from("hello world"),
            },
        ));
    }

    #[test]
    fn test_stream_start_roundtrip() {
        roundtrip(&LogicalReplicationMessage::StreamStart(StreamStartBody {
            xid: 1000,
            first_segment: 1,
        }));
    }

    #[test]
    fn test_stream_stop_roundtrip() {
        roundtrip(&LogicalReplicationMessage::StreamStop(StreamStopBody));
    }

    #[test]
    fn test_stream_commit_roundtrip() {
        roundtrip(&LogicalReplicationMessage::StreamCommit(StreamCommitBody {
            xid: 1000,
            flags: 0,
            commit_lsn: Lsn(0x0100_0000),
            end_lsn: Lsn(0x0100_0100),
            commit_time: 737_000_000_000_000,
        }));
    }

    #[test]
    fn test_stream_abort_roundtrip() {
        roundtrip(&LogicalReplicationMessage::StreamAbort(StreamAbortBody {
            xid: 1000,
            sub_xid: 1001,
            abort_lsn: Lsn(0x0100_0000),
            abort_time: 737_000_000_000_000,
        }));
    }

    #[test]
    fn test_begin_prepare_roundtrip() {
        roundtrip(&LogicalReplicationMessage::BeginPrepare(
            BeginPrepareBody {
                prepare_lsn: Lsn(0x0100_0000),
                end_lsn: Lsn(0x0100_0100),
                prepare_time: 737_000_000_000_000,
                xid: 42,
                gid: "my_gid".to_owned(),
            },
        ));
    }

    #[test]
    fn test_prepare_roundtrip() {
        roundtrip(&LogicalReplicationMessage::Prepare(PrepareBody {
            flags: 0,
            prepare_lsn: Lsn(0x0100_0000),
            end_lsn: Lsn(0x0100_0100),
            prepare_time: 737_000_000_000_000,
            xid: 42,
            gid: "my_gid".to_owned(),
        }));
    }

    #[test]
    fn test_commit_prepared_roundtrip() {
        roundtrip(&LogicalReplicationMessage::CommitPrepared(
            CommitPreparedBody {
                flags: 0,
                commit_lsn: Lsn(0x0100_0000),
                end_lsn: Lsn(0x0100_0100),
                commit_time: 737_000_000_000_000,
                xid: 42,
                gid: "my_gid".to_owned(),
            },
        ));
    }

    #[test]
    fn test_rollback_prepared_roundtrip() {
        roundtrip(&LogicalReplicationMessage::RollbackPrepared(
            RollbackPreparedBody {
                flags: 0,
                prepare_end_lsn: Lsn(0x0100_0000),
                rollback_end_lsn: Lsn(0x0100_0100),
                prepare_time: 737_000_000_000_000,
                rollback_time: 738_000_000_000_000,
                xid: 42,
                gid: "my_gid".to_owned(),
            },
        ));
    }

    #[test]
    fn test_tuple_data_null_and_unchanged() {
        roundtrip(&LogicalReplicationMessage::Insert(InsertBody {
            relation_id: 16384,
            tuple: TupleData::new(vec![
                TupleDataColumn::Null,
                TupleDataColumn::Text(Bytes::from("value")),
            ]),
        }));
    }

    #[test]
    fn test_tuple_data_binary() {
        roundtrip(&LogicalReplicationMessage::Insert(InsertBody {
            relation_id: 16384,
            tuple: TupleData::new(vec![TupleDataColumn::Binary(Bytes::from(
                vec![0x00, 0x01, 0x02, 0xFF],
            ))]),
        }));
    }

    #[test]
    fn test_empty_message_error() {
        let result = LogicalReplicationMessage::decode_from_bytes(Bytes::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_tag_error() {
        let result = LogicalReplicationMessage::decode_from_bytes(Bytes::from_static(&[b'Z']));
        assert!(result.is_err());
    }

    #[test]
    fn test_replica_identity_variants() {
        for (byte, expected) in [
            (b'd', ReplicaIdentity::Default),
            (b'n', ReplicaIdentity::Nothing),
            (b'f', ReplicaIdentity::Full),
            (b'i', ReplicaIdentity::Index),
        ] {
            let ri = ReplicaIdentity::from_byte(byte).unwrap();
            assert_eq!(ri, expected);
            assert_eq!(ri.to_byte(), byte);
        }
    }
}
