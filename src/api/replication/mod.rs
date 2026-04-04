//! Replication protocol handler and helpers.
//!
//! This module provides the `ReplicationHandler` trait for building PostgreSQL
//! replication sources, along with response types and helper functions for
//! sending replication protocol messages.

pub mod command;

use std::fmt::Debug;

use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::{Sink, SinkExt};

use crate::api::results::Tag;
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::PgWireBackendMessage;
use crate::messages::copy::{CopyBothResponse, CopyData, CopyDone};
use crate::messages::data::{DataRow, FieldDescription, RowDescription};
use crate::messages::replication::{
    HotStandbyFeedback, Lsn, PrimaryKeepalive, ReplicationMessage, StandbyStatusUpdate, XLogData,
};
use crate::messages::response::CommandComplete;

use super::ClientInfo;

pub use command::{
    BaseBackupCommand, ReplicationCommand, SlotType, StartReplicationCommand,
    parse_replication_command,
};

/// Response for IDENTIFY_SYSTEM.
#[non_exhaustive]
#[derive(Debug, Clone, new)]
pub struct IdentifySystemResponse {
    pub system_id: String,
    pub timeline: u32,
    pub xlogpos: Lsn,
    pub dbname: Option<String>,
}

/// Response for CREATE_REPLICATION_SLOT.
#[non_exhaustive]
#[derive(Debug, Clone, new)]
pub struct CreateReplicationSlotResponse {
    pub slot_name: String,
    pub consistent_point: Lsn,
    pub snapshot_name: Option<String>,
    pub output_plugin: Option<String>,
}

/// Response for READ_REPLICATION_SLOT.
#[non_exhaustive]
#[derive(Debug, Clone, new)]
pub struct ReadReplicationSlotResponse {
    pub slot_type: SlotType,
    pub restart_lsn: Lsn,
    pub restart_tli: Option<u32>,
}

/// Response for TIMELINE_HISTORY.
#[non_exhaustive]
#[derive(Debug, Clone, new)]
pub struct TimelineHistoryResponse {
    pub filename: String,
    pub content: Bytes,
}

/// Decoded client-side replication message received during streaming.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationClientMessage {
    StandbyStatusUpdate(StandbyStatusUpdate),
    HotStandbyFeedback(HotStandbyFeedback),
}

/// Handler trait for replication protocol operations.
///
/// Implementors can handle replication commands and streaming messages.
/// `on_start_replication` is responsible for sending `CopyBothResponse`,
/// setting connection state to `ReplicationStreaming`, and beginning to stream
/// WAL data. This gives the implementor full control over the streaming loop.
#[async_trait]
pub trait ReplicationHandler: Send + Sync {
    async fn on_identify_system<C>(&self, client: &mut C) -> PgWireResult<IdentifySystemResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    async fn on_timeline_history<C>(
        &self,
        client: &mut C,
        timeline: u32,
    ) -> PgWireResult<TimelineHistoryResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    async fn on_create_replication_slot<C>(
        &self,
        client: &mut C,
        slot_name: &str,
        temporary: bool,
        slot_type: SlotType,
        options: &[(String, Option<String>)],
    ) -> PgWireResult<CreateReplicationSlotResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    async fn on_drop_replication_slot<C>(
        &self,
        client: &mut C,
        slot_name: &str,
        wait: bool,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    async fn on_read_replication_slot<C>(
        &self,
        client: &mut C,
        slot_name: &str,
    ) -> PgWireResult<ReadReplicationSlotResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    /// Handle START_REPLICATION.
    ///
    /// The handler is responsible for:
    /// 1. Sending `CopyBothResponse` via `send_copy_both_for_replication()`
    /// 2. Setting connection state to `ReplicationStreaming`
    /// 3. Beginning to stream `XLogData`/`PrimaryKeepalive` messages
    ///
    /// The streaming loop can be managed however the implementor prefers
    /// (spawned task, channel, etc). pgwire will dispatch incoming
    /// `CopyData` messages to `on_standby_status_update` and
    /// `on_hot_standby_feedback` while in `ReplicationStreaming` state.
    async fn on_start_replication<C>(
        &self,
        client: &mut C,
        cmd: &StartReplicationCommand,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    async fn on_alter_replication_slot<C>(
        &self,
        client: &mut C,
        slot_name: &str,
        options: &[(String, Option<String>)],
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    /// Handle BASE_BACKUP.
    ///
    /// The handler owns the full response sequence: tablespace header
    /// (RowDescription + DataRow(s) + CommandComplete), then for each
    /// tablespace a CopyOut stream of tar data, and optionally a manifest.
    /// The dispatch layer sends ReadyForQuery after this returns.
    async fn on_base_backup<C>(
        &self,
        client: &mut C,
        cmd: &BaseBackupCommand,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    /// Called during the streaming phase when a StandbyStatusUpdate is received.
    async fn on_standby_status_update<C>(
        &self,
        client: &mut C,
        update: StandbyStatusUpdate,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    /// Called during the streaming phase when a HotStandbyFeedback is received.
    async fn on_hot_standby_feedback<C>(
        &self,
        client: &mut C,
        feedback: HotStandbyFeedback,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
}

// --- Helper functions ---

/// Send an IDENTIFY_SYSTEM response as RowDescription + DataRow + CommandComplete.
pub async fn send_identify_system_response<C>(
    client: &mut C,
    resp: &IdentifySystemResponse,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let mut row_desc = RowDescription::default();
    row_desc.fields.push(text_field("systemid"));
    row_desc.fields.push(text_field("timeline"));
    row_desc.fields.push(text_field("xlogpos"));
    row_desc.fields.push(text_field("dbname"));
    client
        .send(PgWireBackendMessage::RowDescription(row_desc))
        .await?;

    let timeline_str = resp.timeline.to_string();
    let xlogpos_str = resp.xlogpos.to_string();
    let values: Vec<Option<&str>> = vec![
        Some(&resp.system_id),
        Some(&timeline_str),
        Some(&xlogpos_str),
        resp.dbname.as_deref(),
    ];
    send_text_data_row(client, &values).await?;

    client
        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
            "IDENTIFY_SYSTEM".to_owned(),
        )))
        .await?;
    Ok(())
}

/// Send a CREATE_REPLICATION_SLOT response.
pub async fn send_create_replication_slot_response<C>(
    client: &mut C,
    resp: &CreateReplicationSlotResponse,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let mut row_desc = RowDescription::default();
    row_desc.fields.push(text_field("slot_name"));
    row_desc.fields.push(text_field("consistent_point"));
    row_desc.fields.push(text_field("snapshot_name"));
    row_desc.fields.push(text_field("output_plugin"));
    client
        .send(PgWireBackendMessage::RowDescription(row_desc))
        .await?;

    let cp_str = resp.consistent_point.to_string();
    let values: Vec<Option<&str>> = vec![
        Some(&resp.slot_name),
        Some(&cp_str),
        resp.snapshot_name.as_deref(),
        resp.output_plugin.as_deref(),
    ];
    send_text_data_row(client, &values).await?;

    client
        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
            "CREATE_REPLICATION_SLOT".to_owned(),
        )))
        .await?;
    Ok(())
}

/// Send a READ_REPLICATION_SLOT response.
pub async fn send_read_replication_slot_response<C>(
    client: &mut C,
    resp: &ReadReplicationSlotResponse,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let mut row_desc = RowDescription::default();
    row_desc.fields.push(text_field("slot_type"));
    row_desc.fields.push(text_field("restart_lsn"));
    row_desc.fields.push(text_field("restart_tli"));
    client
        .send(PgWireBackendMessage::RowDescription(row_desc))
        .await?;

    let slot_type_str = match resp.slot_type {
        SlotType::Physical => "physical",
        SlotType::Logical => "logical",
    };
    let restart_lsn_str = resp.restart_lsn.to_string();
    let restart_tli_str = resp.restart_tli.map(|t| t.to_string());
    let values: Vec<Option<&str>> = vec![
        Some(slot_type_str),
        Some(&restart_lsn_str),
        restart_tli_str.as_deref(),
    ];
    send_text_data_row(client, &values).await?;

    client
        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
            "READ_REPLICATION_SLOT".to_owned(),
        )))
        .await?;
    Ok(())
}

/// Send a TIMELINE_HISTORY response.
pub async fn send_timeline_history_response<C>(
    client: &mut C,
    resp: &TimelineHistoryResponse,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let mut row_desc = RowDescription::default();
    row_desc.fields.push(text_field("filename"));
    row_desc.fields.push(text_field("content"));
    client
        .send(PgWireBackendMessage::RowDescription(row_desc))
        .await?;

    let content_str = String::from_utf8_lossy(&resp.content);
    let values: Vec<Option<&str>> = vec![Some(&resp.filename), Some(&content_str)];
    send_text_data_row(client, &values).await?;

    client
        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
            "TIMELINE_HISTORY".to_owned(),
        )))
        .await?;
    Ok(())
}

/// Send the BASE_BACKUP tablespace header: RowDescription + DataRow(s) + CommandComplete.
///
/// Each entry is (spcoid, spclocation, size). Use `None` for the base tablespace.
pub async fn send_base_backup_tablespace_header<C>(
    client: &mut C,
    tablespaces: &[(Option<String>, Option<String>, Option<i64>)],
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    use crate::messages::data::FORMAT_CODE_TEXT;

    let mut row_desc = RowDescription::default();
    row_desc.fields.push(FieldDescription {
        name: "spcoid".into(),
        table_id: 0,
        column_id: 0,
        type_id: 26, // OID
        type_size: 4,
        type_modifier: -1,
        format_code: FORMAT_CODE_TEXT,
    });
    row_desc.fields.push(text_field("spclocation"));
    row_desc.fields.push(FieldDescription {
        name: "size".into(),
        table_id: 0,
        column_id: 0,
        type_id: 20, // INT8
        type_size: 8,
        type_modifier: -1,
        format_code: FORMAT_CODE_TEXT,
    });
    client
        .send(PgWireBackendMessage::RowDescription(row_desc))
        .await?;

    for (spcoid, spclocation, size) in tablespaces {
        let spcoid_str = spcoid.as_deref();
        let spcloc_str = spclocation.as_deref();
        let size_string = size.map(|s| s.to_string());
        let size_str = size_string.as_deref();
        send_text_data_row(client, &[spcoid_str, spcloc_str, size_str]).await?;
    }

    client
        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
            "BASE_BACKUP".to_owned(),
        )))
        .await?;
    Ok(())
}

/// Send a CopyBothResponse to begin the replication streaming phase.
pub async fn send_copy_both_for_replication<C>(client: &mut C) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    // format=0 (text), 0 columns — replication uses CopyData with sub-messages
    let resp = CopyBothResponse::new(0, 0, vec![]);
    client
        .send(PgWireBackendMessage::CopyBothResponse(resp))
        .await?;
    Ok(())
}

/// Send the terminal backend messages that PostgreSQL emits when a client exits
/// `START_REPLICATION ... LOGICAL` with `CopyDone`.
pub async fn send_start_replication_complete<C>(client: &mut C) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    client
        .send(PgWireBackendMessage::CopyDone(CopyDone::new()))
        .await?;
    client
        .send(PgWireBackendMessage::CommandComplete(
            Tag::new("COPY").with_rows(0).into(),
        ))
        .await?;
    client
        .send(PgWireBackendMessage::CommandComplete(CommandComplete::new(
            "START_REPLICATION".to_owned(),
        )))
        .await?;
    Ok(())
}

/// Send an XLogData message wrapped in CopyData during the streaming phase.
pub async fn send_xlog_data<C>(client: &mut C, xlog: &XLogData) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let payload = ReplicationMessage::XLogData(xlog.clone()).encode_to_bytes()?;
    client
        .send(PgWireBackendMessage::CopyData(CopyData::new(payload)))
        .await?;
    Ok(())
}

/// Send a PrimaryKeepalive message wrapped in CopyData during the streaming phase.
pub async fn send_primary_keepalive<C>(
    client: &mut C,
    keepalive: &PrimaryKeepalive,
) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    let payload = ReplicationMessage::PrimaryKeepalive(*keepalive).encode_to_bytes()?;
    client
        .send(PgWireBackendMessage::CopyData(CopyData::new(payload)))
        .await?;
    Ok(())
}

/// Decode an incoming CopyData payload during the streaming phase into a client message.
pub fn decode_replication_client_message(
    copy_data: &CopyData,
) -> PgWireResult<ReplicationClientMessage> {
    let msg = ReplicationMessage::decode_from_bytes(copy_data.data.clone())?;
    match msg {
        ReplicationMessage::StandbyStatusUpdate(ssu) => {
            Ok(ReplicationClientMessage::StandbyStatusUpdate(ssu))
        }
        ReplicationMessage::HotStandbyFeedback(hsf) => {
            Ok(ReplicationClientMessage::HotStandbyFeedback(hsf))
        }
        other => Err(PgWireError::InvalidReplicationMessage(format!(
            "unexpected replication message from client: {other:?}"
        ))),
    }
}

// --- Internal helpers ---

fn text_field(name: &str) -> FieldDescription {
    use crate::messages::data::FORMAT_CODE_TEXT;
    FieldDescription {
        name: name.into(),
        table_id: 0,
        column_id: 0,
        type_id: 25, // TEXT OID
        type_size: -1,
        type_modifier: -1,
        format_code: FORMAT_CODE_TEXT,
    }
}

async fn send_text_data_row<C>(client: &mut C, values: &[Option<&str>]) -> PgWireResult<()>
where
    C: Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    use bytes::BufMut;
    let mut row = DataRow::default();
    for val in values {
        match val {
            Some(s) => {
                row.data.put_i32(s.len() as i32);
                row.data.put_slice(s.as_bytes());
            }
            None => {
                row.data.put_i32(-1);
            }
        }
    }
    row.field_count = values.len() as i16;
    client.send(PgWireBackendMessage::DataRow(row)).await?;
    Ok(())
}

// --- NoopHandler impl ---

#[async_trait]
impl ReplicationHandler for super::NoopHandler {
    async fn on_identify_system<C>(&self, _client: &mut C) -> PgWireResult<IdentifySystemResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_timeline_history<C>(
        &self,
        _client: &mut C,
        _timeline: u32,
    ) -> PgWireResult<TimelineHistoryResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_create_replication_slot<C>(
        &self,
        _client: &mut C,
        _slot_name: &str,
        _temporary: bool,
        _slot_type: SlotType,
        _options: &[(String, Option<String>)],
    ) -> PgWireResult<CreateReplicationSlotResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_drop_replication_slot<C>(
        &self,
        _client: &mut C,
        _slot_name: &str,
        _wait: bool,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_read_replication_slot<C>(
        &self,
        _client: &mut C,
        _slot_name: &str,
    ) -> PgWireResult<ReadReplicationSlotResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_start_replication<C>(
        &self,
        _client: &mut C,
        _cmd: &StartReplicationCommand,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_alter_replication_slot<C>(
        &self,
        _client: &mut C,
        _slot_name: &str,
        _options: &[(String, Option<String>)],
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_base_backup<C>(
        &self,
        _client: &mut C,
        _cmd: &BaseBackupCommand,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_standby_status_update<C>(
        &self,
        _client: &mut C,
        _update: StandbyStatusUpdate,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }

    async fn on_hot_standby_feedback<C>(
        &self,
        _client: &mut C,
        _feedback: HotStandbyFeedback,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(not_implemented())
    }
}

fn not_implemented() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".to_owned(),
        "08P01".to_owned(),
        "This feature is not implemented.".to_string(),
    )))
}
