use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::{Sink, SinkExt};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::replication::{
    CreateReplicationSlotResponse, IdentifySystemResponse, ReadReplicationSlotResponse,
    ReplicationHandler, SlotType, StartReplicationCommand, TimelineHistoryResponse,
    send_copy_both_for_replication, send_primary_keepalive, send_xlog_data,
};
use pgwire::api::{ClientInfo, PgWireConnectionState, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::replication::{
    HotStandbyFeedback, Lsn, PrimaryKeepalive, StandbyStatusUpdate, XLogData,
};
use pgwire::tokio::process_socket;

pub struct DummyReplicationHandler;

// Use the noop startup handler for simple auth (no password)
impl NoopStartupHandler for DummyReplicationHandler {}

#[async_trait]
impl ReplicationHandler for DummyReplicationHandler {
    async fn on_identify_system<C>(
        &self,
        _client: &mut C,
    ) -> PgWireResult<IdentifySystemResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("IDENTIFY_SYSTEM");
        Ok(IdentifySystemResponse::new(
            "7000000000000001".to_owned(),
            1,
            Lsn(0x0000_0000_0100_0000),
            Some("demo".to_owned()),
        ))
    }

    async fn on_timeline_history<C>(
        &self,
        _client: &mut C,
        timeline: u32,
    ) -> PgWireResult<TimelineHistoryResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("TIMELINE_HISTORY {timeline}");
        Ok(TimelineHistoryResponse::new(
            format!("{timeline:08X}.history"),
            Bytes::from("1\t0/1000000\tno recovery target specified\n"),
        ))
    }

    async fn on_create_replication_slot<C>(
        &self,
        _client: &mut C,
        slot_name: &str,
        temporary: bool,
        slot_type: SlotType,
        options: &[(String, Option<String>)],
    ) -> PgWireResult<CreateReplicationSlotResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!(
            "CREATE_REPLICATION_SLOT {slot_name} temporary={temporary} type={slot_type:?} options={options:?}"
        );
        Ok(CreateReplicationSlotResponse::new(
            slot_name.to_owned(),
            Lsn(0x0000_0000_0100_0000),
            None,
            options
                .iter()
                .find(|(k, _)| k == "output_plugin")
                .and_then(|(_, v)| v.clone()),
        ))
    }

    async fn on_drop_replication_slot<C>(
        &self,
        _client: &mut C,
        slot_name: &str,
        wait: bool,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("DROP_REPLICATION_SLOT {slot_name} wait={wait}");
        Ok(())
    }

    async fn on_read_replication_slot<C>(
        &self,
        _client: &mut C,
        slot_name: &str,
    ) -> PgWireResult<ReadReplicationSlotResponse>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("READ_REPLICATION_SLOT {slot_name}");
        Ok(ReadReplicationSlotResponse::new(
            SlotType::Logical,
            Lsn(0x0000_0000_0100_0000),
            Some(1),
        ))
    }

    async fn on_start_replication<C>(
        &self,
        client: &mut C,
        cmd: &StartReplicationCommand,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("START_REPLICATION {:?}", cmd);

        // Send CopyBothResponse to begin streaming
        send_copy_both_for_replication(client).await?;
        client.set_state(PgWireConnectionState::ReplicationStreaming);

        // Stream some dummy WAL data
        let start_lsn = cmd.start_lsn;
        for i in 0..5 {
            let lsn = Lsn(start_lsn.as_u64() + i * 100);
            let data = format!("WAL record #{i}");
            send_xlog_data(
                client,
                &XLogData::new(lsn, Lsn(lsn.as_u64() + 100), 0, Bytes::from(data)),
            )
            .await?;
        }

        // Send a keepalive requesting a reply
        send_primary_keepalive(
            client,
            &PrimaryKeepalive::new(
                Lsn(start_lsn.as_u64() + 500),
                0,
                true,
            ),
        )
        .await?;

        // Flush to ensure all messages are sent
        client.flush().await?;

        Ok(())
    }

    async fn on_alter_replication_slot<C>(
        &self,
        _client: &mut C,
        slot_name: &str,
        options: &[(String, Option<String>)],
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!("ALTER_REPLICATION_SLOT {slot_name} options={options:?}");
        Ok(())
    }

    async fn on_standby_status_update<C>(
        &self,
        _client: &mut C,
        update: StandbyStatusUpdate,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!(
            "StandbyStatusUpdate: write={} flush={} apply={}",
            update.write_pos, update.flush_pos, update.apply_pos
        );
        Ok(())
    }

    async fn on_hot_standby_feedback<C>(
        &self,
        _client: &mut C,
        feedback: HotStandbyFeedback,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        println!(
            "HotStandbyFeedback: global_xmin={} catalog_xmin={}",
            feedback.global_xmin, feedback.catalog_xmin
        );
        Ok(())
    }
}

struct ReplicationServerFactory {
    handler: Arc<DummyReplicationHandler>,
}

impl PgWireServerHandlers for ReplicationServerFactory {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }

    fn replication_handler(&self) -> Arc<impl ReplicationHandler> {
        self.handler.clone()
    }
}

#[tokio::main]
pub async fn main() {
    let factory = Arc::new(ReplicationServerFactory {
        handler: Arc::new(DummyReplicationHandler),
    });

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Replication server listening on {server_addr}");
    println!("Connect with: psql \"host=127.0.0.1 port=5432 replication=database\"");
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::spawn(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
