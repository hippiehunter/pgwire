#![cfg(feature = "client-api")]
//! Integration tests for the replication protocol against a real PostgreSQL server.
//!
//! These tests require a PostgreSQL instance running on localhost:5555 with:
//! - `wal_level=logical`
//! - User `postgres` with password `postgres`
//! - REPLICATION privilege (default for superuser)
//!
//! Run with: `cargo test --test replication_integration --features client-api`

use std::sync::Arc;

use bytes::Buf;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::Config;
use pgwire::messages::data::DataRow;
use pgwire::messages::replication::{Lsn, ReplicationMessage, StandbyStatusUpdate};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::client::PgWireClient;

async fn connect_replication() -> PgWireClient {
    let config: Config =
        "host=127.0.0.1 port=5555 user=postgres password=postgres dbname=postgres replication=database sslmode=disable"
            .parse()
            .expect("valid config");
    let startup_handler = DefaultStartupHandler::new();
    PgWireClient::connect(Arc::new(config), startup_handler, None)
        .await
        .expect("failed to connect to PostgreSQL replication")
}

/// Extract text field values from a DataRow's raw bytes.
/// DataRow.data contains: for each field, i32 length (-1 = NULL) then that many bytes.
fn extract_text_fields(row: &DataRow) -> Vec<Option<String>> {
    let mut buf = row.data.clone();
    let mut fields = Vec::new();
    for _ in 0..row.field_count {
        let len = buf.get_i32();
        if len < 0 {
            fields.push(None);
        } else {
            let data = buf.split_to(len as usize);
            fields.push(Some(String::from_utf8_lossy(&data).to_string()));
        }
    }
    fields
}

/// Collect all messages until ReadyForQuery, returning (row_descriptions, data_rows, command_completes).
async fn collect_command_response(
    client: &mut PgWireClient,
) -> (
    Vec<pgwire::messages::data::RowDescription>,
    Vec<DataRow>,
    Vec<pgwire::messages::response::CommandComplete>,
) {
    let mut row_descs = Vec::new();
    let mut data_rows = Vec::new();
    let mut cmd_completes = Vec::new();

    loop {
        let msg = client.recv_message().await.expect("recv_message failed");
        match msg {
            PgWireBackendMessage::RowDescription(rd) => row_descs.push(rd),
            PgWireBackendMessage::DataRow(dr) => data_rows.push(dr),
            PgWireBackendMessage::CommandComplete(cc) => cmd_completes.push(cc),
            PgWireBackendMessage::ReadyForQuery(_) => break,
            PgWireBackendMessage::NoticeResponse(_) => continue,
            other => panic!("unexpected message: {other:?}"),
        }
    }

    (row_descs, data_rows, cmd_completes)
}

#[tokio::test]
async fn test_identify_system() {
    let mut client = connect_replication().await;

    client
        .send_replication_command("IDENTIFY_SYSTEM")
        .await
        .expect("send IDENTIFY_SYSTEM");

    let (row_descs, data_rows, cmd_completes) = collect_command_response(&mut client).await;

    // Should get exactly one RowDescription, one DataRow, one CommandComplete
    assert_eq!(row_descs.len(), 1, "expected 1 RowDescription");
    assert_eq!(data_rows.len(), 1, "expected 1 DataRow");
    assert_eq!(cmd_completes.len(), 1, "expected 1 CommandComplete");

    // RowDescription should have 4 fields: systemid, timeline, xlogpos, dbname
    let fields = &row_descs[0].fields;
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[0].name.as_str(), "systemid");
    assert_eq!(fields[1].name.as_str(), "timeline");
    assert_eq!(fields[2].name.as_str(), "xlogpos");
    assert_eq!(fields[3].name.as_str(), "dbname");

    // Parse the DataRow fields
    let row_fields = extract_text_fields(&data_rows[0]);
    assert_eq!(row_fields.len(), 4);
    for (i, field) in row_fields.iter().enumerate() {
        assert!(field.is_some(), "field {i} should not be null");
    }

    // Parse the xlogpos to verify it's a valid LSN
    let xlogpos_str = row_fields[2].as_ref().unwrap();
    let lsn: Lsn = xlogpos_str.parse().expect("xlogpos should be a valid LSN");
    assert!(lsn.is_valid(), "xlogpos should be non-zero");

    // Parse timeline
    let timeline_str = row_fields[1].as_ref().unwrap();
    let timeline: u32 = timeline_str.parse().expect("timeline should be u32");
    assert!(timeline >= 1, "timeline should be >= 1");

    println!("IDENTIFY_SYSTEM: xlogpos={lsn}, timeline={timeline}");
}

#[tokio::test]
async fn test_create_and_drop_replication_slot() {
    let mut client = connect_replication().await;

    let slot_name = "pgwire_test_slot_cd";

    // Create a logical replication slot
    client
        .send_replication_command(&format!(
            "CREATE_REPLICATION_SLOT {slot_name} TEMPORARY LOGICAL test_decoding"
        ))
        .await
        .expect("send CREATE_REPLICATION_SLOT");

    let (row_descs, data_rows, cmd_completes) = collect_command_response(&mut client).await;

    assert_eq!(row_descs.len(), 1, "expected 1 RowDescription");
    assert_eq!(data_rows.len(), 1, "expected 1 DataRow");
    assert_eq!(cmd_completes.len(), 1, "expected 1 CommandComplete");

    // RowDescription should have 4 fields: slot_name, consistent_point, snapshot_name, output_plugin
    let fields = &row_descs[0].fields;
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[0].name.as_str(), "slot_name");
    assert_eq!(fields[1].name.as_str(), "consistent_point");
    assert_eq!(fields[2].name.as_str(), "snapshot_name");
    assert_eq!(fields[3].name.as_str(), "output_plugin");

    // Parse the DataRow
    let row_fields = extract_text_fields(&data_rows[0]);

    // Verify slot_name matches
    assert_eq!(row_fields[0].as_ref().unwrap(), slot_name);

    // Verify consistent_point is a valid LSN
    let cp_str = row_fields[1].as_ref().unwrap();
    let cp_lsn: Lsn = cp_str.parse().expect("consistent_point should be valid LSN");
    assert!(cp_lsn.is_valid(), "consistent_point should be non-zero");

    // Verify output_plugin
    let plugin = row_fields[3].as_ref().unwrap();
    assert_eq!(plugin, "test_decoding");

    println!("CREATE_REPLICATION_SLOT: {slot_name} at {cp_lsn}, plugin={plugin}");

    // Drop the slot
    client
        .send_replication_command(&format!("DROP_REPLICATION_SLOT {slot_name}"))
        .await
        .expect("send DROP_REPLICATION_SLOT");

    let (_row_descs, _data_rows, cmd_completes) = collect_command_response(&mut client).await;
    assert_eq!(cmd_completes.len(), 1, "expected 1 CommandComplete for DROP");

    println!("DROP_REPLICATION_SLOT: {slot_name} dropped successfully");
}

#[tokio::test]
async fn test_start_replication_and_stream() {
    let mut client = connect_replication().await;

    let slot_name = "pgwire_test_slot_stream";

    // Create a temporary logical replication slot
    client
        .send_replication_command(&format!(
            "CREATE_REPLICATION_SLOT {slot_name} TEMPORARY LOGICAL test_decoding"
        ))
        .await
        .unwrap();

    let (_, data_rows, _) = collect_command_response(&mut client).await;
    let row_fields = extract_text_fields(&data_rows[0]);
    let start_lsn: Lsn = row_fields[1].as_ref().unwrap().parse().unwrap();

    println!("Created slot {slot_name} at {start_lsn}");

    // Start replication from the consistent point
    client
        .send_replication_command(&format!(
            "START_REPLICATION SLOT {slot_name} LOGICAL {start_lsn}"
        ))
        .await
        .unwrap();

    // Expect CopyBothResponse
    let msg = client.recv_message().await.unwrap();
    match msg {
        PgWireBackendMessage::CopyBothResponse(_) => {
            println!("Received CopyBothResponse — streaming started");
        }
        other => panic!("expected CopyBothResponse, got: {other:?}"),
    }

    // We should eventually receive a keepalive from the server.
    // Since there's no WAL activity, we'll get a keepalive first.
    // Use a timeout to avoid hanging forever.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.recv_replication_message(),
    )
    .await;

    match result {
        Ok(Ok(Some(ReplicationMessage::PrimaryKeepalive(ka)))) => {
            println!(
                "Received PrimaryKeepalive: wal_end={}, reply_requested={}",
                ka.wal_end, ka.reply_requested
            );
            // Send a standby status update in response
            let update = StandbyStatusUpdate::new(ka.wal_end, ka.wal_end, ka.wal_end, 0, false);
            client
                .send_standby_status_update(&update)
                .await
                .expect("send StandbyStatusUpdate");
            println!("Sent StandbyStatusUpdate confirming {}", ka.wal_end);
        }
        Ok(Ok(Some(ReplicationMessage::XLogData(xlog)))) => {
            println!(
                "Received XLogData: start={}, end={}, {} bytes",
                xlog.start_lsn,
                xlog.end_lsn,
                xlog.data.len()
            );
        }
        Ok(Ok(None)) => {
            println!("Stream ended (CopyDone) — no WAL data");
        }
        Ok(Err(e)) => {
            panic!("Error receiving replication message: {e:?}");
        }
        Ok(Ok(Some(other))) => {
            println!("Received unexpected replication message: {other:?}");
        }
        Err(_) => {
            // Timeout is OK — no WAL activity means the server might not send
            // anything for a while. The important thing is that CopyBothResponse
            // was received and the connection is in streaming mode.
            println!("Timeout waiting for replication message (expected — no WAL activity)");

            // Even though we timed out, send a status update to confirm the
            // protocol works bidirectionally
            let update =
                StandbyStatusUpdate::new(start_lsn, start_lsn, start_lsn, 0, false);
            client
                .send_standby_status_update(&update)
                .await
                .expect("send StandbyStatusUpdate after timeout");
            println!("Sent StandbyStatusUpdate after timeout");
        }
    }
}

#[tokio::test]
async fn test_logical_replication_with_data_changes() {
    let mut client = connect_replication().await;

    let slot_name = "pgwire_test_slot_data";

    // Create a temporary logical replication slot
    client
        .send_replication_command(&format!(
            "CREATE_REPLICATION_SLOT {slot_name} TEMPORARY LOGICAL test_decoding"
        ))
        .await
        .unwrap();

    let (_, data_rows, _) = collect_command_response(&mut client).await;
    let row_fields = extract_text_fields(&data_rows[0]);
    let start_lsn: Lsn = row_fields[1].as_ref().unwrap().parse().unwrap();

    // Generate WAL data via a separate non-replication connection
    let normal_config: Config =
        "host=127.0.0.1 port=5555 user=postgres password=postgres dbname=postgres sslmode=disable"
            .parse()
            .unwrap();
    let startup_handler = DefaultStartupHandler::new();
    let mut normal_client = PgWireClient::connect(Arc::new(normal_config), startup_handler, None)
        .await
        .expect("normal connection");

    // Helper to send SQL and drain response
    async fn exec_sql(client: &mut PgWireClient, sql: &str) {
        client.send_replication_command(sql).await.unwrap();
        loop {
            let msg = client.recv_message().await.unwrap();
            if matches!(msg, PgWireBackendMessage::ReadyForQuery(_)) {
                break;
            }
        }
    }

    // Create table and insert data
    exec_sql(
        &mut normal_client,
        "DROP TABLE IF EXISTS pgwire_repl_test",
    )
    .await;
    exec_sql(
        &mut normal_client,
        "CREATE TABLE pgwire_repl_test (id int PRIMARY KEY, val text)",
    )
    .await;
    exec_sql(
        &mut normal_client,
        "INSERT INTO pgwire_repl_test VALUES (1, 'hello')",
    )
    .await;

    // Now start replication to read the changes
    client
        .send_replication_command(&format!(
            "START_REPLICATION SLOT {slot_name} LOGICAL {start_lsn}"
        ))
        .await
        .unwrap();

    // Expect CopyBothResponse
    let msg = client.recv_message().await.unwrap();
    assert!(
        matches!(msg, PgWireBackendMessage::CopyBothResponse(_)),
        "expected CopyBothResponse, got: {msg:?}"
    );

    // Collect XLogData messages with a timeout
    let mut xlog_messages = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }

        match tokio::time::timeout(remaining, client.recv_replication_message()).await {
            Ok(Ok(Some(ReplicationMessage::XLogData(xlog)))) => {
                println!(
                    "XLogData: start={}, end={}, data={}",
                    xlog.start_lsn,
                    xlog.end_lsn,
                    String::from_utf8_lossy(&xlog.data)
                );
                xlog_messages.push(xlog);
            }
            Ok(Ok(Some(ReplicationMessage::PrimaryKeepalive(ka)))) => {
                println!("PrimaryKeepalive: wal_end={}", ka.wal_end);
                // Always reply to keepalives to keep the connection alive
                let update = StandbyStatusUpdate::new(
                    ka.wal_end, ka.wal_end, ka.wal_end, 0, false,
                );
                client.send_standby_status_update(&update).await.unwrap();
                // If we already have XLogData, a keepalive signals end of batch
                if !xlog_messages.is_empty() {
                    break;
                }
                // Otherwise keep waiting for XLogData
                continue;
            }
            Ok(Ok(None)) => {
                println!("Stream ended");
                break;
            }
            Ok(Err(e)) => {
                panic!("Error: {e:?}");
            }
            Ok(Ok(Some(_other))) => {
                // Skip unexpected message types
                continue;
            }
            Err(_) => {
                // Timeout
                break;
            }
        }
    }

    // We should have received some XLogData messages containing the table
    // creation and insert (via test_decoding output plugin)
    assert!(
        !xlog_messages.is_empty(),
        "expected at least one XLogData message from the WAL changes"
    );

    // test_decoding output typically contains text like "BEGIN", "COMMIT",
    // "table public.pgwire_repl_test: INSERT: ..."
    let all_data: String = xlog_messages
        .iter()
        .map(|x| String::from_utf8_lossy(&x.data).to_string())
        .collect::<Vec<_>>()
        .join("\n");

    println!("All XLogData:\n{all_data}");

    // Verify LSNs are monotonically increasing
    for window in xlog_messages.windows(2) {
        assert!(
            window[0].start_lsn <= window[1].start_lsn,
            "XLogData LSNs should be monotonically increasing"
        );
    }

    // Verify each XLogData has valid LSNs
    for xlog in &xlog_messages {
        assert!(xlog.start_lsn.is_valid());
        assert!(xlog.end_lsn.is_valid());
        assert!(xlog.end_lsn >= xlog.start_lsn);
    }

    // Clean up: drop the test table via normal connection
    exec_sql(
        &mut normal_client,
        "DROP TABLE IF EXISTS pgwire_repl_test",
    )
    .await;

    println!("test_logical_replication_with_data_changes passed!");
}

#[tokio::test]
async fn test_timeline_history() {
    let mut client = connect_replication().await;

    // TIMELINE_HISTORY for timeline 1 — on a fresh server this will error
    // because there's no history for timeline 1. That's OK — we verify the
    // protocol exchange works correctly in either case.
    client
        .send_replication_command("TIMELINE_HISTORY 1")
        .await
        .expect("send TIMELINE_HISTORY");

    let msg = client.recv_message().await.unwrap();
    match msg {
        PgWireBackendMessage::ErrorResponse(_err) => {
            println!(
                "TIMELINE_HISTORY 1 returned error (expected for timeline 1 on fresh install)"
            );
            // Read ReadyForQuery after error
            let rfq = client.recv_message().await.unwrap();
            assert!(
                matches!(rfq, PgWireBackendMessage::ReadyForQuery(_)),
                "expected ReadyForQuery after error"
            );
        }
        PgWireBackendMessage::RowDescription(rd) => {
            println!(
                "TIMELINE_HISTORY 1 returned RowDescription with {} fields",
                rd.fields.len()
            );
            // Consume the rest
            loop {
                let msg = client.recv_message().await.unwrap();
                if matches!(msg, PgWireBackendMessage::ReadyForQuery(_)) {
                    break;
                }
            }
        }
        other => {
            panic!("unexpected response to TIMELINE_HISTORY: {other:?}");
        }
    }
}

#[tokio::test]
async fn test_regular_sql_on_replication_connection() {
    // Logical replication connections can also execute regular SQL
    let mut client = connect_replication().await;

    use pgwire::api::client::query::DefaultSimpleQueryHandler;

    let handler = DefaultSimpleQueryHandler::new();
    let results = client
        .simple_query(handler, "SELECT 1 AS num, 'hello' AS greeting")
        .await
        .expect("simple_query on replication connection");

    assert_eq!(results.len(), 1, "expected 1 result set");
    println!("Regular SQL on replication connection works!");
}
