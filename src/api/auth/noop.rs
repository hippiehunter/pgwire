use std::fmt::Debug;

use futures::sink::{Sink, SinkExt};

use super::{ClientInfo, DefaultServerParameterProvider, StartupHandler};
use crate::api::PgWireConnectionState;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ReadyForQuery, TransactionStatus};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Marker trait for handlers that skip authentication. The blanket impl
/// below provides `StartupHandler` for every implementor; a supertrait
/// bound here would make that impl's opaque return type normalize through
/// its own where-clause (a cycle async-fn-in-trait cannot express).
#[allow(async_fn_in_trait)]
pub trait NoopStartupHandler: Send + Sync {
    async fn post_startup<C>(
        &self,
        _client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Ok(())
    }
}

impl<H> StartupHandler for H
where
    H: NoopStartupHandler,
{
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(ref startup) = message {
            super::protocol_negotiation(client, startup).await?;
            super::save_startup_parameters_to_metadata(client, startup);
            super::finish_authentication0(client, &DefaultServerParameterProvider::default())
                .await?;

            self.post_startup(client, message).await?;

            client
                .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    TransactionStatus::Idle,
                )))
                .await?;
            client.set_state(PgWireConnectionState::ReadyForQuery);
        }

        Ok(())
    }
}

impl NoopStartupHandler for crate::api::NoopHandler {}
