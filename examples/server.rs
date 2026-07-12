use std::sync::Arc;

use tokio::net::TcpListener;

use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::StartupHandler;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::tokio::process_socket;

mod common;

impl PgWireServerHandlers for common::DummyProcessorFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.handler.clone()
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn main() {
    // Handler futures are only `Send` for `Send` clients; these demos
    // drive every connection on the accept thread via a LocalSet.
    tokio::task::LocalSet::new().run_until(main_impl()).await
}

async fn main_impl() {
    let factory = Arc::new(common::DummyProcessorFactory::new());

    let server_addr = "127.0.0.1:5432";
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let factory_ref = factory.clone();
        tokio::task::spawn_local(async move { process_socket(incoming_socket.0, None, factory_ref).await });
    }
}
