use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, put};
use foundationdb::future::FdbSlice;
use foundationdb::options::TransactionOption;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{Whatever, prelude::*};

struct AppState {
    fdb: foundationdb::Database,
}

async fn fdb_set(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<(), StatusCode> {
    let tx = match state.fdb.create_trx() {
        Ok(v) => v,
        Err(e) => return Err(StatusCode::BAD_GATEWAY),
    };
    tx.set(key.as_bytes(), body.as_ref());
    match tx.commit().await {
        Ok(_) => Ok(()),
        Err(e) => Err(StatusCode::BAD_GATEWAY),
    }
}
async fn fdb_get(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Box<[u8]>, StatusCode> {
    let tx = match state.fdb.create_trx() {
        Ok(v) => v,
        Err(e) => return Err(StatusCode::BAD_GATEWAY),
    };
    if let Err(_) = tx.set_option(TransactionOption::Timeout(300)) {
        return Err(StatusCode::BAD_REQUEST);
    }

    println!("GET {key}");
    match tx.get(key.as_bytes(), false).await {
        Ok(Some(data)) => Ok(data.to_vec().into_boxed_slice()),
        Ok(None) => Ok(Box::new([])),
        Err(e) => {
            println!("{e:?}");
            Err(StatusCode::BAD_GATEWAY)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Whatever> {
    // safety: we drop network at the end of main
    let network = unsafe { foundationdb::boot() };
    let fdb = foundationdb::Database::from_path("./fdb.cluster")
        .whatever_context("connecting to foundationDB")?;
    let state = Arc::new(AppState { fdb });

    let app = Router::new()
        .route("/fdb/{key}", get(fdb_get))
        .route("/fdb/{key}", put(fdb_set))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("localhost:4800")
        .await
        .whatever_context("binding port")?;
    axum::serve(listener, app)
        .await
        .whatever_context("running the server")?;

    drop(network);

    Ok(())
}
