use std::sync::Arc;

use crate::db::*;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, patch, post, put};
use foundationdb::future::FdbSlice;
use foundationdb::options::TransactionOption;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::{Whatever, prelude::*};

mod db;
mod error;

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
        .route("/{db}/{collection}", post(collection_query))
        .route("/{db}/{collection}", put(collection_set))
        .route("/{db}/{collection}", patch(collection_update))
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
