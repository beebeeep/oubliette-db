use std::sync::Arc;

use crate::error::AppError;
use axum::Router;
use axum::routing::{get, patch, post, put};
use snafu::prelude::*;

mod error;
mod expression;
mod http_api;
mod planner;
// mod predicate;
mod schema;
mod storage;
mod values;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    // safety: we drop network at the end of main
    let network = unsafe { foundationdb::boot() };
    let db = storage::DB::from_path("./fdb.cluster").await?;
    let state = Arc::new(http_api::AppState { db });

    let app = Router::new()
        .route("/fdb/{key}", get(http_api::fdb_get))
        .route("/fdb/{key}", put(http_api::fdb_set))
        .route(
            "/{db}/{collection}/{doc_id}",
            get(http_api::collection_get_doc),
        )
        .route("/{db}/{collection}", post(http_api::collection_query))
        .route("/{db}/{collection}", put(http_api::collection_set))
        .route("/{db}/{collection}", patch(http_api::collection_update))
        .route(
            "/_manage/{db}/{collection}/create",
            post(http_api::create_collection),
        )
        .route(
            "/_manage/{db}/{collection}/create_index",
            post(http_api::add_index),
        )
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
