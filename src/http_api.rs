use std::sync::Arc;

use crate::{
    encoding::{json2mp, mp2json},
    error::{self, AppError},
    predicate::Predicate,
    storage,
};
use axum::{
    body::Bytes,
    extract::{Json, Path, State},
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Deserialize)]
pub struct QueryRequest {
    query: Box<str>,
    limit: Option<usize>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    results: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct UpdateRequest {}
#[derive(Serialize)]
pub struct UpdateResponse {}

#[derive(Deserialize)]
pub struct SetRequest {
    doc: serde_json::Value,
}

#[derive(Serialize)]
pub struct SetResponse {
    id: Box<str>,
}

pub struct AppState {
    pub db: storage::DB,
}

/// sets raw key in FDB
pub async fn fdb_set(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<(), AppError> {
    state.db.fdb_set(key.as_bytes(), &body).await
}

/// returns raw key from FDB
pub async fn fdb_get(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Box<[u8]>, AppError> {
    state.db.fdb_get(key.as_bytes()).await
}

pub(crate) async fn collection_query(
    State(state): State<Arc<AppState>>,
    Path((db, collection)): Path<(String, String)>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, AppError> {
    let query_result = state
        .db
        .query(&db, &collection, &req.query, req.limit)
        .await?;
    let mut results = Vec::with_capacity(query_result.len());
    for doc in query_result {
        if let rmpv::Value::Map(mut items) = doc.doc {
            items.push((
                rmpv::Value::from("__id"),
                rmpv::Value::from(String::from(&doc.id)),
            ));
            results.push(mp2json(rmpv::Value::Map(items)));
        }
    }
    Ok(Json(QueryResponse { results }))
}

pub(crate) async fn collection_set(
    State(state): State<Arc<AppState>>,
    Path((db, collection)): Path<(String, String)>,
    Json(req): Json<SetRequest>,
) -> Result<Json<SetResponse>, AppError> {
    let id = state
        .db
        .insert_doc(&db, &collection, json2mp(req.doc))
        .await?;

    Ok(Json(SetResponse {
        id: String::from(&id).into_boxed_str(),
    }))
}

pub(crate) async fn collection_update(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    Path(collection): Path<String>,
    Json(req): Json<UpdateRequest>,
) -> Result<Json<UpdateResponse>, AppError> {
    todo!();
}
