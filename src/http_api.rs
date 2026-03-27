use std::sync::Arc;

use crate::{
    encoding::{json2mp, mp2json},
    error::{self, AppError},
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
    doc_id: Option<Box<str>>,
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
    if let Some(doc_id) = req.doc_id {
        match state.db.get_doc(&db, &collection, doc_id.as_ref()).await? {
            Some(r) => {
                return Ok(Json(QueryResponse {
                    results: vec![mp2json(r)],
                }));
            }
            None => {
                return Ok(Json(QueryResponse {
                    results: Vec::new(),
                }));
            }
        }
    } else {
        return error::BadRequest {
            e: "specify doc_id",
        }
        .fail();
    }
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

    Ok(Json(SetResponse { id: id.into() }))
}

pub(crate) async fn collection_update(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    Path(collection): Path<String>,
    Json(req): Json<UpdateRequest>,
) -> Result<Json<UpdateResponse>, AppError> {
    todo!();
}
