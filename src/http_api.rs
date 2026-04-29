use std::sync::Arc;

use crate::{
    error::{self, AppError},
    storage,
    values::{Value, json2mp, mp2json},
};
use axum::{
    Router,
    body::Bytes,
    extract::{Json, Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, patch, post, put},
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Serialize, Deserialize)]
pub struct QueryRequest {
    pub query: Option<Box<str>>,
    pub plan: Option<Box<str>>,
    pub limit: Option<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct QueryResponse {
    results: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateRequest {}
#[derive(Serialize, Deserialize)]
pub struct UpdateResponse {}

#[derive(Serialize, Deserialize)]
pub struct SetRequest {
    pub docs: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize)]
pub struct SetResponse {
    pub ids: Vec<Box<str>>,
}

#[derive(Serialize, Deserialize)]
pub struct IndexField {
    pub field: Box<str>,
    pub prefix_length: Option<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct AddIndexRequest {
    pub name: Box<str>,
    pub fields: Vec<IndexField>,
}

#[derive(Serialize, Deserialize)]
pub struct AddIndexResponse {}

#[derive(Serialize, Deserialize)]
pub struct CreateCollectionRequest {}

#[derive(Serialize, Deserialize)]
pub struct CreateCollectionResponse {}

#[derive(Serialize, Deserialize)]
pub struct GetDocResponse {
    pub doc: Option<serde_json::Value>,
}

struct AppState {
    db: storage::DB,
}

pub async fn start(db_path: &str) -> Result<(), AppError> {
    // safety: we drop network at the end of main
    let network = unsafe { foundationdb::boot() };
    let db = storage::DB::from_path(db_path).await?;
    let state = Arc::new(AppState { db });

    let app = Router::new()
        .route("/fdb/{key}", get(fdb_get))
        .route("/fdb/{key}", put(fdb_set))
        .route("/{db}/{collection}/{doc_id}", get(collection_get_doc))
        .route("/{db}/{collection}", post(collection_query))
        .route("/{db}/{collection}", put(collection_set))
        .route("/{db}/{collection}", patch(collection_update))
        .route("/_manage/{db}/{collection}/create", post(create_collection))
        .route("/_manage/{db}/{collection}/create_index", post(add_index))
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

/// sets raw key in FDB
async fn fdb_set(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<(), AppError> {
    state.db.fdb_set(key.as_bytes(), &body).await
}

/// returns raw key from FDB
async fn fdb_get(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Box<[u8]>, AppError> {
    state.db.fdb_get(key.as_bytes()).await
}

async fn collection_get_doc(
    State(state): State<Arc<AppState>>,
    Path((db, collection, doc_id)): Path<(String, String, String)>,
) -> Result<Json<GetDocResponse>, AppError> {
    let doc = state.db.get_doc(&db, &collection, doc_id.as_str()).await?;
    Ok(Json(GetDocResponse {
        doc: doc.map(mp2json),
    }))
}

async fn collection_query(
    State(state): State<Arc<AppState>>,
    Path((db, collection)): Path<(String, String)>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, AppError> {
    let query_result = state
        .db
        .query(
            &db,
            &collection,
            req.query.as_deref(),
            req.plan.as_deref(),
            req.limit,
        )
        .await?;
    let mut results = Vec::with_capacity(query_result.len());
    for doc in query_result {
        if let rmpv::Value::Map(mut items) = doc.value {
            items.push((
                rmpv::Value::from("__id"),
                rmpv::Value::from(String::from(&doc.id)),
            ));
            results.push(mp2json(rmpv::Value::Map(items)));
        }
    }
    Ok(Json(QueryResponse { results }))
}

async fn collection_set(
    State(state): State<Arc<AppState>>,
    Path((db, collection)): Path<(String, String)>,
    Json(req): Json<SetRequest>,
) -> Result<impl IntoResponse, AppError> {
    let docs: Vec<rmpv::Value> = req.docs.into_iter().map(|v| json2mp(v)).collect();
    let mut ids = Vec::with_capacity(docs.len());
    for doc in docs {
        ids.push(state.db.insert_doc(&db, &collection, doc).await?);
    }

    let ids = ids
        .into_iter()
        .map(|id| String::from(&id).into_boxed_str())
        .collect();
    Ok((StatusCode::CREATED, Json(SetResponse { ids })))
}

async fn create_collection(
    State(state): State<Arc<AppState>>,
    Path((db, collection)): Path<(String, String)>,
    Json(_req): Json<CreateCollectionRequest>,
) -> Result<Json<CreateCollectionResponse>, AppError> {
    state.db.create_collection(&db, &collection).await?;
    Ok(Json(CreateCollectionResponse {}))
}

async fn add_index(
    State(state): State<Arc<AppState>>,
    Path((db, collection)): Path<(String, String)>,
    Json(req): Json<AddIndexRequest>,
) -> Result<Json<AddIndexResponse>, AppError> {
    let fields = req
        .fields
        .into_iter()
        .map(|f| (f.field.into_string(), f.prefix_length))
        .collect();
    state
        .db
        .create_index(&db, &collection, &req.name, fields)
        .await?;
    Ok(Json(AddIndexResponse {}))
}

async fn collection_update(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    Path(collection): Path<String>,
    Json(req): Json<UpdateRequest>,
) -> Result<Json<UpdateResponse>, AppError> {
    todo!();
}
