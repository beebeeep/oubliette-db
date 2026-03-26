use std::sync::Arc;

use crate::error::{self, AppError};
use axum::{
    body::Bytes,
    extract::{Json, Path, State},
};
use foundationdb::{
    options::MutationType,
    tuple::{Subspace, Versionstamp},
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

const PK: &'static str = "pk";

#[derive(Deserialize)]
pub struct QueryRequest {}
#[derive(Serialize)]
pub struct QueryResponse {}

#[derive(Deserialize)]
pub struct UpdateRequest {}
#[derive(Serialize)]
pub struct UpdateResponse {}

#[derive(Deserialize)]
pub struct SetRequest {
    data: Box<str>,
}
#[derive(Serialize)]
pub struct SetResponse {
    id: Box<[u8]>,
}

pub struct AppState {
    pub fdb: foundationdb::Database,
}

/// sets raw key in FDB
pub async fn fdb_set(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
    body: Bytes,
) -> Result<(), AppError> {
    let tx = state.fdb.create_trx().context(error::Fdb {
        e: "starting transaction",
    })?;
    tx.set(key.as_bytes(), body.as_ref());
    tx.commit().await.context(error::FdbTransactionCommit)?;
    Ok(())
}

/// returns raw key from FDB
pub async fn fdb_get(
    State(state): State<Arc<AppState>>,
    Path(key): Path<String>,
) -> Result<Box<[u8]>, AppError> {
    let tx = state.fdb.create_trx().context(error::Fdb {
        e: "starting transaction",
    })?;

    match tx
        .get(key.as_bytes(), false)
        .await
        .context(error::Fdb { e: "getting data" })?
    {
        Some(data) => Ok(data.to_vec().into_boxed_slice()),
        None => Ok(Box::new([])),
    }
}

pub(crate) async fn collection_query(
    State(state): State<Arc<AppState>>,
    Path(db): Path<String>,
    Path(collection): Path<String>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, AppError> {
    todo!();
}

pub(crate) async fn collection_set(
    State(state): State<Arc<AppState>>,
    Path((db, collection)): Path<(String, String)>,
    Json(req): Json<SetRequest>,
) -> Result<Json<SetResponse>, AppError> {
    let subspace = Subspace::all().subspace(&(db, collection));
    let kt = (PK, &Versionstamp::incomplete(0));
    let key = subspace.pack_with_versionstamp(&kt);

    let tx = state.fdb.create_trx().context(error::Fdb {
        e: "starting transaction",
    })?;
    // tx.set(&key, req.data.as_bytes());
    tx.atomic_op(
        &key,
        req.data.as_bytes(),
        MutationType::SetVersionstampedKey,
    );
    let versiontstamp = tx.get_versionstamp();

    let _ = tx.commit().await.context(error::FdbTransactionCommit)?;
    let versionstamp = versiontstamp.await.context(error::Fdb {
        e: "getting versionstamp",
    })?;
    let versionstamp = versionstamp
        .as_ref()
        .try_into()
        .whatever_context("invalid versionstamp")?;
    let versionstamp = Versionstamp::complete(versionstamp, 0);

    /*
    let tx = state.fdb.create_trx().unwrap();
    let kt = (PK, &versionstamp);
    let r = tx.get(&subspace.pack(&kt), false).await.unwrap();
    println!("{:?}", r.unwrap().to_vec());
    */

    Ok(Json(SetResponse {
        id: Box::from(versionstamp.as_bytes().as_slice()),
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
