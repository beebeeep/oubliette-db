use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use foundationdb::RangeOption;
use futures::StreamExt;
use snafu::{ResultExt, whatever};
use tracing::{debug, error, info};

use crate::{
    document::Document,
    error::{self, AppError},
    schema::{Collection, IndexDef, InstanceSchema},
    values::Value,
};

const WORKER_PERIOD: Duration = Duration::from_secs(5);
const INDEXER_TIMEOUT: u128 = 10000; // 10 seconds
const MAX_OPS_PER_TX: usize = 10000;

pub(crate) struct Worker {
    fdb: foundationdb::Database,
    schema: Arc<tokio::sync::RwLock<InstanceSchema>>,
    db_path: String,
}

impl Worker {
    pub(crate) async fn start(
        path: &str,
        schema: Arc<tokio::sync::RwLock<InstanceSchema>>,
    ) -> Result<(), AppError> {
        info!("starting async worker");
        let fdb =
            foundationdb::Database::from_path(path).whatever_context("initializing database")?;
        let worker = Self {
            fdb,
            schema,
            db_path: String::from(path),
        };
        tokio::task::spawn(async move {
            loop {
                if let Err(e) = worker.run().await {
                    error!(error = format!("{e:?}"), "worker run failed");
                }
                tokio::time::sleep(WORKER_PERIOD).await;
            }
        });
        Ok(())
    }

    async fn run(&self) -> Result<(), AppError> {
        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let new_schema = InstanceSchema::load(&tx).await?;
        let mut schema = self.schema.write().await;
        *schema = new_schema;
        let current_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let schema_version = schema.version;
        for (collection, collection_schema) in schema.collections.iter_mut() {
            for (index_name, index_def) in collection_schema.indexes.iter_mut() {
                if index_def.ready {
                    continue;
                }
                if let Some(ts) = index_def.lock_timestamp {
                    if current_ts.saturating_sub(ts) < INDEXER_TIMEOUT {
                        continue;
                    }
                }
                let coll = collection.clone();
                let db_path = self.db_path.clone();
                let idx_name = String::from(index_name);
                tokio::task::spawn(async move {
                    if let Err(e) = materialize_index(db_path, schema_version, coll, idx_name).await
                    {
                        error!(error = format!("{e:?}"), "backfill job failed");
                    }
                });
            }
        }

        Ok(())
    }
}

async fn materialize_index(
    db_path: String,
    schema_version: u32,
    collection: Collection,
    index_name: String,
) -> Result<(), AppError> {
    info!(index = index_name, "materializing index");
    let fdb =
        foundationdb::Database::from_path(&db_path).whatever_context("initializing database")?;

    let mut total_doc_count = 0usize;
    loop {
        // first, load schema, find the index to materialize and lock it
        let tx = fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let mut schema = InstanceSchema::load(&tx).await?;
        let Some(collection_schema) = schema.collections.get_mut(&collection) else {
            whatever!("cannot find schema of collection {collection}")
        };
        let Some(index_def) = collection_schema.indexes.get_mut(&index_name) else {
            whatever!("cannot find index {index_name} in {collection}");
        };

        // next, materializing the index
        let tx = fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let (done, doc_count, last_indexed_key) =
            materialize_index_batch(&collection, index_def, &index_name, schema_version, &tx)
                .await?;
        total_doc_count += doc_count;

        if done {
            index_def.lock_timestamp = None;
            index_def.last_indexed_key = None;
        } else {
            index_def.lock_timestamp = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            );
            index_def.last_indexed_key = last_indexed_key;
        }
        schema.write_to_db(&tx).await?;
        tx.commit().await.context(error::FdbTransactionCommit {})?;
        if done {
            info!(
                index = index_name,
                document_count = total_doc_count,
                "index was successfully materialized"
            );
            return Ok(());
        }
    }
}

async fn materialize_index_batch(
    collection: &Collection,
    index_def: &IndexDef,
    index_name: &str,
    schema_version: u32,
    tx: &foundationdb::Transaction,
) -> Result<(bool, usize, Option<Vec<u8>>), AppError> {
    let coll_ss = collection.pk_subspace();
    let indexed_ss = coll_ss.subspace(&schema_version); // everything right to schema_version is already indexed
    let range = match &index_def.last_indexed_key {
        Some(k) => RangeOption::from((k.clone(), indexed_ss.range().0)),
        None => RangeOption::from((coll_ss.range().0, indexed_ss.range().0)),
    };
    let mut results = tx.get_ranges(range, false);
    let mut doc_count = 0usize;

    while let Some(values) = results.next().await {
        let values = values.context(error::Fdb {
            e: "scanning data to index",
        })?;
        'VALUES: for value in values.iter() {
            let doc = Document::try_from(value)?;
            let index_ss = collection.index_subspace(&index_name);

            // construct index key by appending all indexed field values to the root index key
            let Some(key) = index_def.get_key(index_ss, &doc) else {
                continue 'VALUES;
            };
            tx.set(&key, &[]);
            doc_count += 1;
            if doc_count > MAX_OPS_PER_TX {
                return Ok((false, doc_count, Some(Vec::from(value.key()))));
            }
        }
    }
    Ok((true, doc_count, None))
}
