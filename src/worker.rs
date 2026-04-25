use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use foundationdb::RangeOption;
use futures::StreamExt;
use snafu::ResultExt;

use crate::{
    document::Document,
    error::{self, AppError},
    schema::{Collection, IndexDef, InstanceSchema},
};

const WORKER_PERIOD: Duration = Duration::from_secs(5);
const INDEXER_TIMEOUT: u128 = 10000; // 10 seconds

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
                    eprintln!("worker run error: {e}");
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

        let mut write_schema = false;
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
                let idx_def = index_def.clone();
                let last_indexed_key = index_def.last_indexed_key.clone();
                tokio::task::spawn(async move {
                    if let Err(e) = materialize_index(
                        db_path,
                        schema_version,
                        coll,
                        idx_name,
                        idx_def,
                        last_indexed_key,
                    )
                    .await
                    {
                        eprintln!("backfill job returned error: {e}");
                    }
                });
                index_def.lock_timestamp = Some(current_ts);
                write_schema = true;
            }
        }

        if write_schema {
            schema.write_to_db(&tx).await?;
            tx.commit().await.context(error::FdbTransactionCommit {})?;
        }

        Ok(())
    }
}

async fn materialize_index(
    db_path: String,
    schema_version: u32,
    collection: Collection,
    index_name: String,
    index_def: IndexDef,
    last_indexed_key: Option<Vec<u8>>,
) -> Result<(), AppError> {
    let fdb =
        foundationdb::Database::from_path(&db_path).whatever_context("initializing database")?;
    let tx = fdb.create_trx().context(error::Fdb {
        e: "starting transaction",
    })?;
    let coll_ss = collection.pk_subspace();
    let indexed_ss = coll_ss.subspace(&schema_version); // everything right to schema_version is already indexed
    let range = match last_indexed_key {
        Some(k) => RangeOption::from((k, indexed_ss.range().0)),
        None => RangeOption::from((coll_ss.range().0, indexed_ss.range().0)),
    };
    let mut results = tx.get_ranges(range, false);
    while let Some(values) = results.next().await {
        let values = values.context(error::Fdb {
            e: "scanning data to index",
        })?;
        for value in values {
            let doc = Document::try_from(value)?;
        }
    }

    Ok(())
}
