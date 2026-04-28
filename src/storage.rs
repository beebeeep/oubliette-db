use std::{collections::HashMap, sync::Arc};

use crate::{
    document::{DocID, Document},
    error::{self, AppError, MPVDecode},
    planner::Plan,
    schema::{
        Collection, CollectionSchema, IndexDef, IndexField, InstanceSchema, KEY_INDEX, KEY_PK,
        SPACE_DATA, SchemaUpdate, SchemaVersion,
    },
    values::{self, Value},
    worker,
};
use foundationdb::{
    Transaction,
    options::MutationType,
    tuple::{Subspace, Versionstamp},
};
use futures::StreamExt;
use snafu::ResultExt;

pub(crate) struct DB {
    fdb: foundationdb::Database,
    schema: Arc<tokio::sync::RwLock<InstanceSchema>>,
}

impl DB {
    pub(crate) async fn from_path(path: &str) -> Result<Self, AppError> {
        let fdb =
            foundationdb::Database::from_path(path).whatever_context("initializing database")?;

        let tx = fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let s = InstanceSchema::load(&tx).await?;
        let db = Self {
            fdb,
            schema: Arc::new(tokio::sync::RwLock::new(s)),
        };

        worker::Worker::start(path, db.schema.clone()).await?;

        Ok(db)
    }

    /// sets raw key in FDB
    pub(crate) async fn fdb_set(&self, key: &[u8], value: &[u8]) -> Result<(), AppError> {
        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        tx.set(key, value);
        tx.commit().await.context(error::FdbTransactionCommit)?;
        Ok(())
    }

    /// returns raw key from FDB
    pub(crate) async fn fdb_get(&self, key: &[u8]) -> Result<Box<[u8]>, AppError> {
        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;

        match tx
            .get(key, false)
            .await
            .context(error::Fdb { e: "getting data" })?
        {
            Some(data) => Ok(data.to_vec().into_boxed_slice()),
            None => Ok(Box::new([])),
        }
    }

    pub(crate) async fn insert_doc(
        &self,
        db: &str,
        collection: &str,
        doc: rmpv::Value,
    ) -> Result<DocID, AppError> {
        let schema = self.schema.read().await;

        let collection = Collection::from((db, collection));
        let validation_result = schema.validate_doc(&collection, &doc)?;
        let schema = if let Some(updated_collection) = validation_result.updated_collection {
            // relock for write
            drop(schema);
            let mut schema = self.schema.write().await;
            schema
                .apply_schema_update(
                    &collection,
                    SchemaUpdate::UpdateCollection(updated_collection),
                    &self.fdb,
                )
                .await?;

            // relock back for reading
            drop(schema);
            self.schema.read().await
        } else {
            schema
        };

        let indexes = &schema
            .collections
            .get(&collection)
            .expect("collection should exist")
            .indexes;
        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;

        let kt = (schema.version, &Versionstamp::incomplete(0));
        let key = collection.pk_subspace().pack_with_versionstamp(&kt);

        let mut payload = Vec::with_capacity(64);
        rmpv::encode::write_value(&mut payload, &doc).context(error::MPVEncode {
            e: "encoding document",
        })?;
        tx.atomic_op(&key, &payload, MutationType::SetVersionstampedKey);

        if let Some(affected_indexes) = validation_result.affected_indexes {
            Self::write_indexes(
                &tx,
                schema.version,
                &collection,
                indexes,
                affected_indexes,
                &doc,
            )?;
        }

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

        Ok(DocID::new(schema.version, versionstamp))
    }

    fn write_indexes(
        tx: &foundationdb::Transaction,
        schema_version: SchemaVersion,
        collection: &Collection,
        indexes: &HashMap<String, IndexDef>,
        affected_indexes: Vec<String>,
        doc: &rmpv::Value,
    ) -> Result<(), AppError> {
        'NEXT_INDEX: for index in affected_indexes {
            let mut idx_subspace = collection.index_subspace(&index);
            let index_def = indexes.get(&index).expect("index should exist");
            for (field, prefix_len) in &index_def.fields {
                let Some(value) = Value::extract_field(&field, doc) else {
                    continue 'NEXT_INDEX;
                };
                // TODO: truncate string value to prefix_len
                idx_subspace = idx_subspace.subspace(value); // NOTE: this may panic if value is bad (like invalid UTF-8 for strings) or unsupported
                // match value.as_ref() {
                //     rmpv::Value::Boolean(b) => {
                //         idx_subspace = idx_subspace.subspace(b);
                //     }
                //     rmpv::Value::F32(f) => {
                //         idx_subspace = idx_subspace.subspace(f);
                //     }
                //     rmpv::Value::F64(f) => {
                //         idx_subspace = idx_subspace.subspace(f);
                //     }
                //     rmpv::Value::Integer(n) => {
                //         if let Some(n) = n.as_u64() {
                //             idx_subspace = idx_subspace.subspace(&n);
                //         }
                //         if let Some(n) = n.as_i64() {
                //             idx_subspace = idx_subspace.subspace(&n);
                //         }
                //     }
                //     rmpv::Value::String(s) => {
                //         if let Some(mut s) = s.as_str() {
                //             if let Some(prefix_len) = prefix_len {
                //                 s = &s[..s.floor_char_boundary(*prefix_len)]
                //             }
                //             idx_subspace = idx_subspace.subspace(&s);
                //         } else {
                //             continue 'NEXT_INDEX;
                //         }
                //     }
                //     _ => {
                //         continue 'NEXT_INDEX;
                //     }
                // };
            }
            let key = idx_subspace
                .pack_with_versionstamp(&(schema_version, &Versionstamp::incomplete(0)));
            tx.atomic_op(&key, &[], MutationType::SetVersionstampedKey);
        }
        Ok(())
    }

    pub(crate) async fn query(
        &self,
        db: &str,
        collection: &str,
        query: Option<&str>,
        plan: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Document>, AppError> {
        let mut query_result = Vec::with_capacity(1);
        let collection = Collection::from((db, collection));
        let schema = self.schema.read().await;

        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let Some(coll_schema) = schema.collections.get(&collection) else {
            error::BadRequest {
                e: "unknown collection",
            }
            .fail()?
        };
        let plan = match (query, plan) {
            (None, None) => error::BadRequest {
                e: "neither plan, nor query were provided",
            }
            .fail()?,
            (None, Some(plan)) => Plan::from_str(&collection, coll_schema, plan)?,
            (Some(query), _) => Plan::from_query(&collection, coll_schema, query)?,
        };

        let mut result = plan.execute(&tx);
        while let Some(doc) = result.next().await {
            query_result.push(doc?);
            if let Some(limit) = limit
                && query_result.len() > limit
            {
                break;
            }
        }
        Ok(query_result)
    }

    /// queries single doc by id
    pub(crate) async fn get_doc(
        &self,
        db: &str,
        collection: &str,
        id: impl TryInto<DocID, Error = AppError>,
    ) -> Result<Option<rmpv::Value>, AppError> {
        let id = id.try_into()?;
        let subspace = Subspace::all().subspace(&(SPACE_DATA, db, collection));
        let key = subspace.pack(&(KEY_PK, id.schema, id.versionstamp));
        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;

        match tx.get(&key, false).await.context(error::Fdb {
            e: "reading the document",
        })? {
            Some(data) => Ok(Some(rmpv::decode::read_value(&mut data.as_ref()).context(
                MPVDecode {
                    e: "decoding document",
                },
            )?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn get_doc_tx(
        collection: &Collection,
        id: &DocID,
        tx: &Transaction,
    ) -> Result<Option<Document>, AppError> {
        let key = collection
            .pk_subspace()
            .pack(&(id.schema, &id.versionstamp));
        match tx.get(&key, false).await.context(error::Fdb {
            e: "reading document",
        })? {
            Some(data) => Ok(Some(Document {
                value: rmpv::decode::read_value(&mut data.as_ref()).context(MPVDecode {
                    e: "decoding document",
                })?,
                id: id.clone(),
            })),
            None => Ok(None),
        }
    }

    pub(crate) async fn create_collection(
        &self,
        db: &str,
        collection: &str,
    ) -> Result<(), AppError> {
        let mut schema = self.schema.write().await;

        schema
            .apply_schema_update(
                &Collection::from((db, collection)),
                SchemaUpdate::UpdateCollection(CollectionSchema::default()),
                &self.fdb,
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn create_index(
        &self,
        db: &str,
        collection: &str,
        name: &str,
        fields: Vec<IndexField>,
    ) -> Result<(), AppError> {
        let mut schema = self.schema.write().await;
        schema
            .apply_schema_update(
                &Collection::from((db, collection)),
                SchemaUpdate::CreateIndex((
                    String::from(name),
                    IndexDef {
                        fields: fields.clone(),
                        ready: false,
                        lock_timestamp: None,
                        last_indexed_key: None,
                    },
                )),
                &self.fdb,
            )
            .await?;
        Ok(())
    }
}

#[allow(dead_code)]
fn dump_key(key: &[u8]) -> String {
    let mut r = String::new();
    for b in key {
        if b.is_ascii_alphanumeric() {
            r.push(*b as char);
        } else {
            r.push_str(&format!("\\{b:02x}"));
        }
    }
    r
}

#[cfg(test)]
mod tests {
    use crate::document::DocID;

    #[test]
    fn test_doc_id() {
        let d = DocID::try_from("04000000000000003280028900000000").expect("failed to parse docID");
        assert_eq!(d.schema, 4);
    }
}
