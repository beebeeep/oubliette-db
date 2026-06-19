use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::{
    document::{DocID, Document},
    error::{self, AppError, MPVDecode},
    expression::Update,
    planner::Plan,
    schema::{
        Collection, CollectionSchema, IndexDef, IndexField, InstanceSchema, KEY_PK, SPACE_DATA,
        SchemaUpdate, SchemaVersion,
    },
    values::Value,
    worker,
};
use foundationdb::{
    RangeOption, Transaction,
    options::MutationType,
    tuple::{self, Subspace, Versionstamp},
};
use futures::StreamExt;
use snafu::ResultExt;
use tracing::debug;

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
        doc: Value,
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

        let mut doc = Document {
            id: DocID::incomplete(schema.version),
            value: doc,
        };
        let key = collection.pk_subspace().pack_with_versionstamp(&doc.id);

        let mut payload = Vec::with_capacity(64);
        rmpv::encode::write_value(&mut payload, &doc.value.0).context(error::MPVEncode {
            e: "encoding document",
        })?;
        tx.atomic_op(&key, &payload, MutationType::SetVersionstampedKey);

        if let Some(affected_indexes) = validation_result.affected_indexes {
            Self::write_indexes(&tx, &collection, indexes, &affected_indexes, &doc)?;
        }

        let versiontstamp = tx.get_versionstamp();
        let _ = tx
            .commit()
            .await
            .context(error::FdbTransactionCommit)
            .inspect_err(|e| {
                tracing::error!(
                    err = ?e,
                    "document insert failed to commit transaction"
                )
            })?;
        let versionstamp = versiontstamp.await.context(error::Fdb {
            e: "getting versionstamp",
        })?;
        let versionstamp = versionstamp
            .as_ref()
            .try_into()
            .whatever_context("invalid versionstamp")?;
        doc.id.versionstamp = Versionstamp::complete(versionstamp, 0);

        Ok(doc.id)
    }
    pub(crate) async fn dump_index(
        &self,
        db: &str,
        collection: &str,
        index: &str,
    ) -> Result<String, AppError> {
        // let collection = Collection::from((db, collection));
        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let mut dump = String::new();

        {
            // let range = RangeOption::from(&collection.index_subspace(index));
            let range = RangeOption::from(&Subspace::all().subspace(&(SPACE_DATA, db, collection)));
            let mut results = tx.get_ranges_keyvalues(range, false);
            while let Some(kv) = results.next().await {
                let kv = kv.context(error::Fdb { e: "dumping index" })?;
                let e: Vec<tuple::Element> =
                    tuple::unpack(kv.key()).context(error::FdbTupleUnpack)?;
                eprintln!("{e:?}");
                dump.push_str(&format!("fdb entry: {:?}\n", e));
            }
        }

        let _ = tx.commit().await.context(error::FdbTransactionCommit)?;

        Ok(dump)
    }

    fn write_indexes(
        tx: &foundationdb::Transaction,
        collection: &Collection,
        indexes: &HashMap<Box<str>, IndexDef>,
        affected_indexes: &[Box<str>],
        doc: &Document,
    ) -> Result<(), AppError> {
        'NEXT_INDEX: for index in affected_indexes {
            let idx_subspace = collection.index_subspace(&index);
            let index_def = indexes.get(index).expect("index should exist");
            // for (field, _prefix_len) in &index_def.fields {
            //     let Some(value) = doc.extract_field(&field) else {
            //         continue 'NEXT_INDEX;
            //     };
            //     // TODO: truncate string value to prefix_len
            //     idx_subspace = idx_subspace.subspace(value); // NOTE: this may panic if value is bad (like invalid UTF-8 for strings) or unsupported
            // }
            // let key = idx_subspace.pack_with_versionstamp(&DocID::incomplete(schema_version));
            let Some(subspace) = index_def.subspace(idx_subspace, &doc.value) else {
                continue 'NEXT_INDEX;
            };
            eprintln!("updating index {index} subspace {subspace:?}");
            let key = if doc.id.versionstamp.is_complete() {
                subspace.pack(&doc.id)
            } else {
                subspace.pack_with_versionstamp(&doc.id)
            };
            tx.atomic_op(&key, &[], MutationType::SetVersionstampedKey);
        }
        Ok(())
    }

    fn delete_indexes<'a>(
        tx: &foundationdb::Transaction,
        collection: &Collection,
        indexes: &HashMap<Box<str>, IndexDef>,
        affected_indexes: &[Box<str>],
        doc: &Document,
    ) -> Result<(), AppError> {
        'NEXT_INDEX: for index in affected_indexes {
            let idx_subspace = collection.index_subspace(&index);
            let index_def = indexes.get(index).expect("index should exist");
            let Some(subspace) = index_def.subspace(idx_subspace, &doc.value) else {
                continue 'NEXT_INDEX;
            };
            eprintln!(
                "deleting index {index} subspace {subspace:?} value {:?}",
                doc.value
            );
            let key = subspace.pack(&doc.id);
            tx.clear(&key);
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

    pub(crate) async fn update(
        &self,
        db: &str,
        collection: &str,
        query: Option<&str>,
        plan: Option<&str>,
        update: &str,
    ) -> Result<usize, AppError> {
        let collection_subspace = Subspace::all().subspace(&(SPACE_DATA, db, collection));
        let collection = Collection::from((db, collection));
        let schema = self.schema.read().await;
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
        let update = Update::try_from(update)?;

        let mut affected = 0;
        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        {
            // execute the query, iterate over its results, apply update and insert updated document back
            let mut result = plan.execute(&tx);
            let affected_indexes = update.get_affected_indexes(coll_schema);
            while let Some(doc) = result.next().await {
                affected += 1;
                let mut doc = doc?;
                Self::delete_indexes(
                    &tx,
                    &collection,
                    &coll_schema.indexes,
                    &affected_indexes,
                    &doc,
                )?;
                let drop = update.apply(&mut doc)?;
                let key = collection_subspace.pack(&(KEY_PK, &doc.id.schema, &doc.id.versionstamp));
                if drop {
                    self.drop_document(&schema, &collection, &key, &doc, &tx);
                    continue;
                }
                let validation_result = schema.validate_doc(&collection, &doc.value)?;
                if validation_result.updated_collection.is_some() {
                    error::BadRequest {
                        e: "update cannot change collection schema",
                    }
                    .fail()?;
                }
                Self::write_indexes(
                    &tx,
                    &collection,
                    &coll_schema.indexes,
                    &affected_indexes,
                    &doc,
                )?;
                let mut payload = Vec::with_capacity(64);
                rmpv::encode::write_value(&mut payload, &doc.value.0).context(
                    error::MPVEncode {
                        e: "encoding document",
                    },
                )?;
                tx.set(&key, &payload);
            }
        }
        let _ = tx.commit().await.context(error::FdbTransactionCommit)?;
        Ok(affected)
    }

    /// queries single doc by id
    pub(crate) async fn get_doc(
        &self,
        db: &str,
        collection: &str,
        id: impl TryInto<DocID, Error = AppError>,
    ) -> Result<Option<rmpv::Value>, AppError> {
        let id = id.try_into()?;
        // avoid building Collection to save on boxing stuff
        let key = Subspace::all()
            .subspace(&(SPACE_DATA, db, collection, KEY_PK))
            .pack(&id);
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
                value: rmpv::decode::read_value(&mut data.as_ref())
                    .context(MPVDecode {
                        e: "decoding document",
                    })?
                    .into(),
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
        for field in &fields {
            if !field.0.starts_with(".") {
                error::BadRequest {
                    e: format!(
                        "invalid field name '{}', field name must start from dot, e.g. .foo",
                        field.0
                    ),
                }
                .fail()?;
            }
        }
        schema
            .apply_schema_update(
                &Collection::from((db, collection)),
                SchemaUpdate::CreateIndex((
                    Box::from(name),
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

    fn drop_document(
        &self,
        schema: &InstanceSchema,
        collection: &Collection,
        key: &[u8],
        doc: &Document,
        tx: &Transaction,
    ) {
        tx.clear(key);
        /*
        let Some(col_schema) = schema.collections.get(collection) else {
            return;
        };
        for (idx_name, idx) in &col_schema.indexes {
            let mut subspace = collection.index_subspace(&idx_name);
            for (field, _size) in &idx.fields {
                let Some(value) = doc.value.extract_field(field) else {
                    continue;
                };
                subspace = subspace.subspace(value);
            }
        }
        */
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
