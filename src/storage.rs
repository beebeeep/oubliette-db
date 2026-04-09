use std::sync::Arc;

use crate::{
    error::{self, AppError, MPVDecode},
    predicate::Predicate,
    schema::{CollectionSchema, InstanceSchema, KEY_PK, SPACE_DATA, SchemaVersion},
};
use foundationdb::{
    RangeOption,
    options::MutationType,
    tuple::{self, Subspace, Versionstamp},
};
use futures::StreamExt;
use snafu::ResultExt;

pub(crate) struct DocID {
    schema: SchemaVersion,
    versionstamp: Versionstamp,
}
impl DocID {
    fn new(schema: SchemaVersion, versionstamp: Versionstamp) -> Self {
        Self {
            schema,
            versionstamp,
        }
    }
}

pub(crate) struct DB {
    fdb: foundationdb::Database,
    schema: Arc<tokio::sync::RwLock<InstanceSchema>>,
}

pub(crate) struct Document {
    pub(crate) id: DocID,
    pub(crate) doc: rmpv::Value,
}

impl From<&DocID> for String {
    fn from(d: &DocID) -> Self {
        let mut s = hex::encode(d.schema.to_le_bytes());
        s.push_str(&hex::encode(d.versionstamp.as_bytes()));
        s
    }
}

impl TryFrom<&str> for DocID {
    type Error = AppError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 2 * size_of::<SchemaVersion>() + 12 {
            return Err(AppError::DocIDDecode {
                source: hex::FromHexError::InvalidStringLength,
            });
        }
        let schema_bytes: [u8; 4] = hex::decode(&value[..2 * size_of::<SchemaVersion>()])
            .context(error::DocIDDecode)?
            .try_into()
            .unwrap();
        let vs_bytes: [u8; 12] = hex::decode(&value[2 * size_of::<SchemaVersion>()..])
            .context(error::DocIDDecode)?
            .try_into()
            .unwrap();
        Ok(Self {
            schema: SchemaVersion::from_le_bytes(schema_bytes),
            versionstamp: Versionstamp::from(vs_bytes),
        })
    }
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
        let validation_result = {
            let schema = self.schema.write().await;
            schema.validate_doc(db, collection, &doc)?
        };

        let schema_version = if let Some(updated_collection) = validation_result.updated_collection
        {
            let mut schema = self.schema.write().await;
            schema
                .apply_schema_update(db, collection, Some(updated_collection), &self.fdb)
                .await?
        } else {
            validation_result.schema_version
        };

        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;

        let subspace = Subspace::all().subspace(&(SPACE_DATA, db, collection));
        let kt = (KEY_PK, schema_version, &Versionstamp::incomplete(0));
        let key = subspace.pack_with_versionstamp(&kt);

        let mut payload = Vec::with_capacity(64);
        rmpv::encode::write_value(&mut payload, &doc).context(error::MPVEncode {
            e: "encoding document",
        })?;
        tx.atomic_op(&key, &payload, MutationType::SetVersionstampedKey);
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

        Ok(DocID::new(schema_version, versionstamp))
    }

    pub(crate) async fn query(
        &self,
        db: &str,
        collection: &str,
        query: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Document>, AppError> {
        let p = Predicate::from_query(query).whatever_context("parsing query")?;
        let mut query_result = Vec::with_capacity(1);

        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        // TODO: implement query planner lol
        // doing fullscan instead
        let subspace = Subspace::all().subspace(&(db, collection, KEY_PK));
        let opts = RangeOption::from(&subspace);
        let mut results = tx.get_ranges(opts, false);
        while let Some(docs) = results.next().await {
            let docs = docs.context(error::Fdb {
                e: "streaming documents from FDB",
            })?;
            for doc in docs {
                let (_space, _db, _collection, _pk, schema_version, versionstamp) =
                    tuple::unpack::<(String, String, String, String, SchemaVersion, Versionstamp)>(
                        doc.key(),
                    )
                    .context(error::FdbTupleUnpack)?;
                let value =
                    rmpv::decode::read_value(&mut doc.value()).context(error::MPVDecode {
                        e: "decoding document",
                    })?;
                let id = DocID::new(schema_version, versionstamp);
                if p.execute(&String::from(&id), &value)? {
                    query_result.push(Document { id, doc: value });
                }
                if let Some(l) = limit
                    && query_result.len() >= l
                {
                    return Ok(query_result);
                }
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

    pub(crate) async fn create_collection(
        &self,
        db: &str,
        collection: &str,
    ) -> Result<(), AppError> {
        let mut schema = self.schema.write().await;
        schema.create_collection(db, collection);
        schema
            .apply_schema_update(db, collection, Some(CollectionSchema::default()), &self.fdb)
            .await?;
        Ok(())
    }

    pub(crate) async fn create_index(
        &self,
        db: &str,
        collection: &str,
        field: &str,
    ) -> Result<(), AppError> {
        todo!()
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
