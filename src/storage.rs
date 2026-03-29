use crate::{
    error::{self, AppError, MPVDecode},
    predicate::Predicate,
};
use foundationdb::{
    RangeOption,
    options::MutationType,
    tuple::{self, Subspace, Versionstamp},
};
use futures::StreamExt;
use snafu::ResultExt;

const PK: &'static str = "pk";

pub(crate) struct DocID([u8; 12]);

pub(crate) struct DB {
    fdb: foundationdb::Database,
}

pub(crate) struct Document {
    pub(crate) id: DocID,
    pub(crate) doc: rmpv::Value,
}

impl DocID {
    fn as_slice_ref(&self) -> &[u8] {
        &self.0.as_slice()
    }
}

impl From<&DocID> for String {
    fn from(d: &DocID) -> Self {
        hex::encode(d.0)
    }
}

impl TryFrom<&str> for DocID {
    type Error = AppError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let v = hex::decode(value).context(error::DocIDDecode)?;
        if v.len() != 12 {
            return Err(AppError::DocIDDecode {
                source: hex::FromHexError::InvalidStringLength,
            });
        }
        Ok(Self(v.try_into().unwrap()))
    }
}

impl DB {
    pub(crate) fn from_path(path: &str) -> Result<Self, AppError> {
        Ok(Self {
            fdb: foundationdb::Database::from_path(path)
                .whatever_context("initializing database")?,
        })
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
        Self::validate_doc(&doc)?;

        let subspace = Subspace::all().subspace(&(db, collection));
        let kt = (PK, &Versionstamp::incomplete(0));
        let key = subspace.pack_with_versionstamp(&kt);

        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
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

        Ok(DocID(versionstamp.as_bytes().clone()))
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
        let subspace = Subspace::all().subspace(&(db, collection, PK));
        let opts = RangeOption::from(&subspace);
        let mut results = tx.get_ranges(opts, false);
        while let Some(docs) = results.next().await {
            let docs = docs.context(error::Fdb {
                e: "streaming documents from FDB",
            })?;
            for doc in docs {
                let (_db, _collection, _pk, versionstamp) =
                    tuple::unpack::<(String, String, String, Versionstamp)>(doc.key())
                        .context(error::FdbTupleUnpack)?;
                let value =
                    rmpv::decode::read_value(&mut doc.value()).context(error::MPVDecode {
                        e: "decoding document",
                    })?;
                let id = DocID(*versionstamp.as_bytes());
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
        let subspace = Subspace::all().subspace(&(db, collection));
        let key = subspace.pack(&(PK, Versionstamp::from(id.0)));
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

    fn validate_doc(doc: &rmpv::Value) -> Result<(), AppError> {
        if let rmpv::Value::Map(v) = doc {
            for (k, _) in v {
                if let rmpv::Value::String(_) = k {
                    continue;
                }
                return error::BadRequest {
                    e: "field name must be string",
                }
                .fail();
            }
            Ok(())
        } else {
            error::BadRequest {
                e: "document must be an object",
            }
            .fail()
        }
    }
}

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
