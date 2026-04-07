use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, Mutex, RwLock},
};

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
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

const KEY_PK: &'static str = "pk";
const KEY_INDEX: &'static str = "ix";
const KEY_SCHEMA: &'static str = "schema";

const SPACE_DATA: &'static str = "d";
const SPACE_SCHEMA: &'static str = "m";

/*
    Key layout:
    "d".db.collection."pk".doc_id -> [document value], where doc_id is versionstamp  | main data
    "d".db.collection."ix".f1.f2.<...>.fn.doc_id -> null | secondary index over fields f1, f2, ... fn
    "m".db.collection.schema -> [encoded Schema] | schema for that db and collection
*/

pub(crate) struct DocID([u8; 12]);

type InstanceSchema = HashMap<(String, String), Schema>;
pub(crate) struct DB {
    fdb: foundationdb::Database,
    schema: Arc<RwLock<InstanceSchema>>,
    fld_name: regex::Regex,
}

pub(crate) struct Document {
    pub(crate) id: DocID,
    pub(crate) doc: rmpv::Value,
}

#[derive(Serialize, Deserialize, PartialEq)]
enum DataType {
    Integer,
    Float,
    String,
    Boolean,
    // will support non-scalar types later
    // Array(Box<DataType>),
    // Map(Box<DataType>),
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DataType::Integer => "integer",
            DataType::Float => "float",
            DataType::String => "string",
            DataType::Boolean => "boolean",
        };
        write!(f, "{s}")
    }
}

#[derive(Serialize, Deserialize)]
struct IndexDef {
    name: String,
    fields: Vec<String>,
    ready: bool,
}

#[derive(Serialize, Deserialize)]
struct Schema {
    fields: HashMap<String, DataType>, // name is flattened path to field, e.g ".foo.bar.baz"
    indexes: Vec<IndexDef>,
}

impl DocID {
    #[allow(dead_code)]
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
    pub(crate) async fn from_path(path: &str) -> Result<Self, AppError> {
        let db = Self {
            fdb: foundationdb::Database::from_path(path)
                .whatever_context("initializing database")?,
            schema: Arc::new(RwLock::new(HashMap::new())),
            fld_name: regex::Regex::new(r"^[a-zA-Z][-_0-9a-zA-Z]*$").unwrap(),
        };
        db.load_schema().await.whatever_context("loading schema")?;

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
        Self::validate_doc(&doc)?;

        let subspace = Subspace::all().subspace(&(SPACE_DATA, db, collection));
        let kt = (KEY_PK, &Versionstamp::incomplete(0));
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
        let subspace = Subspace::all().subspace(&(db, collection, KEY_PK));
        let opts = RangeOption::from(&subspace);
        let mut results = tx.get_ranges(opts, false);
        while let Some(docs) = results.next().await {
            let docs = docs.context(error::Fdb {
                e: "streaming documents from FDB",
            })?;
            for doc in docs {
                let (_space, _db, _collection, _pk, versionstamp) =
                    tuple::unpack::<(String, String, String, String, Versionstamp)>(doc.key())
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
        let subspace = Subspace::all().subspace(&(SPACE_DATA, db, collection));
        let key = subspace.pack(&(KEY_PK, Versionstamp::from(id.0)));
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

    pub(crate) async fn add_index(
        &self,
        db: &str,
        collection: &str,
        field: &str,
    ) -> Result<(), AppError> {
        todo!()
    }

    async fn load_schema(&self) -> Result<(), AppError> {
        let tx = self.fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let opt = RangeOption::from(&Subspace::all().subspace(&(SPACE_SCHEMA)));
        let mut result = tx.get_ranges(opt, false);
        let mut schemas = self.schema.write().expect("poisoned lock");
        while let Some(schema_values) = result.next().await {
            let schema_values = schema_values.context(error::Fdb {
                e: "getting schema from FDB",
            })?;
            for schema in schema_values {
                let (_space, db, collection, _schema) =
                    tuple::unpack::<(String, String, String, String)>(schema.key())
                        .context(error::FdbTupleUnpack)?;
                let collection_schema =
                    Schema::deserialize(&mut rmp_serde::Deserializer::new(schema.value()))
                        .context(error::MPDecode {
                            e: format!("decoding schema for {db}.{collection}"),
                        })?;
                schemas.insert((db, collection), collection_schema);
            }
        }
        Ok(())
    }

    fn validate_doc(
        &self,
        db: &str,
        collection: &str,
        fields: &mut HashMap<String, DataType>,
        doc: &rmpv::Value,
    ) -> Result<(), AppError> {
        if let rmpv::Value::Map(v) = doc {
            self.validate_object(v, String::from("."), None);
            todo!()
        } else {
            error::BadRequest {
                e: "document must be an object",
            }
            .fail()?
        }

        // TODO: recursively validate document: all maps shall be have key of type string and key (i.e. field name) must match /[-_a-zA-Z0-9]+/

        // TODO: implement emergent database schema:
        // 1. Field type is not enforced until first document containing that field is inserted.
        // 2. Upon insertion, the inserted field name and its type become a contract,
        //    and all subsequent documents with same field shall have the same type
        // 3. Field names in nested documents are flattened, e.g. ".foo.bar.baz"
    }

    /// Takes MessagePack map items and recursively travereses through it, accumulating field names and their types
    fn validate_object(
        &self,
        obj: &[(rmpv::Value, rmpv::Value)],
        prefix: String,
        fields: &HashMap<String, DataType>,
        mut new_fields: Option<Vec<(String, DataType)>>,
    ) -> Result<Option<Vec<(String, DataType)>>, AppError> {
        for (key, value) in obj {
            let field_name = match key {
                rmpv::Value::String(field_name) => field_name,
                _ => error::BadRequest {
                    e: format!("object {prefix} key is not string"),
                }
                .fail()?,
            };
            let field_name = match field_name.as_str() {
                Some(s) => s,
                None => error::BadRequest {
                    e: format!("object {prefix} key is not valid utf-8"),
                }
                .fail()?,
            };

            if !self.fld_name.is_match(field_name) {
                error::BadRequest {
                    e: format!("bad field name '{field_name}'"),
                }
                .fail()?;
            }

            let field_name = format!("{prefix}.{field_name}");
            let field_type = match value {
                rmpv::Value::F32(_) => Some(DataType::Float),
                rmpv::Value::F64(_) => Some(DataType::Float),
                rmpv::Value::Boolean(_) => Some(DataType::Boolean),
                rmpv::Value::Integer(_) => Some(DataType::Integer),
                rmpv::Value::String(_) => Some(DataType::String),
                rmpv::Value::Map(items) => {
                    new_fields =
                        self.validate_object(items, field_name.clone(), fields, new_fields)?;
                    None
                }
                _ => error::BadRequest {
                    e: format!("field {field_name} has unsupported data type"),
                }
                .fail()?,
            };

            if let Some(field_type) = field_type {
                if let Some(existing_type) = fields.get(&field_name) {
                    if field_type != *existing_type {
                        error::BadRequest {
                            e: format!(
                                "field {field_name} should have type {existing_type}, but has {field_type}"
                            ),
                        }
                        .fail()?
                    }
                } else {
                    new_fields = match new_fields {
                        Some(mut f) => {
                            f.push((field_name, field_type));
                            Some(f)
                        }
                        None => Some(vec![(field_name, field_type)]),
                    };
                }
            }
        }

        Ok(new_fields)
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
