use std::{collections::HashMap, fmt::Display, sync::LazyLock, time};

use foundationdb::tuple::Subspace;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::{
    error::{self, AppError},
    storage::DocID,
};

/*
    Key layout:
    "d".db.collection."pk".<doc_id> -> [document value], where <doc_id> is schema.versionstamp             | main data
    "d".db.collection."ix".name.f1.f2.<...>.fn.<doc_id> -> null                                            | secondary index over fields f1, f2, ... fn
    "m"."schema" -> [encoded InstanceSchema]                                                               | instance schema
*/
pub(crate) const SPACE_DATA: &'static str = "d";
pub(crate) const SPACE_META: &'static str = "m";
pub(crate) const KEY_PK: &'static str = "pk";
pub(crate) const KEY_INDEX: &'static str = "ix";
pub(crate) const KEY_SCHEMA: &'static str = "schema";

static FLD_NAME_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z][-_0-9a-zA-Z]*$").unwrap());

pub(crate) type SchemaVersion = u32;

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Debug, Clone)]
pub(crate) struct Collection {
    pub(crate) db: Box<str>,
    pub(crate) collection: Box<str>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct InstanceSchema {
    pub(crate) version: SchemaVersion,
    pub(crate) collections: HashMap<Collection, CollectionSchema>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub(crate) struct CollectionSchema {
    pub(crate) fields: HashMap<String, DataType>, // name is flattened path to field, e.g ".foo.bar.baz"
    pub(crate) indexes: HashMap<String, IndexDef>,
}

/// field name and prefix length in bytes (utf-8 strings are truncated to closest char boundary)
pub(crate) type IndexField = (String, Option<usize>);

#[derive(Clone, Serialize, Deserialize, Debug)]
pub(crate) struct IndexDef {
    pub(crate) fields: Vec<IndexField>,
    pub(crate) ready: bool,
    pub(crate) lock_timestamp: Option<u128>, // unixtime in ms
    pub(crate) last_indexed_doc: Option<Vec<u8>>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
pub(crate) enum DataType {
    Integer,
    Float,
    String,
    Boolean,
    // TODO: add support non-scalar types
    // Array(Box<DataType>),
    // Map(Box<DataType>),
}

pub(crate) enum SchemaUpdate {
    UpdateCollection(CollectionSchema),
    CreateIndex((String, IndexDef)),
}

pub(crate) struct ValidationResult {
    pub(crate) updated_collection: Option<CollectionSchema>,
    pub(crate) affected_indexes: Option<Vec<String>>,
}

type NewFields = Vec<(String, DataType)>;
type ReferredFields = Vec<String>;

impl IndexDef {
    fn contains(&self, field: &str) -> bool {
        self.fields.iter().any(|(idx_field, _)| idx_field == field)
    }
}

impl Display for Collection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.db, self.collection)
    }
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

impl From<(&str, &str)> for Collection {
    fn from(value: (&str, &str)) -> Self {
        Self {
            db: Box::from(value.0),
            collection: Box::from(value.1),
        }
    }
}

impl Collection {
    pub(crate) fn subspace(&self) -> foundationdb::tuple::Subspace {
        Subspace::all().subspace(&(SPACE_DATA, self.db.as_ref(), self.collection.as_ref()))
    }
    pub(crate) fn pk_subspace(&self) -> foundationdb::tuple::Subspace {
        Subspace::all().subspace(&(
            SPACE_DATA,
            self.db.as_ref(),
            self.collection.as_ref(),
            KEY_PK,
        ))
    }
    pub(crate) fn index_subspace(&self, index: &str) -> foundationdb::tuple::Subspace {
        Subspace::all().subspace(&(
            SPACE_DATA,
            self.db.as_ref(),
            self.collection.as_ref(),
            KEY_INDEX,
            index,
        ))
    }
}

impl InstanceSchema {
    /// Loads schema from FDB
    pub(crate) async fn load(tx: &foundationdb::Transaction) -> Result<Self, AppError> {
        let key = Subspace::from_bytes(SPACE_META).pack(&(KEY_SCHEMA));
        let schema = match tx.get(&key, false).await.context(error::Fdb {
            e: "getting schema from FDB",
        })? {
            Some(d) => InstanceSchema::deserialize(&mut rmp_serde::Deserializer::new(d.as_ref()))
                .context(error::MPDecode {
                e: "decoding schema",
            })?,
            None => Self::default(),
        };
        Ok(schema)
    }

    pub(crate) async fn apply_schema_update(
        &mut self,
        collection: &Collection,
        schema_update: SchemaUpdate,
        fdb: &foundationdb::Database,
    ) -> Result<(), AppError> {
        let tx = fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;

        let mut current_schema = Self::load(&tx).await?;
        if current_schema.version != self.version {
            // TODO: maybe update self with loaded version and retry validation?
            error::SchemaConflict.fail()?;
        }
        current_schema.version += 1;

        match schema_update {
            SchemaUpdate::UpdateCollection(col) => {
                current_schema.collections.insert(collection.clone(), col);
            }
            SchemaUpdate::CreateIndex((name, index)) => {
                let col =
                    current_schema
                        .collections
                        .get_mut(collection)
                        .context(error::BadRequest {
                            e: "collection does not exist",
                        })?;
                if col.indexes.contains_key(&name) {
                    error::BadRequest {
                        e: "index already exists",
                    }
                    .fail()?;
                }

                col.indexes.insert(name, index);
            }
        }

        current_schema.write_to_db(&tx).await?;
        tx.commit().await.context(error::FdbTransactionCommit)?;

        *self = current_schema;

        Ok(())
    }

    pub(crate) async fn write_to_db(&self, tx: &foundationdb::Transaction) -> Result<(), AppError> {
        let mut schema_bytes = Vec::new();
        self.serialize(&mut rmp_serde::Serializer::new(&mut schema_bytes))
            .whatever_context("serializing schema")?;
        let key = Subspace::from_bytes(SPACE_META).pack(&(KEY_SCHEMA));
        tx.set(&key, &schema_bytes);
        Ok(())
    }

    /// Validates document against schema.
    /// Oubliette leverages concept of "emergent schema": schema evolution is automatic,
    /// the type of each field is determined by first inserted document containing that field.
    /// Any schema changes will update the in-memory representation of schema (i.e., the self),
    /// but will not write them to FDB.
    ///
    /// Also returns list of secondary indexes that shall be updated upon insertion of the document.
    pub(crate) fn validate_doc(
        &self,
        collection: &Collection,
        doc: &rmpv::Value,
    ) -> Result<ValidationResult, AppError> {
        let rmpv::Value::Map(doc) = doc else {
            error::Validation {
                e: "document must be an object",
            }
            .fail()?
        };

        let mut result = ValidationResult {
            updated_collection: None,
            affected_indexes: None,
        };
        let coll = self
            .collections
            .get(collection)
            .context(error::BadRequest {
                e: format!("Collection {collection} does not exist"),
            })?;

        let (referred_fields, new_fields) = Self::validate_object(
            doc,
            String::from(""),
            &coll.fields,
            Vec::with_capacity(coll.fields.len()),
            None,
        )?;

        if let Some(new_fields) = new_fields {
            let mut updated_schema = coll.clone();
            updated_schema.fields.extend(new_fields);
            result.updated_collection = Some(updated_schema);
        }

        for field in referred_fields {
            for (index_name, index) in &coll.indexes {
                if index.contains(&field) {
                    let n = index_name.clone();
                    result.affected_indexes = match result.affected_indexes {
                        Some(mut a) => {
                            if !a.contains(&n) {
                                a.push(n);
                            }
                            Some(a)
                        }
                        None => Some(vec![n]),
                    };
                }
            }
        }

        Ok(result)
    }

    /// Takes MessagePack map items and recursively travereses through it, accumulating field names and their types
    fn validate_object(
        obj: &[(rmpv::Value, rmpv::Value)],
        prefix: String,
        fields: &HashMap<String, DataType>,
        mut referred_fields: ReferredFields,
        mut new_fields: Option<NewFields>,
    ) -> Result<(ReferredFields, Option<NewFields>), AppError> {
        for (key, value) in obj {
            let rmpv::Value::String(field_name) = key else {
                error::Validation {
                    e: format!("object {prefix} key is not string"),
                }
                .fail()?
            };
            let Some(field_name) = field_name.as_str() else {
                error::Validation {
                    e: format!("object {prefix} key is not valid utf-8"),
                }
                .fail()?
            };

            if !FLD_NAME_RE.is_match(field_name) {
                error::Validation {
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
                    (referred_fields, new_fields) = Self::validate_object(
                        items,
                        field_name.clone(),
                        fields,
                        referred_fields,
                        new_fields,
                    )?;
                    None
                }
                _ => error::Validation {
                    e: format!("field {field_name} has unsupported data type"),
                }
                .fail()?,
            };

            let Some(field_type) = field_type else {
                continue;
            };
            if let Some(existing_type) = fields.get(&field_name) {
                if field_type != *existing_type {
                    error::Validation {
                            e: format!(
                                "field {field_name} should have type {existing_type}, but has {field_type}"
                            ),
                        }
                        .fail()?
                }
            } else {
                // new field was added
                let field_name = field_name.clone();
                new_fields = match new_fields {
                    Some(mut f) => {
                        f.push((field_name, field_type));
                        Some(f)
                    }
                    None => Some(vec![(field_name, field_type)]),
                };
            }
            referred_fields.push(field_name);
        }

        Ok((referred_fields, new_fields))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        schema::{Collection, CollectionSchema, DataType, IndexDef, InstanceSchema},
        values::json2mp,
    };

    fn j(s: &str) -> rmpv::Value {
        json2mp(serde_json::from_str(s).unwrap())
    }

    #[test]
    fn doc_validation() {
        let coll = Collection::from(("testdb", "testcol"));
        let mut schema = InstanceSchema {
            version: 0,
            collections: HashMap::from([(
                coll.clone(),
                CollectionSchema {
                    fields: HashMap::new(),
                    indexes: HashMap::from([
                        (
                            String::from("idx_foo"),
                            IndexDef {
                                fields: vec![(String::from(".foo"), Some(0))],
                                ready: true,
                            },
                        ),
                        (
                            String::from("idx_foo_barbaz"),
                            IndexDef {
                                fields: vec![
                                    (String::from(".foo"), Some(0)),
                                    (String::from(".bar.baz"), Some(0)),
                                ],
                                ready: true,
                            },
                        ),
                    ]),
                },
            )]),
        };

        assert!(schema.validate_doc(&coll, &j("137")).is_err());

        let r = schema.validate_doc(&coll, &j(r#"{"foo": 137}"#)).unwrap();
        schema.collections.insert(
            coll.clone(),
            r.updated_collection.expect("collection update expected"),
        );
        assert_eq!(
            r.affected_indexes.map(|mut v| {
                v.sort();
                v
            }),
            Some(vec![
                String::from("idx_foo"),
                String::from("idx_foo_barbaz")
            ])
        );

        assert!(schema.validate_doc(&coll, &j(r#"{"foo": "bar"}"#)).is_err());

        let r = schema
            .validate_doc(&coll, &j(r#"{"bar": {"baz": "chlos"}}"#))
            .unwrap();
        schema.collections.insert(
            coll.clone(),
            r.updated_collection.expect("collection update expected"),
        );
        assert_eq!(
            r.affected_indexes,
            Some(vec![String::from("idx_foo_barbaz")])
        );

        assert!(
            schema
                .validate_doc(&coll, &j(r#"{"foo": 137, "bar": {"baz": 12.3}}"#))
                .is_err()
        );

        let r = schema
            .validate_doc(&coll, &j(r#"{"foo": 138, "bar": {"baq": 12.3}}"#))
            .unwrap();
        schema.collections.insert(
            coll.clone(),
            r.updated_collection.expect("collection update expected"),
        );

        let r = schema
            .validate_doc(
                &coll,
                &j(r#"{"foo": 139, "bar": {"baz": "CHLOS", "baq": 12.3}}"#),
            )
            .unwrap();
        assert!(r.updated_collection.is_none());
        assert_eq!(
            r.affected_indexes,
            Some(vec![
                String::from("idx_foo_barbaz"),
                String::from("idx_foo"),
            ])
        );

        let r = schema
            .validate_doc(&coll, &j(r#"{"bar": {"baw": {"foo": true}}}"#))
            .unwrap();

        schema.collections.insert(
            coll.clone(),
            r.updated_collection.expect("collection update expected"),
        );
        assert!(r.affected_indexes.is_none());

        let dbs = schema.collections.get(&coll).unwrap();
        assert_eq!(dbs.fields.len(), 4);
        assert_eq!(dbs.fields.get(".foo"), Some(&DataType::Integer));
        assert_eq!(dbs.fields.get(".bar.baz"), Some(&DataType::String));
        assert_eq!(dbs.fields.get(".bar.baq"), Some(&DataType::Float));
        assert_eq!(dbs.fields.get(".bar.baw.foo"), Some(&DataType::Boolean));
    }
}
