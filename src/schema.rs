use std::{collections::HashMap, fmt::Display, sync::LazyLock};

use foundationdb::{
    RangeOption,
    tuple::{self, Subspace},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, whatever};

use crate::error::{self, AppError, FdbTransactionCommit};

/*
    Key layout:
    "d".db.collection."pk".<doc_id> -> [document value], where <doc_id> is schema.versionstamp             | main data
    "d".db.collection."ix".f1.f2.<...>.fn.<doc_id> -> null                                                 | secondary index over fields f1, f2, ... fn
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

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct InstanceSchema {
    version: SchemaVersion,
    schemas: HashMap<(String, String), CollectionSchema>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub(crate) struct CollectionSchema {
    fields: HashMap<String, DataType>, // name is flattened path to field, e.g ".foo.bar.baz"
    indexes: Vec<IndexDef>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
struct IndexDef {
    name: String,
    fields: Vec<String>,
    ready: bool,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
enum DataType {
    Integer,
    Float,
    String,
    Boolean,
    // TODO: add support non-scalar types
    // Array(Box<DataType>),
    // Map(Box<DataType>),
}

pub(crate) struct ValidationResult {
    pub(crate) updated_collection: Option<CollectionSchema>,
    pub(crate) affected_indexes: Option<Vec<String>>,
    pub(crate) schema_version: SchemaVersion,
}

type NewFields = Vec<(String, DataType)>;
type ReferredFields = Vec<String>;

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
        db: &str,
        collection: &str,
        updated_collection: Option<CollectionSchema>,
        fdb: &foundationdb::Database,
    ) -> Result<SchemaVersion, AppError> {
        let tx = fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let mut current_schema = Self::load(&tx).await?;
        if current_schema.version != self.version {
            // TODO: maybe update self with loaded version and retry validation?
            error::SchemaConflict.fail()?;
        }
        current_schema.version += 1;
        if let Some(updated_collection) = updated_collection {
            current_schema.schemas.insert(
                (String::from(db), String::from(collection)),
                updated_collection,
            );
        }

        let mut schema_bytes = Vec::new();
        current_schema
            .serialize(&mut rmp_serde::Serializer::new(&mut schema_bytes))
            .whatever_context("serializing schema")?;
        let key = Subspace::from_bytes(SPACE_META).pack(&(KEY_SCHEMA));
        tx.set(&key, &schema_bytes);
        tx.commit().await.context(error::FdbTransactionCommit)?;

        *self = current_schema;
        eprintln!("schema now is {:?}", self);

        Ok(self.version)
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
        db: &str,
        collection: &str,
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
            schema_version: self.version,
        };
        let schema = self
            .schemas
            .get(&(String::from(db), String::from(collection)))
            .context(error::BadRequest {
                e: format!("Collection {db}.{collection} does not exist"),
            })?;

        let (referred_fields, new_fields) = Self::validate_object(
            doc,
            String::from(""),
            &schema.fields,
            Vec::with_capacity(schema.fields.len()),
            None,
        )?;

        if let Some(new_fields) = new_fields {
            let mut updated_schema = schema.clone();
            updated_schema.fields.extend(new_fields);
            result.updated_collection = Some(updated_schema);
        }

        for field in referred_fields {
            for index in &schema.indexes {
                if index.fields.contains(&field) {
                    let n = index.name.clone();
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

    /// creates new collection to in-memory representation of schema. Does not write schema to FDB!
    pub(crate) fn create_collection(&mut self, db: &str, collection: &str) {
        self.schemas
            .entry((String::from(db), String::from(collection)))
            .or_insert(CollectionSchema::default());
    }

    /// adds new index to in-memory representation of schema. Does not write schema to FDB!
    pub(crate) fn add_index(
        &mut self,
        db: &str,
        collection: &str,
        name: String,
        fields: Vec<String>,
    ) {
        let schema = self
            .schemas
            .entry((String::from(db), String::from(collection)))
            .or_insert(CollectionSchema::default());
        schema.indexes.push(IndexDef {
            name,
            fields,
            ready: false,
        });
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
    use crate::{
        encoding::json2mp,
        schema::{DataType, InstanceSchema},
    };

    fn j(s: &str) -> rmpv::Value {
        json2mp(serde_json::from_str(s).unwrap())
    }

    #[test]
    fn doc_validation() {
        let mut schema = InstanceSchema::default();
        schema.add_index(
            "testdb",
            "testcol",
            String::from("idx_foo"),
            vec![String::from(".foo")],
        );
        schema.add_index(
            "testdb",
            "testcol",
            String::from("idx_foo_barbaz"),
            vec![String::from(".foo"), String::from(".bar.baz")],
        );

        assert!(schema.validate_doc("testdb", "testcol", &j("137")).is_err());

        let r = schema
            .validate_doc("testdb", "testcol", &j(r#"{"foo": 137}"#))
            .unwrap();
        assert!(r.schema_changed);
        assert_eq!(
            r.affected_indexes,
            Some(vec![
                String::from("idx_foo"),
                String::from("idx_foo_barbaz")
            ])
        );

        assert!(
            schema
                .validate_doc("testdb", "testcol", &j(r#"{"foo": "bar"}"#))
                .is_err()
        );

        let r = schema
            .validate_doc("testdb", "testcol", &j(r#"{"bar": {"baz": "chlos"}}"#))
            .unwrap();
        assert!(r.schema_changed);
        assert_eq!(
            r.affected_indexes,
            Some(vec![String::from("idx_foo_barbaz")])
        );

        assert!(
            schema
                .validate_doc(
                    "testdb",
                    "testcol",
                    &j(r#"{"foo": 137, "bar": {"baz": 12.3}}"#)
                )
                .is_err()
        );

        let r = schema
            .validate_doc(
                "testdb",
                "testcol",
                &j(r#"{"foo": 138, "bar": {"baq": 12.3}}"#),
            )
            .unwrap();
        assert!(r.schema_changed);

        let r = schema
            .validate_doc(
                "testdb",
                "testcol",
                &j(r#"{"foo": 139, "bar": {"baz": "CHLOS", "baq": 12.3}}"#),
            )
            .unwrap();
        assert!(!r.schema_changed);
        assert_eq!(
            r.affected_indexes,
            Some(vec![
                String::from("idx_foo_barbaz"),
                String::from("idx_foo"),
            ])
        );

        let r = schema
            .validate_doc(
                "testdb",
                "testcol",
                &j(r#"{"bar": {"baw": {"foo": true}}}"#),
            )
            .unwrap();

        assert!(r.schema_changed);
        assert!(r.affected_indexes.is_none());

        let dbs = schema
            .schemas
            .get(&(String::from("testdb"), String::from("testcol")))
            .unwrap();
        assert_eq!(dbs.fields.len(), 4);
        assert_eq!(dbs.fields.get(".foo"), Some(&DataType::Integer));
        assert_eq!(dbs.fields.get(".bar.baz"), Some(&DataType::String));
        assert_eq!(dbs.fields.get(".bar.baq"), Some(&DataType::Float));
        assert_eq!(dbs.fields.get(".bar.baw.foo"), Some(&DataType::Boolean));
    }
}
