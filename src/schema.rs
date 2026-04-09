use std::{collections::HashMap, fmt::Display, sync::LazyLock};

use foundationdb::{
    RangeOption,
    tuple::{self, Subspace},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, whatever};

use crate::error::{self, AppError};

/*
    Key layout:
    "d".db.collection."pk".doc_id -> [document value], where doc_id is versionstamp  | main data
    "d".db.collection."ix".f1.f2.<...>.fn.doc_id -> null                             | secondary index over fields f1, f2, ... fn
    "m"."schema" -> [encoded InstanceSchema]                                         | instance schema
*/
pub(crate) const SPACE_DATA: &'static str = "d";
pub(crate) const SPACE_META: &'static str = "m";
pub(crate) const KEY_PK: &'static str = "pk";
pub(crate) const KEY_INDEX: &'static str = "ix";
pub(crate) const KEY_SCHEMA: &'static str = "schema";

static FLD_NAME_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z][-_0-9a-zA-Z]*$").unwrap());

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct InstanceSchema {
    version: u32,
    schemas: HashMap<(String, String), CollectionSchema>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
struct CollectionSchema {
    fields: HashMap<String, DataType>, // name is flattened path to field, e.g ".foo.bar.baz"
    indexes: Vec<IndexDef>,
}

#[derive(Serialize, Deserialize, Debug)]
struct IndexDef {
    name: String,
    fields: Vec<String>,
    ready: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
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
    pub(crate) update_schema: Option<CollectionSchema>,
    pub(crate) affected_indexes: Option<Vec<String>>,
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

    pub(crate) async fn commit_schema(
        &mut self,
        tx: &foundationdb::Transaction,
    ) -> Result<(), AppError> {
        let current_schema = Self::load(tx).await?;
        if current_schema.version > self.version {
            // TODO: maybe update self with loaded version and retry validation?
            error::SchemaConflict.fail()?;
        }
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
        &mut self,
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
            schema_changed: false,
            affected_indexes: None,
        };
        let schema = self
            .schemas
            .entry((String::from(db), String::from(collection)))
            .or_insert(CollectionSchema::default());

        let (referred_fields, new_fields) = Self::validate_object(
            doc,
            String::from(""),
            &schema.fields,
            Vec::with_capacity(schema.fields.len()),
            None,
        )?;
        if let Some(new_fields) = new_fields {
            result.schema_changed = true;
            schema.fields.extend(new_fields);
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
