use std::{collections::HashMap, fmt::Display, sync::LazyLock};

use foundationdb::{
    RangeOption,
    tuple::{self, Subspace},
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error::{self, AppError};

pub(crate) const SPACE_DATA: &'static str = "d";
pub(crate) const SPACE_SCHEMA: &'static str = "m";
pub(crate) const KEY_PK: &'static str = "pk";
pub(crate) const KEY_INDEX: &'static str = "ix";
pub(crate) const KEY_SCHEMA: &'static str = "schema";

/*
    Key layout:
    "d".db.collection."pk".doc_id -> [document value], where doc_id is versionstamp  | main data
    "d".db.collection."ix".f1.f2.<...>.fn.doc_id -> null | secondary index over fields f1, f2, ... fn
    "m".db.collection.schema -> [encoded Schema] | schema for that db and collection
*/

static FLD_NAME_RE: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"^[a-zA-Z][-_0-9a-zA-Z]*$").unwrap());

#[derive(Default)]
pub(crate) struct InstanceSchema {
    schemas: HashMap<(String, String), Schema>,
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

#[derive(Serialize, Deserialize, Default)]
struct Schema {
    fields: HashMap<String, DataType>, // name is flattened path to field, e.g ".foo.bar.baz"
    indexes: Vec<IndexDef>,
}

impl InstanceSchema {
    pub(crate) async fn load(fdb: &foundationdb::Database) -> Result<Self, AppError> {
        let tx = fdb.create_trx().context(error::Fdb {
            e: "starting transaction",
        })?;
        let opt = RangeOption::from(&Subspace::all().subspace(&(SPACE_SCHEMA)));
        let mut result = tx.get_ranges(opt, false);

        let mut schemas = HashMap::new();
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
        Ok(Self { schemas })
    }

    pub(crate) fn validate_doc(
        &mut self,
        db: &str,
        collection: &str,
        doc: &rmpv::Value,
    ) -> Result<(), AppError> {
        // TODO: this should also return list of indexed fields (perhaps with values?) to write secondary indexes
        if let rmpv::Value::Map(v) = doc {
            {
                let schema = self
                    .schemas
                    .entry((String::from(db), String::from(collection)))
                    .or_insert(Schema::default());
                if let Some(new_fields) =
                    Self::validate_object(v, String::from("."), &schema.fields, None)?
                {
                    schema.fields.extend(new_fields);
                }
            }
            Ok(())
        } else {
            error::Validation {
                e: "document must be an object",
            }
            .fail()?
        }
    }

    /// Takes MessagePack map items and recursively travereses through it, accumulating field names and their types
    fn validate_object(
        obj: &[(rmpv::Value, rmpv::Value)],
        prefix: String,
        fields: &HashMap<String, DataType>,
        mut new_fields: Option<Vec<(String, DataType)>>,
    ) -> Result<Option<Vec<(String, DataType)>>, AppError> {
        for (key, value) in obj {
            let field_name = match key {
                rmpv::Value::String(field_name) => field_name,
                _ => error::Validation {
                    e: format!("object {prefix} key is not string"),
                }
                .fail()?,
            };
            let field_name = match field_name.as_str() {
                Some(s) => s,
                None => error::Validation {
                    e: format!("object {prefix} key is not valid utf-8"),
                }
                .fail()?,
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
                    new_fields =
                        Self::validate_object(items, field_name.clone(), fields, new_fields)?;
                    None
                }
                _ => error::Validation {
                    e: format!("field {field_name} has unsupported data type"),
                }
                .fail()?,
            };

            if let Some(field_type) = field_type {
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

#[cfg(test)]
mod tests {
    use crate::{
        encoding::json2mp,
        schema::{FLD_NAME_RE, InstanceSchema},
    };
    use std::{
        str::FromStr,
        sync::{Arc, RwLock},
    };

    fn j(s: &str) -> rmpv::Value {
        let jsonv = serde_json::Value::from_str(s).unwrap();
        json2mp(jsonv)
    }

    #[test]
    fn doc_validation() {
        let mut schema = InstanceSchema::default();

        assert!(schema.validate_doc("testdb", "testcol", &j("137")).is_err());

        assert!(
            schema
                .validate_doc("testdb", "testcol", &j(r#"{"foo": 137}"#))
                .is_ok()
        );

        assert!(
            schema
                .validate_doc("testdb", "testcol", &j(r#"{"foo": "bar"}"#))
                .is_err()
        );

        assert!(
            schema
                .validate_doc(
                    "testdb",
                    "testcol",
                    &j(r#"{"foo": 137, "bar": {"baz": "chlos"}}"#)
                )
                .is_ok()
        );
    }
}
