use std::fmt::Display;

use foundationdb::{
    future::FdbValue,
    tuple::{self, TuplePack, Versionstamp},
};
use snafu::ResultExt;

use crate::{
    error::{self, AppError, MPVDecode},
    schema::SchemaVersion,
};

#[derive(Debug, Clone)]
pub(crate) struct DocID {
    pub(crate) schema: SchemaVersion,
    pub(crate) versionstamp: Versionstamp,
}

pub(crate) struct Document {
    pub(crate) id: DocID,
    pub(crate) value: rmpv::Value,
}

impl Default for DocID {
    fn default() -> Self {
        Self {
            schema: 0,
            versionstamp: Versionstamp::from([0u8; 12]),
        }
    }
}
impl From<&DocID> for String {
    fn from(d: &DocID) -> Self {
        let mut s = hex::encode(d.schema.to_le_bytes());
        s.push_str(&hex::encode(d.versionstamp.as_bytes()));
        s
    }
}

impl Display for DocID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", String::from(self))
    }
}

impl TuplePack for DocID {
    fn pack<W: std::io::Write>(
        &self,
        w: &mut W,
        tuple_depth: tuple::TupleDepth,
    ) -> std::io::Result<tuple::VersionstampOffset> {
        (self.schema, &self.versionstamp).pack(w, tuple_depth)
    }
}

impl TryFrom<&str> for DocID {
    type Error = AppError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.len() != 2 * (size_of::<SchemaVersion>() + 12) {
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

impl DocID {
    pub(crate) fn new(schema: SchemaVersion, versionstamp: Versionstamp) -> Self {
        Self {
            schema,
            versionstamp,
        }
    }
}

impl TryFrom<FdbValue> for Document {
    type Error = AppError;

    fn try_from(value: FdbValue) -> Result<Self, Self::Error> {
        let (_space, _db, _collection, _pk, schema_version, versionstamp) =
            tuple::unpack::<(String, String, String, String, SchemaVersion, Versionstamp)>(
                value.key(),
            )
            .context(error::FdbTupleUnpack)?;
        Ok(Self {
            id: DocID::new(schema_version, versionstamp),
            value: rmpv::decode::read_value(&mut value.value().as_ref()).context(MPVDecode {
                e: "decoding document",
            })?,
        })
    }
}
