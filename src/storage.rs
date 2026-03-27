use crate::error::{self, AppError};
use foundationdb::{
    options::MutationType,
    tuple::{Subspace, Versionstamp},
};
use snafu::ResultExt;

const PK: &'static str = "pk";

pub(crate) struct DocID([u8; 12]);

pub(crate) struct DB {
    fdb: foundationdb::Database,
}

impl Into<Box<str>> for DocID {
    fn into(self) -> Box<str> {
        hex::encode(self.0).into_boxed_str()
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

    pub(crate) async fn get_doc(
        &self,
        db: &str,
        collection: &str,
        id: impl TryInto<DocID, Error = AppError>,
    ) -> Result<rmpv::Value, AppError> {
        let id = id.try_into()?;
        todo!()
    }
}
