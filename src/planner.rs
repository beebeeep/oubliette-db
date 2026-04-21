use std::future;

use foundationdb::{
    FdbError, RangeOption, TransactError, Transaction, TransactionCancelled,
    future::FdbValues,
    tuple::{self, Subspace, Versionstamp},
};
use futures::{Stream, StreamExt, TryStreamExt, future::Either, stream};
use rmpv::Value;
use snafu::{ResultExt, whatever};

use crate::{
    error::{self, AppError, MPVDecode},
    expression::Expression,
    schema::{Collection, CollectionSchema, IndexDef, SchemaVersion},
    storage::{DB, DocID, Document},
};

pub(crate) enum PlanResult<A, B, C, D> {
    Union(A),
    Filter(B),
    Fullscan(C),
    IdxScan(D),
}

pub(crate) enum Plan<'a> {
    Union(Vec<Plan<'a>>),
    Filter(Filter<'a>),
    Fullscan(Fullscan<'a>),
    IdxScan(IdxScan<'a>),
}

pub(crate) struct Filter<'a> {
    driver: Box<Plan<'a>>,
    filter: Expression,
}

pub(crate) struct IdxScan<'a> {
    collection: &'a Collection,
    idx_space: Subspace,
}

pub(crate) struct Fullscan<'a> {
    collection: &'a Collection,
    filter: Expression,
}

impl Fullscan<'_> {
    fn execute(&self, tx: &Transaction) -> impl Stream<Item = Result<Document, AppError>> {
        let opt = RangeOption::from(&self.collection.subspace());
        tx.get_ranges_keyvalues(opt, false)
            .map_err(|e| AppError::Fdb {
                e: String::from("scanning collection"),
                source: e,
            })
            .try_filter_map(async |value| {
                let (_space, _db, _collection, _pk, schema_version, versionstamp) =
                    tuple::unpack::<(String, String, String, String, SchemaVersion, Versionstamp)>(
                        value.key(),
                    )
                    .context(error::FdbTupleUnpack)?;
                let doc = Document {
                    id: DocID::new(schema_version, versionstamp),
                    doc: rmpv::decode::read_value(&mut value.value().as_ref()).context(
                        MPVDecode {
                            e: "decoding document",
                        },
                    )?,
                };
                if self.filter.evaluate(&doc) {
                    Ok(Some(doc))
                } else {
                    Ok(None)
                }
            })
    }
}

// impl<'a, S> IdxScan<'a, S>
// where
//     S: Stream<Item = Result<FdbValues, FdbError>> + Send + Sync + Unpin + 'a,
impl IdxScan<'_> {
    fn execute(&self, tx: &Transaction) -> impl Stream<Item = Result<Document, AppError>> {
        let opt = RangeOption::from(&self.idx_space);
        tx.get_ranges_keyvalues(opt, false)
            .map_err(|e| AppError::Fdb {
                e: String::from("scanning index"),
                source: e,
            })
            .try_filter_map(async |value| {
                let (schema_version, versionstamp) = self
                    .idx_space
                    .unpack::<(SchemaVersion, Versionstamp)>(value.key())
                    .context(error::FdbTupleUnpack)?;
                let doc_id = DocID::new(schema_version, versionstamp);
                let Some(doc) = DB::get_doc_tx(self.collection, &doc_id, tx).await? else {
                    whatever!("missing indexed document {doc_id}");
                };
                Ok(Some(doc))
            })
    }
    /*
    async fn next(&mut self) -> Result<Option<Document>, AppError> {
        let idx_stream = match self.idx_stream {
            Some(s) => &mut s,
            None => {
                let opt = RangeOption::from(&self.idx_space);
                self.idx_stream = Some(self.tx.get_ranges(opt, false));
                self.idx_stream.as_mut().unwrap()
            }
        };
        while let Some(idx_values) = idx_stream.next().await {
            let idx_values = idx_values.context(error::Fdb {
                e: "streaming index from FDB",
            })?;
            for val in idx_values {
                let (schema_version, versionstamp) = self
                    .idx_space
                    .unpack::<(SchemaVersion, Versionstamp)>(val.key())
                    .context(error::FdbTupleUnpack)?;
                let doc_id = DocID::new(schema_version, versionstamp);
                let doc = DB::get_doc_tx(self.collection, &doc_id, self.tx).await?;
                let Some(doc) = doc else {
                    eprintln!("ERROR, {doc_id}i from index is not found!");
                    continue;
                };
                if let Some(filter) = &self.filter {
                    if filter.execute(&doc) {
                        return Ok(Some(doc));
                    } else {
                        continue;
                    }
                }
                return Ok(Some(doc));
            }
        }
        Ok(None)
    }
    */
}

impl<'a> Plan<'a> {
    pub(crate) fn from_query(
        collection: &'a Collection,
        schema: &CollectionSchema,
        query: &str,
    ) -> Result<Self, AppError> {
        let mut expr = Expression::try_from(query)?;
        expr.hydrate_indexes(schema);
        Self::from_expr(expr, collection, schema)
    }

    fn from_expr(
        expr: Expression,
        collection: &'a Collection,
        schema: &CollectionSchema,
    ) -> Result<Self, AppError> {
        match expr {
            Expression::And(expressions) => todo!(),
            Expression::Or(expressions) => todo!(),
            Expression::Not(expression) => todo!(),
            Expression::Atomic(predicate) => todo!(),
            Expression::Empty => Ok(Self::Fullscan(Fullscan {
                collection,
                filter: Expression::Empty,
            })),
        }
    }

    pub(crate) fn execute(
        &self,
        tx: &Transaction,
    ) -> impl Stream<Item = Result<Document, AppError>> + Send + Sync {
        stream::once(future::ready(Err(AppError::BadRequest {
            e: String::from("not implemented"),
        })))
    }
}
