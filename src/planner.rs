use foundationdb::{
    RangeOption, Transaction,
    tuple::{Subspace, Versionstamp},
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use sexpression::Expression as Sexpr;
use snafu::{ResultExt, whatever};

use crate::{
    document::{DocID, Document},
    error::{self, AppError},
    expression::Expression,
    misc::{assert_len, assert_longer},
    schema::{Collection, CollectionSchema, SchemaVersion},
    storage::DB,
};

/// DocumentStream is stream produced by Plan. Due to recursive nature of Plan type,
/// the type shenanigans escalate quickly, so opting to dynamic dispatching with BoxStream.
/// Performance cost should be negligible, as for every Document polled from the stream,
/// there will be few dynamic dispatches, and one or two network round trips to FDB.
pub(crate) type DocumentStream<'a> = stream::BoxStream<'a, Result<Document, AppError>>;

pub(crate) enum Plan<'a> {
    Union(Vec<Plan<'a>>),   // (union (subplan1) (subplan2) ...)
    Filter(Filter<'a>),     // (filter (subplan) (filter-expr))
    Fullscan(Fullscan<'a>), // (scan (filter-expr))
    IdxScan(IdxScan<'a>), // (idxscan (idx-field1-predicate) (idx-field2-predicate) ...), different predicates are intersected (i.e. joined by AND)
}

pub(crate) struct Filter<'a> {
    driver: Box<Plan<'a>>,
    expr: Expression,
}

pub(crate) struct IdxScan<'a> {
    collection: &'a Collection,
    idx_space: Subspace,
    // TODO: add extra filter here to support range scans over composite indexes:
    // Suppose (foo, bar) is index, and query is (and (gt .foo 300) (le .bar 200))
    // Scan takes continious subrange where .foo > 300 and can select only those index entries
    // where .bar <= 200, *before* fetching the documents from FDB
}

pub(crate) struct Fullscan<'a> {
    collection: &'a Collection,
    filter: Expression,
}

impl<'a> Fullscan<'a> {
    fn from_expr(expr: &Sexpr, collection: &'a Collection) -> Result<Self, AppError> {
        Ok(Self {
            collection,
            filter: Expression::try_from(expr)?,
        })
    }

    fn execute(&self, tx: &Transaction) -> impl Stream<Item = Result<Document, AppError>> {
        let opt = RangeOption::from(&self.collection.pk_subspace());
        tx.get_ranges_keyvalues(opt, false)
            .map_err(|e| AppError::Fdb {
                e: String::from("scanning collection"),
                source: e,
            })
            .try_filter_map(async |value| {
                let doc = Document::try_from(value)?;
                if self.filter.evaluate(&doc) {
                    Ok(Some(doc))
                } else {
                    Ok(None)
                }
            })
    }
}

impl<'a> IdxScan<'a> {
    fn from_exprs(
        exprs: &[Sexpr],
        collection: &'a Collection,
        schema: &CollectionSchema,
    ) -> Result<Self, AppError> {
        let mut predicates = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let expr = Expression::try_from(expr)?;
            let Expression::Atomic(predicate) = expr else {
                return error::BadRequest {
                    e: "ixscan expression shall be predicate using indexed field",
                }
                .fail();
            };
            predicates.push(predicate);
        }
        let predicate_fields: Vec<&str> = predicates.iter().map(|p| p.fld.as_str()).collect();

        'NEXT_INDEX: for (idx_name, def) in schema.indexes.iter() {
            if !def.ready || predicate_fields.len() > def.fields.len() {
                continue;
            }
            for f in predicate_fields.iter() {
                if !def.fields.iter().any(|(field, _)| field.as_str() == *f) {
                    continue 'NEXT_INDEX;
                }
            }
            // all predicate fields are found in index, we can use it
            let mut idx_space = collection.index_subspace(&idx_name);
            for (field, prefix) in def.fields.iter() {
                let predicate = predicates.iter().find(|p| &p.fld == field).unwrap();
                let val = match prefix {
                    Some(prefix) => {
                        let mut v = predicate.val.clone();
                        v.truncate(*prefix)?;
                        v
                    }
                    None => predicate.val.clone(),
                };
                match predicate.rel {
                    crate::expression::Relation::Eq => {
                        idx_space = idx_space.subspace(&val);
                    }
                    crate::expression::Relation::Gt => todo!("relation unsupported"),
                    crate::expression::Relation::Ge => todo!("relation unsupported"),
                    crate::expression::Relation::Lt => todo!("relation unsupported"),
                    crate::expression::Relation::Le => todo!("relation unsupported"),
                }
            }
            return Ok(Self {
                collection,
                idx_space,
            });
        }

        return error::BadRequest {
            e: "idxscan refers to non-indexed fields, or index is not yet ready to use",
        }
        .fail();
    }

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
}

impl<'a> Plan<'a> {
    pub(crate) fn from_str(
        collection: &'a Collection,
        schema: &CollectionSchema,
        plan: &str,
    ) -> Result<Self, AppError> {
        let (plan, _) = sexpression::read(plan).context(error::QueryParse {
            e: "failed to parse plan",
        })?;

        Self::from_expr(&plan, collection, schema)
    }

    pub(crate) fn from_query(
        _collection: &'a Collection,
        _schema: &CollectionSchema,
        _query: &str,
    ) -> Result<Self, AppError> {
        whatever!("Query planner is not yet implemented, try querying with plan");
    }

    fn from_expr(
        expr: &Sexpr,
        collection: &'a Collection,
        schema: &CollectionSchema,
    ) -> Result<Self, AppError> {
        let Sexpr::List(list) = expr else {
            return error::BadRequest {
                e: "plan expression must be list",
            }
            .fail();
        };
        Self::from_list(list, collection, schema)
    }

    fn from_list(
        list: &[Sexpr],
        collection: &'a Collection,
        schema: &CollectionSchema,
    ) -> Result<Self, AppError> {
        match list.get(0) {
            Some(Sexpr::Symbol(op)) => match *op {
                "ixscan" => {
                    assert_longer(&list, 1)?;
                    Ok(Self::IdxScan(IdxScan::from_exprs(
                        &list[1..],
                        collection,
                        schema,
                    )?))
                }
                "scan" => {
                    assert_len(&list, 2)?;
                    Ok(Self::Fullscan(Fullscan::from_expr(&list[1], collection)?))
                }
                "filter" => {
                    assert_len(&list, 3)?;
                    Ok(Self::Filter(Filter {
                        driver: Box::new(Self::from_expr(&list[1], collection, schema)?),
                        expr: Expression::try_from(&list[2])?,
                    }))
                }
                "union" => {
                    assert_longer(&list, 2)?;
                    let mut es = Vec::with_capacity(list.len() - 1);
                    for e in list.iter().skip(1) {
                        es.push(Self::from_expr(e, collection, schema)?);
                    }
                    Ok(Self::Union(es))
                }
                v => error::BadRequest {
                    e: format!("unknown plan operation: {v}"),
                }
                .fail()?,
            },
            Some(v) => error::BadRequest {
                e: format!("failed to parse plan: unknown token {v}"),
            }
            .fail()?,
            None => error::BadRequest { e: "empty plan" }.fail()?,
        }
    }

    pub(crate) fn execute(&'a self, tx: &'a Transaction) -> DocumentStream<'a> {
        match self {
            Plan::Union(plans) => {
                let streams = plans.iter().map(|p| p.execute(tx));
                stream::select_all(streams).boxed()
            }
            Plan::Filter(filter) => filter
                .driver
                .execute(tx)
                .try_filter_map(async |doc| {
                    if filter.expr.evaluate(&doc) {
                        Ok(Some(doc))
                    } else {
                        Ok(None)
                    }
                })
                .boxed(),
            Plan::Fullscan(fullscan) => fullscan.execute(tx).boxed(),
            Plan::IdxScan(idx_scan) => idx_scan.execute(tx).boxed(),
        }
    }
}
