use foundationdb::{
    KeySelector, RangeOption, Transaction,
    tuple::{self, Subspace, Versionstamp},
};
use futures::{Stream, StreamExt, TryStreamExt, stream};
use sexpression::Expression as Sexpr;
use snafu::{OptionExt, ResultExt, whatever};
use tracing::debug;

use crate::{
    document::{DocID, Document},
    error::{self, AppError},
    expression::Expression,
    misc::{assert_len, assert_longer},
    schema::{Collection, CollectionSchema, SchemaVersion},
    storage::DB,
    values::Value,
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
    range: RangeOption<'a>,
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
    fn from_expr(
        expr: &Sexpr,
        collection: &'a Collection,
        schema: &CollectionSchema,
    ) -> Result<Self, AppError> {
        let Sexpr::List(exprs) = expr else {
            return error::BadRequest {
                e: "ixscan argument must be a list",
            }
            .fail()?;
        };
        let Sexpr::Symbol(idx_name) = exprs[1] else {
            return error::BadRequest {
                e: "invalid ixscan expression, index name is expected",
            }
            .fail();
        };
        let index = schema.indexes.get(idx_name).context(error::BadRequest {
            e: format!("unknown index {idx_name}"),
        })?;
        if !index.ready {
            return error::BadRequest {
                e: format!("index {idx_name} is not yet ready to use"),
            }
            .fail();
        }

        match (exprs.get(0), exprs.get(2), exprs.get(3)) {
            (Some(Sexpr::Symbol(op)), Some(Sexpr::List(op_values)), None) => {
                // (ixscan (eq idx_name (137 "foo")))
                let (idx_space_begin, idx_space_end) = collection.index_subspace(idx_name).range();
                let mut idx_subspace = collection.index_subspace(idx_name);
                if index.fields.len() != op_values.len() {
                    return error::BadRequest {
                        e: format!(
                            "index {idx_name} has {} fields, {} values given",
                            index.fields.len(),
                            op_values.len()
                        ),
                    }
                    .fail();
                }
                for value in op_values {
                    idx_subspace = idx_subspace.subspace(&Value::from_sexpr(value)?);
                }

                let (first, last) = idx_subspace.range();
                let range = match *op {
                    "eq" => RangeOption::from(&idx_subspace),
                    "ge" => RangeOption {
                        begin: KeySelector::first_greater_or_equal(first),
                        end: KeySelector::first_greater_than(idx_space_end),
                        ..Default::default()
                    },
                    "gt" => RangeOption {
                        begin: KeySelector::first_greater_than(last),
                        end: KeySelector::first_greater_than(idx_space_end),
                        ..Default::default()
                    },
                    "le" => RangeOption {
                        begin: KeySelector::first_greater_or_equal(idx_space_begin),
                        end: KeySelector::first_greater_than(last),
                        ..Default::default()
                    },
                    "lt" => RangeOption {
                        begin: KeySelector::first_greater_than(idx_space_begin),
                        end: KeySelector::last_less_than(last),
                        ..Default::default()
                    },
                    v => {
                        return error::BadRequest {
                            e: format!("invalid ixscan operation {v}"),
                        }
                        .fail();
                    }
                };
                return Ok(Self { collection, range });
            }
            (
                Some(Sexpr::Symbol(intvl_op)),
                Some(Sexpr::List(left_values)),
                Some(Sexpr::List(right_values)),
            ) => {
                // (ixscan (interval idx_name (137 "foo") (731 "bar")))
                let mut left_subspace = collection.index_subspace(idx_name);
                let mut right_subspace = collection.index_subspace(idx_name);
                if index.fields.len() != left_values.len()
                    || index.fields.len() != right_values.len()
                {
                    return error::BadRequest {
                        e: format!(
                            "invalid number of fields in range expression, index {idx_name} has {} fields",
                            index.fields.len(),
                        ),
                    }
                    .fail();
                }
                for value in left_values {
                    left_subspace = left_subspace.subspace(&Value::from_sexpr(value)?);
                }
                for value in right_values {
                    right_subspace = right_subspace.subspace(&Value::from_sexpr(value)?);
                }
                let (left_first, left_last) = left_subspace.range();
                let (right_first, right_last) = right_subspace.range();
                let range = match *intvl_op {
                    "interval" => RangeOption {
                        begin: KeySelector::first_greater_or_equal(left_first),
                        end: KeySelector::first_greater_than(right_last),
                        ..Default::default()
                    },
                    "right-interval" => RangeOption {
                        begin: KeySelector::first_greater_or_equal(left_first),
                        end: KeySelector::last_less_than(right_last),
                        ..Default::default()
                    },
                    "left-interval" => RangeOption {
                        begin: KeySelector::first_greater_than(left_last),
                        end: KeySelector::first_greater_than(right_last),
                        ..Default::default()
                    },
                    "open-interval" => RangeOption {
                        begin: KeySelector::first_greater_than(left_last),
                        end: KeySelector::last_less_than(right_last),
                        ..Default::default()
                    },
                    v => {
                        return error::BadRequest {
                            e: format!("invalid range expression {v}"),
                        }
                        .fail();
                    }
                };
                return Ok(Self { collection, range });
            }
            _ => {
                return error::BadRequest {
                    e: format!("invalid ixscan for index {idx_name}"),
                }
                .fail();
            }
        };
    }

    fn execute(&self, tx: &'a Transaction) -> impl Stream<Item = Result<Document, AppError>> {
        // let range = RangeOption::from(&self.idx_subspace);
        tx.get_ranges_keyvalues(self.range.clone(), false)
            .map_err(|e| AppError::Fdb {
                e: String::from("scanning index"),
                source: e,
            })
            .try_filter_map(async |value| {
                let elems: Vec<tuple::Element> =
                    tuple::unpack(value.key()).context(error::FdbTupleUnpack)?;
                if elems.len() < 7 {
                    whatever!("unexpected index key length: {}", elems.len());
                }
                // last two elements of tuple are schema version and versionstamp that define document ID
                let (schema_version, versionstamp) =
                    match (&elems[elems.len() - 2], &elems[elems.len() - 1]) {
                        (tuple::Element::Int(n), tuple::Element::Versionstamp(vs)) => {
                            (*n as u32, vs.clone())
                        }
                        _ => whatever!("unexpected index key format"),
                    };
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
                    assert_len(&list, 2)?;
                    Ok(Self::IdxScan(IdxScan::from_expr(
                        &list[1], collection, schema,
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

#[cfg(test)]
mod tests {
    use crate::schema::Collection;

    #[test]
    fn test_unpack() {
        let coll = Collection {
            db: Box::from("testdb"),
            collection: Box::from("testcol"),
        };

        let subspace = coll
            .index_subspace("testindex")
            .subspace(&1u32)
            .subspace(&2u32)
            .subspace(&3u32)
            .subspace(&4u32);
        let key = subspace.pack(&"foo");
        let tp: Vec<foundationdb::tuple::Element> = subspace.unpack(&key).unwrap();
        match tp.last().unwrap() {
            foundationdb::tuple::Element::String(cow) => assert_eq!(cow, "fo11o"),
            _ => panic!("aaa"),
        }
    }
}
