use std::future;

use foundationdb::{
    FdbError, RangeOption, TransactError, Transaction, TransactionCancelled,
    future::FdbValues,
    tuple::{self, Subspace, Versionstamp},
};
use futures::{Stream, StreamExt, TryStreamExt, future::Either, stream};
use rmpv::Value;
use sexpression::Expression as Sexpr;
use snafu::{ResultExt, whatever};

use crate::{
    error::{self, AppError, MPVDecode},
    schema::{Collection, CollectionSchema, IndexDef, SchemaVersion},
    storage::{DB, DocID, Document},
};

#[derive(Clone, PartialEq, Debug)]
enum Expression {
    And(Vec<Expression>),
    Or(Vec<Expression>),
    Not(Box<Expression>),
    Atomic(Predicate),
    Empty,
}

#[derive(Clone, PartialEq, Debug)]
enum Operator {
    Eq(Value),
    Gt(Value),
    Lt(Value),
}

#[derive(Clone, PartialEq, Debug)]
struct Predicate {
    fld: String,
    op: Operator,
    idx: Option<String>,
}

pub(crate) enum Plan {
    Join(Vec<Plan>),
    Intersect(Vec<Plan>),
    Fullscan(Expression),
    Ixscan {
        fields: Vec<Predicate>,
        idx: String,
    },
    IxscanFilter {
        fields: Vec<Predicate>,
        idx: String,
        filter: Expression,
    },
}

struct PlanRuntime<'a> {
    plan: Plan,
    tx: &'a Transaction,
}

// struct IdxScan<'a, S: Stream<Item = Result<FdbValues, FdbError>> + Send + Sync + 'a> {
struct IdxScan<'a> {
    tx: &'a Transaction,
    // fields: Vec<Predicate>,
    collection: &'a Collection,
    idx_space: Subspace,
    filter: Option<Expression>,
    // idx_stream: Option<S>,
}

struct Fullscan<'a> {
    tx: &'a Transaction,
    collection: &'a Collection,
    filter: Expression,
}

impl Fullscan<'_> {
    fn execute(&self) -> impl Stream<Item = Result<Document, AppError>> {
        let opt = RangeOption::from(&self.collection.subspace());
        self.tx
            .get_ranges_keyvalues(opt, false)
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
    fn execute(&self) -> impl Stream<Item = Result<Document, AppError>> {
        let opt = RangeOption::from(&self.idx_space);
        self.tx
            .get_ranges_keyvalues(opt, false)
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
                let Some(doc) = DB::get_doc_tx(self.collection, &doc_id, self.tx).await? else {
                    whatever!("missing indexed document {doc_id}");
                };
                match &self.filter {
                    Some(f) if f.evaluate(&doc) => Ok(Some(doc)),
                    Some(_) => Ok(None),
                    None => Ok(Some(doc)),
                }
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

impl Plan {
    pub(crate) fn from_query(
        collection: &Collection,
        schema: &CollectionSchema,
        query: &str,
    ) -> Result<Self, AppError> {
        todo!()
    }

    fn execute(self, tx: &Transaction) -> impl Stream<Item = Result<rmpv::Value, AppError>> {
        PlanRuntime { plan: self, tx: tx }
    }
}

impl TryFrom<&sexpression::Expression<'_>> for Expression {
    type Error = AppError;
    fn try_from(sexpr: &sexpression::Expression) -> Result<Self, Self::Error> {
        let Sexpr::List(list) = sexpr else {
            error::BadRequest {
                e: "query expression must be list",
            }
            .fail()?
        };
        match list.get(0) {
            Some(Sexpr::Symbol(op)) => match *op {
                "and" => {
                    assert_longer(&list, 2)?;
                    let mut v = Vec::with_capacity(list.len() - 1);
                    for e in list.iter().skip(1) {
                        v.push(Self::try_from(e)?);
                    }
                    Ok(Self::And(v))
                }
                "or" => {
                    assert_longer(&list, 2)?;
                    let mut v = Vec::with_capacity(list.len() - 1);
                    for e in list.iter().skip(1) {
                        v.push(Self::try_from(e)?);
                    }
                    Ok(Self::Or(v))
                }
                "not" => {
                    assert_len(&list, 2)?;
                    Ok(Self::Not(Box::new(Self::try_from(&list[1])?)))
                }
                "eq" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(Predicate {
                        fld,
                        op: Operator::Eq(arg),
                        idx: None,
                    }))
                }
                "gt" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(Predicate {
                        fld,
                        op: Operator::Gt(arg),
                        idx: None,
                    }))
                }
                "ge" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Or(vec![
                        // (ge .f v) is expanded to (or (gt .f v) (eq .f v))
                        Self::Atomic(Predicate {
                            fld: fld.clone(),
                            op: Operator::Gt(arg.clone()),
                            idx: None,
                        }),
                        Self::Atomic(Predicate {
                            fld,
                            op: Operator::Eq(arg),
                            idx: None,
                        }),
                    ]))
                }
                "lt" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(Predicate {
                        fld,
                        op: Operator::Lt(arg),
                        idx: None,
                    }))
                }
                "le" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Or(vec![
                        // (le .f v) is expanded to (or (lt .f v) (eq .f v))
                        Self::Atomic(Predicate {
                            fld: fld.clone(),
                            op: Operator::Lt(arg.clone()),
                            idx: None,
                        }),
                        Self::Atomic(Predicate {
                            fld,
                            op: Operator::Eq(arg),
                            idx: None,
                        }),
                    ]))
                }
                "in" => {
                    assert_longer(&list, 2)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let mut arg = Vec::with_capacity(list.len() - 2);
                    for v in list.iter().skip(2) {
                        arg.push(Self::extract_constant(v)?);
                    }
                    // (in .f v1 2 ...) is expaned into (or (eq f v1) (eq .f2 v2) ...)
                    Ok(Self::Or(
                        arg.into_iter()
                            .map(|v| {
                                Self::Atomic(Predicate {
                                    fld: fld.clone(),
                                    op: Operator::Eq(v),
                                    idx: None,
                                })
                            })
                            .collect(),
                    ))
                }
                op => error::BadRequest {
                    e: format!("unknown operator {op}"),
                }
                .fail()?,
            },
            Some(v) => error::BadRequest {
                e: format!("unexpected token {v:?}: predicate must start with operator"),
            }
            .fail()?,
            None => Ok(Self::Empty),
        }
    }
}

impl TryFrom<&str> for Expression {
    type Error = AppError;

    fn try_from(query: &str) -> Result<Self, Self::Error> {
        let (expr, _) = sexpression::read(query).context(error::QueryParse {
            e: "failed to parse query",
        })?;
        let Sexpr::List(_) = expr else {
            error::BadRequest {
                e: "query must be list",
            }
            .fail()?
        };
        Self::try_from(&expr)
    }
}

impl Expression {
    fn evaluate(&self, doc: &Document) -> bool {
        todo!()
    }

    fn hydrate_indexes(&mut self, schema: &CollectionSchema) {
        match self {
            Expression::And(expressions) => {
                expressions
                    .iter_mut()
                    .for_each(|e| e.hydrate_indexes(schema));
            }
            Expression::Or(expressions) => {
                expressions
                    .iter_mut()
                    .for_each(|e| e.hydrate_indexes(schema));
            }
            Expression::Not(expression) => {
                expression.hydrate_indexes(schema);
            }
            Expression::Atomic(predicate) => {
                for (index, def) in schema.indexes.iter() {
                    if def.ready && def.fields[0].0 == predicate.fld {
                        predicate.idx = Some(index.clone());
                        break;
                    }
                }
            }
            Expression::Empty => {}
        }
    }

    fn is_sargable(&self) -> bool {
        match self {
            Expression::And(expressions) => expressions.iter().any(|e| e.is_sargable()),
            Expression::Or(expressions) => expressions.iter().all(|e| e.is_sargable()),
            Expression::Not(_) => false,
            Expression::Empty => true,
            Expression::Atomic(predicate) => predicate.idx.is_some(),
        }
    }

    fn extract_field_ref(sexpr: &Sexpr) -> Result<String, AppError> {
        let Sexpr::Symbol(fld) = sexpr else {
            error::BadRequest {
                e: "field name expected, got {sexpr:?}",
            }
            .fail()?
        };
        if !fld.starts_with(".") {
            error::BadRequest {
                e: "field name should start with dot, got {fld}",
            }
            .fail()?
        }
        Ok(String::from(*fld))
    }

    fn extract_constant(sexpr: &Sexpr) -> Result<Value, AppError> {
        match sexpr {
            Sexpr::Number(n) => {
                if n.fract() == 0.0 {
                    Ok(rmpv::Value::from(*n as i64))
                } else {
                    Ok(rmpv::Value::from(*n))
                }
            }
            Sexpr::Bool(b) => Ok(rmpv::Value::from(*b)),
            Sexpr::Str(s) => Ok(rmpv::Value::from(*s)),
            v => error::BadRequest {
                e: format!("constant expected, got {v:?}"),
            }
            .fail()?,
        }
    }
}

fn assert_longer(v: &[Sexpr], len: usize) -> Result<(), AppError> {
    if v.len() <= len {
        error::BadRequest {
            e: format!("invalid length of expression {v:?}: more than {len} expected"),
        }
        .fail()?
    }
    Ok(())
}

fn assert_len(v: &[Sexpr], len: usize) -> Result<(), AppError> {
    if v.len() != len {
        error::BadRequest {
            e: format!("invalid length of expression {v:?}: {len} expected"),
        }
        .fail()?
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::planner::{Expression, Operator, Predicate};

    #[test]
    fn parsing() {
        let p = Expression::try_from("(eq .foo 137)").unwrap();
        assert_eq!(
            p,
            Expression::Atomic(Predicate {
                fld: String::from(".foo"),
                op: Operator::Eq(rmpv::Value::from(137)),
                idx: None,
            })
        );

        let p = Expression::try_from(r#"(and (eq .foo 137) (eq .bar "chlos"))"#).unwrap();
        assert_eq!(
            p,
            Expression::And(vec![
                Expression::Atomic(Predicate {
                    fld: String::from(".foo"),
                    op: Operator::Eq(rmpv::Value::from(137)),
                    idx: None,
                }),
                Expression::Atomic(Predicate {
                    fld: String::from(".bar"),
                    op: Operator::Eq(rmpv::Value::from("chlos")),
                    idx: None,
                }),
            ])
        );
    }
}
