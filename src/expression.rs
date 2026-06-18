use std::{collections::HashSet, ops::Add};

use crate::{
    document::Document,
    error::{self, AppError},
    misc::{assert_len, assert_longer},
    schema::CollectionSchema,
    values::{self, Value},
};
use sexpression::Expression as Sexpr;
use snafu::ResultExt;
use tracing::instrument::WithSubscriber;

pub(crate) struct Update(Vec<AtomicUpdate>);
enum AtomicUpdate {
    Set(Box<str>, Value),
    Add(Box<str>, Value),
    Delete(Box<str>),
    Drop(),
    //Push(Value), TODO: once we add support of arrays
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    Atomic(AtomicPredicate),
    Empty,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Relation {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct AtomicPredicate {
    pub(crate) fld: String,
    pub(crate) rel: Relation,
    pub(crate) val: Value,
}

impl AtomicPredicate {
    fn evaluate(&self, doc: &Document) -> bool {
        let Some(lhs) = doc.value.extract_field(&self.fld) else {
            return false;
        };
        let rhs = &self.val;

        match self.rel {
            Relation::Eq => lhs == rhs,
            Relation::Gt => lhs > rhs,
            Relation::Ge => lhs >= rhs,
            Relation::Lt => lhs < rhs,
            Relation::Le => lhs <= rhs,
        }
    }
}

impl Predicate {
    pub(crate) fn evaluate(&self, doc: &Document) -> bool {
        match self {
            Predicate::And(expressions) => expressions.iter().all(|e| e.evaluate(doc)),
            Predicate::Or(expressions) => expressions.iter().any(|e| e.evaluate(doc)),
            Predicate::Not(expression) => !expression.evaluate(doc),
            Predicate::Atomic(predicate) => predicate.evaluate(doc),
            Predicate::Empty => true,
        }
    }

    // fn is_sargable(&self) -> bool {
    //     match self {
    //         Expression::And(expressions) => expressions.iter().any(|e| e.is_sargable()),
    //         Expression::Or(expressions) => expressions.iter().all(|e| e.is_sargable()),
    //         Expression::Not(_) => false,
    //         Expression::Empty => true,
    //         Expression::Atomic(predicate) => predicate.idx.is_some(),
    //     }
    // }

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
                    Ok(Value::from(*n as i64))
                } else {
                    Ok(Value::from(*n))
                }
            }
            Sexpr::Bool(b) => Ok(Value::from(*b)),
            Sexpr::Str(s) => Ok(Value::from(*s)),
            v => error::BadRequest {
                e: format!("constant expected, got {v:?}"),
            }
            .fail()?,
        }
    }
}

impl TryFrom<&str> for Predicate {
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

impl TryFrom<&sexpression::Expression<'_>> for Predicate {
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
                    Ok(Self::Atomic(AtomicPredicate {
                        fld,
                        rel: Relation::Eq,
                        val: arg,
                    }))
                }
                "gt" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(AtomicPredicate {
                        fld,
                        rel: Relation::Gt,
                        val: arg,
                    }))
                }
                "ge" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(AtomicPredicate {
                        fld: fld.clone(),
                        rel: Relation::Ge,
                        val: arg,
                    }))
                }
                "lt" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(AtomicPredicate {
                        fld,
                        rel: Relation::Lt,
                        val: arg,
                    }))
                }
                "le" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(AtomicPredicate {
                        fld,
                        rel: Relation::Le,
                        val: arg,
                    }))
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
                                Self::Atomic(AtomicPredicate {
                                    fld: fld.clone(),
                                    rel: Relation::Eq,
                                    val: v,
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
impl TryFrom<&str> for Update {
    type Error = AppError;

    fn try_from(query: &str) -> Result<Self, Self::Error> {
        let (expr, _) = sexpression::read(query).context(error::QueryParse {
            e: "failed to parse update query",
        })?;
        let Sexpr::List(updates) = expr else {
            error::BadRequest {
                e: "update query must be list of update expressions",
            }
            .fail()?
        };

        let mut r: Vec<AtomicUpdate> = Vec::with_capacity(updates.len());
        for e in &updates {
            r.push(e.try_into()?);
        }
        Ok(Self(r))
    }
}

impl TryFrom<&sexpression::Expression<'_>> for AtomicUpdate {
    type Error = AppError;
    fn try_from(sexpr: &sexpression::Expression) -> Result<Self, Self::Error> {
        let Sexpr::List(args) = sexpr else {
            error::BadRequest {
                e: "update expression must be a list",
            }
            .fail()?
        };
        assert_longer(args, 0)?;
        match &args[0] {
            Sexpr::Symbol("add") => {
                assert_len(args, 3)?;
                let Sexpr::Symbol(fld) = args[1] else {
                    error::BadRequest {
                        e: "field reference expected in 'add' update expression",
                    }
                    .fail()?
                };
                let v = Value::from_sexpr(&args[2])?;
                Ok(Self::Add(fld.into(), v))
            }
            Sexpr::Symbol("set") => {
                assert_len(args, 3)?;
                let Sexpr::Symbol(fld) = args[1] else {
                    error::BadRequest {
                        e: "field reference expected in 'set' update expression",
                    }
                    .fail()?
                };
                let v = Value::from_sexpr(&args[2])?;
                Ok(Self::Set(fld.into(), v))
            }
            Sexpr::Symbol("delete") => {
                assert_len(args, 2)?;
                let Sexpr::Symbol(fld) = args[1] else {
                    error::BadRequest {
                        e: "field reference expected in 'set' update expression",
                    }
                    .fail()?
                };
                Ok(Self::Delete(fld.into()))
            }
            Sexpr::Symbol("drop") => {
                assert_len(args, 1)?;
                Ok(Self::Drop())
            }
            v => error::BadRequest {
                e: format!("update expression syntax error: unexpected token {v:?}"),
            }
            .fail()?,
        }
    }
}

impl Update {
    /// apply modifies document according the update query. If it returns Ok(true), the document shall be dropped
    pub(crate) fn apply(&self, doc: &mut Document) -> Result<bool, AppError> {
        let mut drop = false;
        for u in &self.0 {
            drop = drop || u.apply(doc)?;
        }
        Ok(drop)
    }

    pub(crate) fn get_affected_indexes<'a>(
        &'a self,
        schema: &'a CollectionSchema,
    ) -> Vec<Box<str>> {
        let mut r = HashSet::new();
        for u in &self.0 {
            match u {
                AtomicUpdate::Set(fld, _) => {
                    r.insert(fld.clone());
                }
                AtomicUpdate::Add(fld, _) => {
                    r.insert(fld.clone());
                }
                AtomicUpdate::Delete(fld) => {
                    r.insert(fld.clone());
                }
                AtomicUpdate::Drop() => {
                    for (_, def) in &schema.indexes {
                        for (fld, _) in &def.fields {
                            r.insert(fld.clone());
                        }
                    }
                }
            }
        }
        r.into_iter().collect()
    }
}

impl AtomicUpdate {
    fn apply(&self, doc: &mut Document) -> Result<bool, AppError> {
        match self {
            AtomicUpdate::Set(fld, value) => {
                doc.value.update_field(&fld, |_| Ok(Some(value.clone())))?;
                Ok(false)
            }
            AtomicUpdate::Add(fld, value) => {
                doc.value.update_field(&fld, |v| {
                    if let Some(v) = v {
                        *v += value.clone();
                    }
                    Ok(None)
                })?;
                Ok(false)
            }
            AtomicUpdate::Delete(fld) => {
                doc.value.delete_field(&fld)?;
                Ok(false)
            }
            AtomicUpdate::Drop() => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{
        document::{DocID, Document},
        expression::{AtomicPredicate, Predicate, Relation},
        values::{Value, json2mp},
    };

    #[test]
    fn parsing() {
        let p = Predicate::try_from("(eq .foo 137)").unwrap();
        assert_eq!(
            p,
            Predicate::Atomic(AtomicPredicate {
                fld: String::from(".foo"),
                rel: Relation::Eq,
                val: Value::from(137),
            })
        );

        let p = Predicate::try_from(r#"(and (lt .foo 137) (eq .bar "chlos"))"#).unwrap();
        assert_eq!(
            p,
            Predicate::And(vec![
                Predicate::Atomic(AtomicPredicate {
                    fld: String::from(".foo"),
                    rel: Relation::Lt,
                    val: Value::from(137),
                }),
                Predicate::Atomic(AtomicPredicate {
                    fld: String::from(".bar"),
                    rel: Relation::Eq,
                    val: Value::from("chlos"),
                }),
            ])
        );
    }

    #[test]
    fn evaluation() {
        let p = Predicate::try_from("(eq .foo 137)").unwrap();
        let mut doc = Document {
            id: DocID::default(),
            value: json2mp(json!({})).into(),
        };
        assert!(!p.evaluate(&doc));

        doc.value = json2mp(json!({"foo": 137, "bar": "chlos", "baz": {"baq": 300}})).into();
        assert!(p.evaluate(&doc));

        let p = Predicate::try_from("(gt .foo 0)").unwrap();
        assert!(p.evaluate(&doc));

        let p = Predicate::try_from("(eq .bar \"chlos\")").unwrap();
        assert!(p.evaluate(&doc));

        let p = Predicate::try_from("(eq .baz.baq 300)").unwrap();
        assert!(p.evaluate(&doc));
    }
}
