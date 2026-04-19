use crate::{
    error::{self, AppError},
    schema::CollectionSchema,
    storage::Document,
};
use rmpv::Value;
use sexpression::Expression as Sexpr;
use snafu::ResultExt;

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Expression {
    And(Vec<Expression>),
    Or(Vec<Expression>),
    Not(Box<Expression>),
    Atomic(Predicate),
    Empty,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Operator {
    Eq(Value),
    Gt(Value),
    Lt(Value),
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Predicate {
    fld: String,
    op: Operator,
    idx: Option<String>,
}

impl Expression {
    pub(crate) fn evaluate(&self, doc: &Document) -> bool {
        todo!()
    }

    pub(crate) fn hydrate_indexes(&mut self, schema: &CollectionSchema) {
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
    use crate::expression::{Expression, Operator, Predicate};

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
