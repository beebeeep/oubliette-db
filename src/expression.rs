use crate::{
    error::{self, AppError},
    schema::CollectionSchema,
    storage::Document,
    values::{Value, extract_field},
};
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
pub(crate) enum Relation {
    Eq,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Predicate {
    fld: String,
    rel: Relation,
    val: Value,
}

impl Predicate {
    fn evaluate(&self, doc: &Document) -> bool {
        let Some(lhs) = extract_field(&self.fld, &doc.doc) else {
            return false;
        };
        let rhs = Value::from_ref(&doc.doc);

        match self.rel {
            Relation::Eq => lhs == rhs,
            Relation::Gt => lhs > rhs,
            Relation::Ge => lhs >= rhs,
            Relation::Lt => lhs < rhs,
            Relation::Le => lhs <= rhs,
        }
    }
}

impl Expression {
    pub(crate) fn evaluate(&self, doc: &Document) -> bool {
        match self {
            Expression::And(expressions) => expressions.iter().all(|e| e.evaluate(doc)),
            Expression::Or(expressions) => expressions.iter().any(|e| e.evaluate(doc)),
            Expression::Not(expression) => !expression.evaluate(doc),
            Expression::Atomic(predicate) => predicate.evaluate(doc),
            Expression::Empty => true,
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
                        rel: Relation::Eq,
                        val: arg,
                    }))
                }
                "gt" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(Predicate {
                        fld,
                        rel: Relation::Gt,
                        val: arg,
                    }))
                }
                "ge" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(Predicate {
                        fld: fld.clone(),
                        rel: Relation::Ge,
                        val: arg,
                    }))
                }
                "lt" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(Predicate {
                        fld,
                        rel: Relation::Lt,
                        val: arg,
                    }))
                }
                "le" => {
                    assert_len(&list, 3)?;
                    let fld = Self::extract_field_ref(&list[1])?;
                    let arg = Self::extract_constant(&list[2])?;
                    Ok(Self::Atomic(Predicate {
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
                                Self::Atomic(Predicate {
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
    use crate::expression::{Expression, Predicate, Relation};

    #[test]
    fn parsing() {
        let p = Expression::try_from("(eq .foo 137)").unwrap();
        assert_eq!(
            p,
            Expression::Atomic(Predicate {
                fld: String::from(".foo"),
                op: Relation::Eq(Value::from(137)),
                idx: None,
            })
        );

        let p = Expression::try_from(r#"(and (eq .foo 137) (eq .bar "chlos"))"#).unwrap();
        assert_eq!(
            p,
            Expression::And(vec![
                Expression::Atomic(Predicate {
                    fld: String::from(".foo"),
                    op: Relation::Eq(Value::from(137)),
                    idx: None,
                }),
                Expression::Atomic(Predicate {
                    fld: String::from(".bar"),
                    op: Relation::Eq(Value::from("chlos")),
                    idx: None,
                }),
            ])
        );
    }
}
