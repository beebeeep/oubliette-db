use rmpv::Value;
use snafu::ResultExt;

use crate::error::{self, AppError};

enum Expression {
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Atomic(Predicate),
}

enum Predicate {
    Eq(String, Value),
    Gt(String, Value),
    Lt(String, Value),
    In(String, Vec<Value>),
}

impl Expression {
    fn from_query(query: &str) -> Result<Self, AppError> {
        let (expression, _) = sexpression::read(query).context(error::QueryParse {
            e: "failed to parse query",
        })?;
    }
}
