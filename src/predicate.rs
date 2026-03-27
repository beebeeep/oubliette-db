use crate::error::{self, AppError};
use sexpression::Expression;
use snafu::{ResultExt, ensure, whatever};

pub(crate) struct Predicate<'a> {
    expressions: Vec<sexpression::Expression<'a>>,
}

impl<'a> Predicate<'a> {
    pub(crate) fn from_query(query: &'a str) -> Result<Self, AppError> {
        let expr = sexpression::read(query).context(error::QueryParse {
            e: "failed to parse query",
        })?;

        // TODO: more validations
        if let Expression::List(expressions) = expr {
            Ok(Self { expressions })
        } else {
            whatever!("query expression must be a list")
        }
    }

    pub(crate) fn execute(&self, value: &rmpv::Value) -> Result<bool, AppError> {
        if let Expression::Symbol(sym) = self.expressions[0] {}
        todo!()
    }
}
