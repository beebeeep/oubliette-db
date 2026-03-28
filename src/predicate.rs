use std::collections::HashMap;

use crate::error::{self, AppError};
use sexpression::Expression;
use snafu::{ResultExt, whatever};

pub(crate) struct Predicate<'a> {
    referred_fields: Vec<&'a str>,
    expression: sexpression::Expression<'a>,
}

impl<'a> Predicate<'a> {
    pub(crate) fn from_query(query: &'a str) -> Result<Self, AppError> {
        let expression = sexpression::read(query).context(error::QueryParse {
            e: "failed to parse query",
        })?;

        let mut p = Self {
            expression,
            referred_fields: Vec::with_capacity(2),
        };
        p.extract_referred_fields();

        Ok(p)
    }

    pub(crate) fn execute(&self, value: &rmpv::Value) -> Result<bool, AppError> {
        Val::new(value, Some(&self.expression))?.evaluate()
    }

    fn extract_referred_fields(&mut self) {
        let mut expressions = vec![&self.expression];
        while let Some(expr) = expressions.pop() {
            match expr {
                Expression::Symbol(sym) => {
                    if sym.starts_with(".") {
                        self.referred_fields.push(*sym);
                    }
                }
                Expression::List(exprs) => {
                    for expr in exprs {
                        expressions.push(expr);
                    }
                }
                _ => {}
            }
        }
    }

    fn get_referred_fields(&mut self, value: &rmpv::Value) -> HashMap<&str, rmpv::Value> {
        let mut r = HashMap::new();
        'NEXT_FIELD: for path in &self.referred_fields {
            let v = value;
            for field in path.split(".").skip(1) {
                if let rmpv::Value::Map(items) = v {
                    for (k, v) in items {
                        let k = if let Some(s) = k.as_str() {
                            s
                        } else {
                            continue 'NEXT_FIELD;
                        };
                        if k != field {
                            continue 'NEXT_FIELD;
                        }
                        todo!()
                    }
                } else {
                    continue 'NEXT_FIELD;
                }
            }
        }
        r
    }
}

struct Val<'a> {
    doc: &'a rmpv::Value,
    expr: &'a sexpression::Expression<'a>,
}

impl<'a> Val<'a> {
    fn new(
        doc: &'a rmpv::Value,
        expr: Option<&'a sexpression::Expression>,
    ) -> Result<Self, AppError> {
        if let Some(expr) = expr {
            Ok(Self { doc, expr })
        } else {
            whatever!("syntax error")
        }
    }
}

impl<'a> Val<'a> {
    fn get_from_doc(&self, path: &str) -> Option<&'a Expression<'a>> {
        if !path.starts_with(".") {
            return None;
        }
        Some(&Expression::Number(137.0))
    }

    fn evaluate(&self) -> Result<bool, AppError> {
        let r = match self.expr {
            Expression::Number(_) => true,
            Expression::Bool(b) => *b,
            Expression::Str(_) => true,
            Expression::Symbol("false") | Expression::Symbol("nil") | Expression::Symbol("#f") => {
                false
            }
            Expression::Symbol(_) => true, // all other symbols are "truthy"
            Expression::List(expressions) => self.eval_list(expressions)?,
            Expression::Null => false,
        };
        Ok(r)
    }

    fn eval_list(&self, exprs: &[sexpression::Expression]) -> Result<bool, AppError> {
        if exprs.is_empty() {
            return Ok(true);
        }
        match &exprs.get(0) {
            Some(Expression::Symbol(op)) => {
                let result = match *op {
                    "eq" => Val::new(self.doc, exprs.get(1))? == Val::new(self.doc, exprs.get(2))?,
                    "in" => {
                        // (in .foo (1 2 3 4))
                        if exprs.len() < 3 {
                            whatever!("syntax error");
                        }
                        for v in &exprs[2..] {
                            if Val::new(self.doc, exprs.get(1))? == Val::new(self.doc, Some(v))? {
                                return Ok(true);
                            }
                        }
                        false
                    }
                    "ge" => Val::new(self.doc, exprs.get(1))? >= Val::new(self.doc, exprs.get(2))?,
                    "gt" => Val::new(self.doc, exprs.get(1))? > Val::new(self.doc, exprs.get(2))?,
                    "le" => Val::new(self.doc, exprs.get(1))? <= Val::new(self.doc, exprs.get(2))?,
                    "lt" => Val::new(self.doc, exprs.get(1))? < Val::new(self.doc, exprs.get(2))?,
                    "and" => {
                        // (and (eq .foo "chlos") (eq .bar 137))
                        if exprs.len() < 3 {
                            whatever!("syntax error")
                        }
                        let mut r = true;
                        for v in &exprs[2..] {
                            r = r && Val::new(self.doc, Some(v))?.evaluate()?;
                        }
                        r
                    }
                    "or" => {
                        // (or (eq .foo "chlos") (eq .bar 137))
                        if exprs.len() < 3 {
                            whatever!("syntax error")
                        }
                        let mut r = false;
                        for v in &exprs[2..] {
                            r = r || Val::new(self.doc, Some(v))?.evaluate()?;
                        }
                        r
                    }
                    "not" => !Val::new(self.doc, exprs.get(1))?.evaluate()?,
                    s => {
                        whatever!("invalid operation '{s}'");
                    }
                };
                Ok(result)
            }
            Some(v) => {
                whatever!("unexpected token {v:?} in query expression");
            }
            None => {
                whatever!("syntax error in query")
            }
        }
    }
}

impl<'a> PartialEq for Val<'a> {
    fn eq(&self, other: &Self) -> bool {
        // atm we don't support any calculations inside predicates, so we expect that
        // both values will be either literal number or string, or document reference

        // first, dereference values
        let a = if let Expression::Symbol(path) = self.expr {
            if let Some(v) = self.get_from_doc(path) {
                v
            } else {
                return false;
            }
        } else {
            self.expr
        };
        let b = if let Expression::Symbol(path) = other.expr {
            if let Some(v) = self.get_from_doc(path) {
                v
            } else {
                return false;
            }
        } else {
            other.expr
        };

        match (a, b) {
            (Expression::Number(a), Expression::Number(b)) => a == b,
            (Expression::Str(a), Expression::Str(b)) => a == b,
            (Expression::Bool(a), Expression::Bool(b)) => a == b,
            _ => false,
        }
    }
}

impl<'a> PartialOrd for Val<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::predicate::Predicate;

    #[test]
    fn exprs() {
        // let a = sexpression::read(r#"(and (eq .foo "chlos") (eq .baz 137))"#).unwrap();
        let a = ".foo.bar.baz".split(".").skip(1);
        for v in a {
            println!("{v:?}");
        }
    }

    #[test]
    fn test_referred_fields_extraction() {
        let p = Predicate::from_query(r#"(and (eq .foo "chlos") (eq '.baz.bar' 137))"#).unwrap();
        assert!(p.referred_fields.contains(&".foo"));
        assert!(p.referred_fields.contains(&".baz.bar"));
    }
}
