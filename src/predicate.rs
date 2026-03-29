use std::collections::HashMap;

use crate::{
    error::{self, AppError},
    storage::DocID,
};
use foundationdb::tuple::TuplePack;
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

    pub(crate) fn execute(&self, id: &str, value: &rmpv::Value) -> Result<bool, AppError> {
        let data = self.get_referred_fields(id, value);
        Val::new(&data, Some(&self.expression))?.evaluate()
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

    fn get_referred_fields(
        &self,
        id: &'a str,
        value: &'a rmpv::Value,
    ) -> HashMap<&'a str, Expression<'a>> {
        let mut r = HashMap::new();
        r.insert(".__id", Expression::Str(id)); // always inject doc ID as a bogus field "__id"

        'NEXT_FIELD: for path in &self.referred_fields {
            // path looks like .foo.bar.baz, split it by ".", skip 1st part
            // and incrementally dig into the value, expecting that .foo and .foo.bar are objects
            // note that document may not contain fields referred by query, that is normal
            let mut tail = value;
            for field in path.split(".").skip(1) {
                if let Some(v) = Self::get_field(field, tail) {
                    tail = v;
                } else {
                    continue 'NEXT_FIELD;
                }
            }
            // convert value we got into Expression assuming it is scalar value
            let tail = match tail {
                rmpv::Value::Boolean(b) => Expression::Bool(*b),
                rmpv::Value::Integer(n) => Expression::Number(n.as_f64().unwrap()),
                rmpv::Value::F32(f) => Expression::Number(*f as f64),
                rmpv::Value::F64(f) => Expression::Number(*f),
                rmpv::Value::String(s) => match s.as_str() {
                    Some(s) => Expression::Str(s),
                    None => continue 'NEXT_FIELD,
                },
                rmpv::Value::Binary(_) => {
                    continue 'NEXT_FIELD; // TODO: support this? 
                }
                _ => {
                    continue 'NEXT_FIELD;
                }
            };
            r.insert(*path, tail);
        }
        r
    }

    fn get_field(name: &str, value: &'a rmpv::Value) -> Option<&'a rmpv::Value> {
        if let rmpv::Value::Map(items) = value {
            for (k, v) in items {
                if let Some(s) = k.as_str() {
                    if s == name {
                        return Some(v);
                    }
                }
            }
        }
        None
    }
}

struct Val<'a> {
    values: &'a HashMap<&'a str, Expression<'a>>,
    expr: &'a sexpression::Expression<'a>,
}

impl<'a> Val<'a> {
    fn new(
        values: &'a HashMap<&str, Expression<'a>>,
        expr: Option<&'a sexpression::Expression>,
    ) -> Result<Self, AppError> {
        if let Some(expr) = expr {
            Ok(Self { values, expr })
        } else {
            whatever!("syntax error")
        }
    }
}

impl<'a> Val<'a> {
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

    fn eval_list(&self, list: &[sexpression::Expression]) -> Result<bool, AppError> {
        if list.is_empty() {
            return Ok(true);
        }
        match &list.get(0) {
            Some(Expression::Symbol(op)) => {
                let result = match *op {
                    "eq" => {
                        Val::new(self.values, list.get(1))? == Val::new(self.values, list.get(2))?
                    }
                    "ne" => {
                        Val::new(self.values, list.get(1))? != Val::new(self.values, list.get(2))?
                    }
                    "in" => {
                        // (in .foo (1 2 3 4))
                        if list.len() != 3 {
                            whatever!("syntax error");
                        }
                        if let Expression::List(items) = &list[2] {
                            let rhs = Val::new(self.values, list.get(1))?;
                            for v in items {
                                if rhs == Val::new(self.values, Some(v))? {
                                    return Ok(true);
                                }
                            }
                        }
                        false
                    }
                    "ge" => {
                        Val::new(self.values, list.get(1))? >= Val::new(self.values, list.get(2))?
                    }
                    "gt" => {
                        eprintln!("{:?} > {:?}", list.get(1), list.get(2));
                        Val::new(self.values, list.get(1))? > Val::new(self.values, list.get(2))?
                    }
                    "le" => {
                        Val::new(self.values, list.get(1))? <= Val::new(self.values, list.get(2))?
                    }
                    "lt" => {
                        Val::new(self.values, list.get(1))? < Val::new(self.values, list.get(2))?
                    }
                    "and" => {
                        // (and (eq .foo "chlos") (eq .bar 137))
                        if list.len() < 3 {
                            whatever!("syntax error")
                        }
                        let mut r = true;
                        for v in &list[1..] {
                            r = r && Val::new(self.values, Some(v))?.evaluate()?;
                        }
                        r
                    }
                    "or" => {
                        // (or (eq .foo "chlos") (eq .bar 137))
                        if list.len() < 3 {
                            whatever!("syntax error")
                        }
                        let mut r = false;
                        for v in &list[1..] {
                            r = r || Val::new(self.values, Some(v))?.evaluate()?;
                        }
                        r
                    }
                    "not" => !Val::new(self.values, list.get(1))?.evaluate()?,
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

        // if value is document refence, get its value, otherwise return as is
        let lhs = if let Expression::Symbol(path) = self.expr {
            if let Some(v) = self.values.get(path) {
                v
            } else {
                return false;
            }
        } else {
            &self.expr
        };
        let rhs = if let Expression::Symbol(path) = other.expr {
            if let Some(v) = self.values.get(path) {
                v
            } else {
                return false;
            }
        } else {
            other.expr
        };

        lhs == rhs
    }
}

impl<'a> PartialOrd for Val<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // if value is document refence, get its value, otherwise return as is
        let lhs = if let Expression::Symbol(path) = self.expr {
            if let Some(v) = self.values.get(path) {
                v
            } else {
                return None;
            }
        } else {
            &self.expr
        };
        let rhs = if let Expression::Symbol(path) = other.expr {
            if let Some(v) = self.values.get(path) {
                v
            } else {
                return None;
            }
        } else {
            other.expr
        };

        match (lhs, rhs) {
            (Expression::Number(a), Expression::Number(b)) => a.partial_cmp(b),
            (Expression::Str(a), Expression::Str(b)) => a.partial_cmp(b),
            (Expression::Bool(a), Expression::Bool(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sexpression::Expression;

    use crate::{encoding::json2mp, predicate::Predicate};

    #[test]
    fn referred_fields_extraction() {
        let p = Predicate::from_query(r#"(and (eq .foo "chlos") (eq .baz.bar 137))"#).unwrap();
        assert!(p.referred_fields.contains(&".foo"));
        assert!(p.referred_fields.contains(&".baz.bar"));
        let v = json2mp(json!({"foo": 123, "baz": {"bar": "chlos", "bak": true}}));
        let fields = p.get_referred_fields("id", &v);
        assert_eq!(fields.get(".foo"), Some(&Expression::Number(123.0)));
        assert_eq!(fields.get(".baz.bar"), Some(&Expression::Str("chlos")));
        assert!(!fields.contains_key(".baz.bak"));
        assert!(fields.contains_key(".__id"));
    }

    #[test]
    fn evaluation() {
        let v = json2mp(json!({"foo": "chlos", "baz": {"bar": 137, "bak": true}}));

        // empty query
        assert_eq!(
            Predicate::from_query(r#"()"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // always true
        assert_eq!(
            Predicate::from_query(r#"true"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // always false
        assert_eq!(
            Predicate::from_query(r#"false"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            false
        );

        // get by id
        assert_eq!(
            Predicate::from_query(r#"(eq .__id "id")"#)
                .unwrap()
                .execute("id", &v)
                .unwrap(),
            true
        );

        // simple comparison
        assert_eq!(
            Predicate::from_query(r#"(eq .foo "chlos")"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // simple comparison
        assert_eq!(
            Predicate::from_query(r#"(eq .foo "chlos")"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // simple comparison
        assert_eq!(
            Predicate::from_query(r#"(ge .baz.bar 137)"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // simple comparison
        assert_eq!(
            Predicate::from_query(r#"(le .baz.bar 137)"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // simple comparison
        assert_eq!(
            Predicate::from_query(r#"(lt .baz.bar 1000)"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // simple comparison
        assert_eq!(
            // Note that currently the upstream version of s-expressions is incorrectly parsing single-digit numbers
            // https://github.com/eckertliam/s-expression/issues/3
            // so this test will fail with predicate (gt .baz.bar 0)
            Predicate::from_query(r#"(gt .baz.bar 0.0)"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );
        // not
        assert_eq!(
            Predicate::from_query(r#"(not (ge .baz.bar 10))"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            false
        );

        // not equal
        assert_eq!(
            Predicate::from_query(r#"(ne .baz.bar 166)"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // in
        assert_eq!(
            Predicate::from_query(r#"(in .foo ("chlos" "CHLOS" "chicken))"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // and
        assert_eq!(
            Predicate::from_query(r#"(and (eq .foo "chlos") (eq .baz.bar 137))"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // and, no match
        assert_eq!(
            Predicate::from_query(r#"(and (eq .foo "CHLOS") (eq .baz.bar 137))"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            false
        );

        // or
        assert_eq!(
            Predicate::from_query(r#"(or (eq .foo "chlos") (eq .baz.bar 0))"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            true
        );

        // or, no match
        assert_eq!(
            Predicate::from_query(r#"(or (eq .foo "CHLOS") (eq .baz.bar 0))"#)
                .unwrap()
                .execute("", &v)
                .unwrap(),
            false
        );
    }
}
