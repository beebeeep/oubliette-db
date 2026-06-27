use std::{
    cmp::Ordering,
    ops::{Add, AddAssign},
};

use base64::prelude::*;
use bytemuck::TransparentWrapper;
use snafu::ResultExt;

use crate::error::{self, AppError};

/// Value wraps rmpv::Value providing a bit more flexible comparison operations (can compare ints with floats and different floats with each other)
#[repr(transparent)]
#[derive(Clone, Debug, TransparentWrapper)]
pub(crate) struct Value(pub(crate) rmpv::Value);

impl Value {
    pub(crate) fn from_ref(v: &rmpv::Value) -> &Self {
        TransparentWrapper::wrap_ref(v)
    }
    pub(crate) fn from_mut(v: &mut rmpv::Value) -> &mut Self {
        TransparentWrapper::wrap_mut(v)
    }

    pub(crate) fn from_sexpr(e: &sexpression::Expression<'_>) -> Result<Self, AppError> {
        Ok(match e {
            sexpression::Expression::Number(f) => {
                if f.fract() == 0.0 {
                    Self::from(*f as i64)
                } else {
                    Self::from(*f)
                }
            }
            sexpression::Expression::Bool(b) => Self::from(*b),
            sexpression::Expression::Str(s) => Self::from(*s),
            sexpression::Expression::Symbol(s) if s.len() >= 4 => {
                // rust-like suffixes like 137u32 or 3.14f32
                match (&s[..s.len() - 3], &s[s.len() - 3..]) {
                    (v, "u32") => {
                        Self::from(v.parse::<u32>().whatever_context("invalid u32 value")?)
                    }
                    (v, "u64") => {
                        Self::from(v.parse::<u64>().whatever_context("invalid u64 value")?)
                    }
                    (v, "i32") => {
                        Self::from(v.parse::<i32>().whatever_context("invalid i32 value")?)
                    }
                    (v, "i64") => {
                        Self::from(v.parse::<i64>().whatever_context("invalid i64 value")?)
                    }
                    (v, "f32") => {
                        Self::from(v.parse::<f32>().whatever_context("invalid f32 value")?)
                    }
                    (v, "f64") => {
                        Self::from(v.parse::<f32>().whatever_context("invalid f64 value")?)
                    }
                    _ => {
                        return error::BadRequest {
                            e: "cannot parse value".to_string(),
                        }
                        .fail();
                    }
                }
            }
            _ => {
                return error::BadRequest {
                    e: "invalid value expression",
                }
                .fail();
            }
        })
    }

    pub(crate) fn extract_field<'a>(&'a self, path: &str) -> Option<&'a Self> {
        // path looks like .foo.bar.baz, split it by ".", skip 1st part
        // and incrementally dig into the value, expecting that .foo and .foo.bar are objects
        // note that document may not contain fields referred by query, that is normal
        let mut tail = self;
        for field in path.split(".").skip(1) {
            if let Some(v) = tail.extract_field_entry(field) {
                tail = v;
            } else {
                return None;
            }
        }
        Some(tail)
    }

    fn extract_field_entry<'a>(&'a self, entry: &str) -> Option<&'a Self> {
        if let rmpv::Value::Map(items) = &self.0 {
            for (k, v) in items {
                if let Some(s) = k.as_str()
                    && s == entry
                {
                    return Some(Self::from_ref(v));
                }
            }
        }
        None
    }

    /// update_field updates fill in place or replaces it.
    /// if update returns Some(Value), the field will be replaced with that value
    pub(crate) fn update_field(
        &mut self,
        path: &str,
        update: impl FnOnce(Option<&mut Self>) -> Result<Option<Self>, AppError>,
    ) -> Result<(), AppError> {
        let mut tail = &mut self.0;
        let mut path_parts = path.split(".").skip(1).peekable();
        while let Some(field) = path_parts.next() {
            let last_part = path_parts.peek().is_none();
            if let rmpv::Value::Map(items) = tail {
                let pos = items
                    .iter()
                    .position(|(fname, _)| fname.as_str() == Some(field));
                match pos {
                    Some(v) => {
                        if last_part {
                            if let Some(updated) = update(Some(Self::from_mut(&mut items[v].1)))? {
                                items[v].1 = updated.0;
                            }
                            return Ok(());
                        } else {
                            tail = &mut items[v].1;
                        }
                    }
                    None => {
                        if last_part {
                            if let Some(v) = update(None)? {
                                items.push((field.into(), v.0));
                            }
                            break;
                        } else {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn delete_field(&mut self, path: &str) -> Result<(), AppError> {
        let mut tail = &mut self.0;
        let mut path_parts = path.split(".").skip(1).peekable();
        while let Some(field) = path_parts.next() {
            let last_part = path_parts.peek().is_none();
            if let rmpv::Value::Map(items) = tail {
                let pos = items
                    .iter()
                    .position(|(fname, _)| fname.as_str() == Some(field));
                match pos {
                    Some(v) => {
                        if last_part {
                            items.remove(v);
                            return Ok(());
                        }
                        tail = &mut items[v].1;
                    }
                    None => return Ok(()),
                }
            }
        }
        Ok(())
    }
}

impl<T> From<T> for Value
where
    T: Into<rmpv::Value>,
{
    fn from(v: T) -> Self {
        Self(v.into())
    }
}

impl AsRef<rmpv::Value> for Value {
    fn as_ref(&self) -> &rmpv::Value {
        &self.0
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (rmpv::Value::Integer(a), rmpv::Value::Integer(b)) => match (a.as_u64(), b.as_u64()) {
                (Some(a), Some(b)) => Some(a.eq(&b)),
                _ => None,
            }
            .or(match (a.as_i64(), b.as_i64()) {
                (Some(a), Some(b)) => Some(a.eq(&b)),
                _ => None,
            })
            .unwrap_or(false),
            (rmpv::Value::F32(a), rmpv::Value::Integer(b)) => eq_f64(*a as f64, b),
            (rmpv::Value::F64(a), rmpv::Value::Integer(b)) => eq_f64(*a, b),
            (rmpv::Value::Integer(a), rmpv::Value::F32(b)) => eq_f64(*b as f64, a),
            (rmpv::Value::Integer(a), rmpv::Value::F64(b)) => eq_f64(*b, a),
            (a, b) => a.eq(b),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (&self.0, &other.0) {
            (rmpv::Value::Integer(a), rmpv::Value::Integer(b)) => match (a.as_u64(), b.as_u64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => None,
            }
            .or(match (a.as_i64(), b.as_i64()) {
                (Some(a), Some(b)) => a.partial_cmp(&b),
                _ => None,
            }),
            (rmpv::Value::F32(a), rmpv::Value::F32(b)) => a.partial_cmp(b),
            (rmpv::Value::F64(a), rmpv::Value::F64(b)) => a.partial_cmp(b),
            (rmpv::Value::F64(a), rmpv::Value::F32(b)) => a.partial_cmp(&(*b as f64)),
            (rmpv::Value::F32(a), rmpv::Value::F64(b)) => (*a as f64).partial_cmp(b),
            (rmpv::Value::String(a), rmpv::Value::String(b)) => a.as_str().partial_cmp(&b.as_str()),
            (rmpv::Value::F32(a), rmpv::Value::Integer(b)) => cmp_f64(*a as f64, b),
            (rmpv::Value::F64(a), rmpv::Value::Integer(b)) => cmp_f64(*a, b),
            (rmpv::Value::Integer(a), rmpv::Value::F32(b)) => cmp_f64(*b as f64, a),
            (rmpv::Value::Integer(a), rmpv::Value::F64(b)) => cmp_f64(*b, a),
            (_, _) => None,
        }
    }
}

impl Add for Value {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let sum = match (self.0, rhs.0) {
            (rmpv::Value::Integer(a), rmpv::Value::Integer(b)) => {
                if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                    rmpv::Value::from(a + b)
                } else if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
                    rmpv::Value::from(a + b)
                } else {
                    rmpv::Value::Integer(a)
                }
            }
            (rmpv::Value::String(a), rmpv::Value::String(b)) => match (a.as_str(), b.as_str()) {
                (Some(a), Some(b)) => {
                    let mut s = String::from(a);
                    s.push_str(b);
                    rmpv::Value::from(s)
                }
                _ => rmpv::Value::String(a),
            },
            (rmpv::Value::F32(a), rmpv::Value::F32(b)) => rmpv::Value::from(a + b),
            (rmpv::Value::F64(a), rmpv::Value::F64(b)) => rmpv::Value::from(a + b),
            (rmpv::Value::F64(a), rmpv::Value::F32(b)) => rmpv::Value::from(a + b as f64),
            (rmpv::Value::F32(a), rmpv::Value::F64(b)) => rmpv::Value::from(a as f64 + b),
            (rmpv::Value::F32(a), rmpv::Value::Integer(b)) => {
                rmpv::Value::from(a as f64 + b.as_f64().unwrap_or(0.0))
            }
            (rmpv::Value::F64(a), rmpv::Value::Integer(b)) => {
                rmpv::Value::from(a + b.as_f64().unwrap_or(0.0))
            }
            (rmpv::Value::Integer(a), rmpv::Value::F32(b)) => {
                rmpv::Value::from(a.as_f64().unwrap_or(0.0) + b as f64)
            }
            (rmpv::Value::Integer(a), rmpv::Value::F64(b)) => {
                rmpv::Value::from(a.as_f64().unwrap_or(0.0) + b)
            }
            (v, _) => v,
        };
        sum.into()
    }
}

impl AddAssign for Value {
    fn add_assign(&mut self, rhs: Self) {
        let sum = match (&mut self.0, rhs.0) {
            (rmpv::Value::Integer(a), rmpv::Value::Integer(b)) => {
                if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
                    rmpv::Value::from(a + b)
                } else if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
                    rmpv::Value::from(a + b)
                } else {
                    rmpv::Value::Integer(*a)
                }
            }
            (rmpv::Value::String(a), rmpv::Value::String(b)) => match (a.as_str(), b.as_str()) {
                (Some(a), Some(b)) => {
                    let mut s = String::from(a);
                    s.push_str(b);
                    rmpv::Value::from(s)
                }
                _ => rmpv::Value::String(a.clone()),
            },
            (rmpv::Value::F32(a), rmpv::Value::F32(b)) => rmpv::Value::from(*a + b),
            (rmpv::Value::F64(a), rmpv::Value::F64(b)) => rmpv::Value::from(*a + b),
            (rmpv::Value::F64(a), rmpv::Value::F32(b)) => rmpv::Value::from(*a + b as f64),
            (rmpv::Value::F32(a), rmpv::Value::F64(b)) => rmpv::Value::from(*a as f64 + b),
            (rmpv::Value::F32(a), rmpv::Value::Integer(b)) => {
                rmpv::Value::from(*a as f64 + b.as_f64().unwrap_or(0.0))
            }
            (rmpv::Value::F64(a), rmpv::Value::Integer(b)) => {
                rmpv::Value::from(*a + b.as_f64().unwrap_or(0.0))
            }
            (rmpv::Value::Integer(a), rmpv::Value::F32(b)) => {
                rmpv::Value::from(a.as_f64().unwrap_or(0.0) + b as f64)
            }
            (rmpv::Value::Integer(a), rmpv::Value::F64(b)) => {
                rmpv::Value::from(a.as_f64().unwrap_or(0.0) + b)
            }
            (v, _) => v.clone(),
        };
        *self = Self::from(sum)
    }
}

impl foundationdb::tuple::TuplePack for Value {
    fn pack<W: std::io::Write>(
        &self,
        w: &mut W,
        tuple_depth: foundationdb::tuple::TupleDepth,
    ) -> std::io::Result<foundationdb::tuple::VersionstampOffset> {
        match &self.0 {
            rmpv::Value::Boolean(b) => b.pack(w, tuple_depth),
            rmpv::Value::Integer(n) => {
                if let Some(n) = n.as_u64() {
                    n.pack(w, tuple_depth)
                } else {
                    n.as_i64()
                        .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidData))?
                        .pack(w, tuple_depth)
                }
            }
            rmpv::Value::F32(f) => f.pack(w, tuple_depth),
            rmpv::Value::F64(f) => f.pack(w, tuple_depth),
            rmpv::Value::String(s) => s
                .as_str()
                .ok_or(std::io::Error::from(std::io::ErrorKind::InvalidInput))?
                .pack(w, tuple_depth),
            _ => Err(std::io::Error::from(std::io::ErrorKind::Unsupported)),
        }
    }
}

// todo: put into From<serde_json::Value> for Value?
pub(crate) fn json2mp(v: serde_json::Value) -> rmpv::Value {
    match v {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(b),
        serde_json::Value::Number(n) => j_number2mp(n),
        serde_json::Value::String(s) => j_string2mp(s),
        serde_json::Value::Array(arr) => rmpv::Value::Array(arr.into_iter().map(json2mp).collect()),
        serde_json::Value::Object(map) => rmpv::Value::Map(
            map.into_iter()
                .map(|(k, obj)| (rmpv::Value::String(rmpv::Utf8String::from(k)), json2mp(obj)))
                .collect(),
        ),
    }
}

// todo: put into Into<serde_json::Value> for Value?
pub(crate) fn mp2json(v: rmpv::Value) -> serde_json::Value {
    match v {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(b),
        rmpv::Value::Integer(n) => mp_int2json(n),
        rmpv::Value::F32(f) => serde_json::Value::from(f),
        rmpv::Value::F64(f) => serde_json::Value::from(f),
        rmpv::Value::String(s) => {
            serde_json::Value::String(s.into_str().unwrap_or(String::from("")))
        }
        rmpv::Value::Binary(items) => serde_json::Value::String(BASE64_STANDARD.encode(items)),
        rmpv::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(mp2json).collect())
        }
        rmpv::Value::Map(items) => mp_map2json(items),
        rmpv::Value::Ext(_, _) => serde_json::Value::Null, // not supported
    }
}

fn mp_map2json(items: Vec<(rmpv::Value, rmpv::Value)>) -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::from_iter(
        items.into_iter().filter(|(k, _)| k.is_str()).map(|(k, v)| {
            (
                if let rmpv::Value::String(k) = k {
                    k.into_str().unwrap_or(String::from(""))
                } else {
                    String::from("")
                },
                mp2json(v),
            )
        }),
    ))
}

fn j_string2mp(s: String) -> rmpv::Value {
    // TODO: some sort of base64 encoded binary data support?
    rmpv::Value::String(rmpv::Utf8String::from(s))
}

fn j_number2mp(v: serde_json::Number) -> rmpv::Value {
    if let Some(n) = v.as_i64() {
        rmpv::Value::from(n)
    } else if let Some(n) = v.as_u64() {
        rmpv::Value::from(n)
    } else if let Some(n) = v.as_f64() {
        rmpv::Value::from(n)
    } else {
        rmpv::Value::from(0)
    }
}

fn mp_int2json(v: rmpv::Integer) -> serde_json::Value {
    if let Some(n) = v.as_i64() {
        serde_json::Value::from(n)
    } else if let Some(n) = v.as_u64() {
        serde_json::Value::from(n)
    } else if let Some(n) = v.as_f64() {
        serde_json::Value::from(n)
    } else {
        serde_json::Value::from(0)
    }
}

fn eq_f64(f: f64, i: &rmpv::Integer) -> bool {
    matches!(cmp_f64(f, i), Some(Ordering::Equal))
}

fn cmp_f64(f: f64, i: &rmpv::Integer) -> Option<Ordering> {
    let i = if let Some(i) = i.as_i64() {
        i as i128
    } else if let Some(i) = i.as_u64() {
        i as i128
    } else {
        return None;
    };
    if f.is_nan() {
        return None;
    }
    if f.is_infinite() {
        return Some(if f.is_sign_positive() {
            Ordering::Greater
        } else {
            Ordering::Less
        });
    }
    match (f as i128).cmp(&i) {
        Ordering::Less => Some(Ordering::Less),
        Ordering::Greater => Some(Ordering::Greater),
        Ordering::Equal => {
            let fr = f.fract();
            if fr < 0.0 {
                Some(Ordering::Less)
            } else if fr > 0.0 {
                Some(Ordering::Greater)
            } else {
                Some(Ordering::Equal)
            }
        }
    }
}
