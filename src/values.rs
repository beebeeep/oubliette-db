use std::cmp::Ordering;

use base64::prelude::*;
use bytemuck::TransparentWrapper;
use snafu::OptionExt;

use crate::error::AppError;

/// Value wraps rmpv::Value providing a bit more flexible comparison operations (can compare ints with floats and different floats with each other)
#[repr(transparent)]
#[derive(Clone, Debug, TransparentWrapper)]
pub(crate) struct Value(pub(crate) rmpv::Value);

impl Value {
    pub(crate) fn from_ref(v: &rmpv::Value) -> &Self {
        TransparentWrapper::wrap_ref(v)
    }
    pub(crate) fn truncate(&mut self, len: usize) -> Result<(), AppError> {
        if let rmpv::Value::String(us) = &self.0 {
            let mut s = us.as_str().whatever_context("non UTF-8 string")?;
            if len < s.len() {
                s = &s[..s.floor_char_boundary(len)];
                self.0 = rmpv::Value::from(s);
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
            (a, b) => a.eq(&b),
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
                    return n.as_i64().unwrap().pack(w, tuple_depth);
                }
            }
            rmpv::Value::F32(f) => f.pack(w, tuple_depth),
            rmpv::Value::F64(f) => f.pack(w, tuple_depth),
            rmpv::Value::String(s) => s.as_str().unwrap().pack(w, tuple_depth),
            _ => unimplemented!("data type not supported"),
        }
    }
}

pub(crate) fn extract_field<'a>(path: &str, value: &'a rmpv::Value) -> Option<&'a Value> {
    // path looks like .foo.bar.baz, split it by ".", skip 1st part
    // and incrementally dig into the value, expecting that .foo and .foo.bar are objects
    // note that document may not contain fields referred by query, that is normal
    let mut tail = value;
    for field in path.split(".").skip(1) {
        if let Some(v) = extract_field_entry(field, tail) {
            tail = v;
        } else {
            return None;
        }
    }
    Some(Value::from_ref(tail))
}

fn extract_field_entry<'a>(entry: &str, value: &'a rmpv::Value) -> Option<&'a rmpv::Value> {
    if let rmpv::Value::Map(items) = value {
        for (k, v) in items {
            if let Some(s) = k.as_str() {
                if s == entry {
                    return Some(v);
                }
            }
        }
    }
    None
}

// todo: put into From<serde_json::Value> for Value?
pub(crate) fn json2mp(v: serde_json::Value) -> rmpv::Value {
    match v {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(b),
        serde_json::Value::Number(n) => j_number2mp(n),
        serde_json::Value::String(s) => j_string2mp(s),
        serde_json::Value::Array(arr) => {
            rmpv::Value::Array(arr.into_iter().map(|e| json2mp(e)).collect())
        }
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
            serde_json::Value::Array(values.into_iter().map(|e| mp2json(e)).collect())
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
    match cmp_f64(f, i) {
        Some(Ordering::Equal) => true,
        _ => false,
    }
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
