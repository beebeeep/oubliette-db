use base64::prelude::*;

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
