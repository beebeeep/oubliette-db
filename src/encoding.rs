pub(crate) fn json2mp(v: serde_json::Value) -> rmpv::Value {
    match v {
        serde_json::Value::Null => rmpv::Value::Nil,
        serde_json::Value::Bool(b) => rmpv::Value::Boolean(b),
        serde_json::Value::Number(n) => jNumber2mp(n),
        serde_json::Value::String(s) => jString2mp(s),
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
        rmpv::Value::Integer(n) => mpInt2json(n),
        rmpv::Value::F32(f) => serde_json::Value::from(f),
        rmpv::Value::F64(f) => serde_json::Value::from(f),
        rmpv::Value::String(s) => {
            serde_json::Value::String(s.into_str().unwrap_or(String::from("")))
        }
        rmpv::Value::Binary(items) => todo!(),
        rmpv::Value::Array(values) => todo!(),
        rmpv::Value::Map(items) => todo!(),
        rmpv::Value::Ext(_, items) => serde_json::Value::Null, // not supported
    }
}

fn jString2mp(s: &str) -> rmpv::Value {
    rmpv::Value::String(rmpv::Utf8String::from(s));
    todo!()
}
fn jNumber2mp(v: serde_json::Number) -> rmpv::Value {
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

fn mpInt2json(v: rmpv::Integer) -> serde_json::Value {
    todo!()
}
