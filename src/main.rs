use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize, Debug)]
struct T {
    num: i64,
    str: String,
    val: Value,
}

fn main() {
    let d = r#"
    {
        "num": 137,
        "str": "CHLOS",
        "val": {
            "foo": "bar",
            "baz": 166
        }
    }
    "#;
    let v: T = serde_json::from_str(d).unwrap();
    println!("{v:?}");
    let buf = Vec::new();
    let mut res = rmp_serde::Serializer::new(buf);
    serde_transcode::transcode(v.val, &mut res).unwrap();

    println!("{:?}", res.into_inner());
}
