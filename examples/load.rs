use std::{env::args, fs::File};

use oubliette_db::{error, http_api};
use reqwest::StatusCode;
use serde_json::Value;
use snafu::{ResultExt, Whatever};

fn main() -> Result<(), Whatever> {
    let fname = args().skip(1).next().unwrap();
    let f = File::open(fname).whatever_context("opening file")?;
    let docs = serde_json::Deserializer::from_reader(f).into_iter::<Value>();
    let mut req = http_api::SetRequest {
        docs: Vec::with_capacity(200),
    };
    let client = reqwest::blocking::Client::new();
    let mut count = 0usize;
    for doc in docs {
        count += 1;
        let doc = doc.whatever_context("deserializing value")?;
        req.docs.push(doc);
        if req.docs.len() == req.docs.capacity() {
            match client
                .put("http://localhost:4800/testdb/nyc-taxi")
                .json(&req)
                .send()
            {
                Ok(r) => {
                    if r.status() != StatusCode::CREATED {
                        let resp = r.bytes().unwrap();
                        eprintln!("request failed:{resp:?}");
                    }
                }
                Err(e) => eprintln!("request failed: {e:?}"),
            }
            req.docs.truncate(0);
            println!("ingested {count}");
        }
    }

    Ok(())
}
