use std::{
    env::args,
    fs::File,
    io::{self, BufReader, Write},
    time,
};

use oubliette_db::{error, http_api};
use reqwest::StatusCode;
use serde_json::Value;
use snafu::{ResultExt, Whatever};

#[tokio::main]
async fn main() -> Result<(), Whatever> {
    let fname = args().skip(1).next().unwrap();
    let f = BufReader::new(File::open(fname).whatever_context("opening file")?);
    let jdocs = serde_json::Deserializer::from_reader(f).into_iter::<Value>();
    let (tx, mut rx) = tokio::sync::mpsc::channel(32);
    let mut done = false;

    for _ in 0..32 {
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = upload(tx).await {
                eprintln!("task failed: {e}");
            }
        });
    }
    let start = time::Instant::now();
    let mut docs = jdocs.into_iter();
    let mut count = 0usize;
    loop {
        if done {
            break;
        }
        let mut buf = Vec::with_capacity(400);
        for _ in 0..buf.capacity() {
            if let Some(d) = docs.next() {
                buf.push(d.whatever_context("unmarshalling doc")?);
            } else {
                done = true;
                break;
            }
        }
        let batch_tx = rx.recv().await.unwrap();
        count += buf.len();
        if let Err(_) = batch_tx.send(buf) {
            eprintln!("failed to send")
        }

        print!(
            "loaded {count} docs at {:.1} rps\r",
            count as f64 / start.elapsed().as_secs_f64()
        );
        io::stdout().flush().unwrap();
    }
    Ok(())
}

async fn upload(
    ch: tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<Vec<Value>>>,
) -> Result<(), Whatever> {
    let client = reqwest::Client::new();
    loop {
        let (tx, rx) = tokio::sync::oneshot::channel();
        ch.send(tx).await.whatever_context("requesting batch")?;
        let docs = rx.await.whatever_context("getting batch")?;
        let req = http_api::SetRequest { docs };
        match client
            .put("http://localhost:4800/testdb/nyc-taxi")
            .json(&req)
            .send()
            .await
        {
            Ok(r) => {
                if r.status() != StatusCode::CREATED {
                    let resp = r.bytes().await.unwrap();
                    eprintln!("request failed:{resp:?}");
                }
            }
            Err(e) => eprintln!("request failed: {e:?}"),
        }
    }
}
