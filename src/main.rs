use oubliette_db::error::AppError;
use oubliette_db::http_api;
use snafu::prelude::*;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    let layer = tracing_subscriber::fmt::layer().json().flatten_event(true);
    let filter = EnvFilter::builder()
        .parse(format!("info,oubliette_db={}", "debug"))
        .whatever_context("setting up logger")?;
    let subscriber = tracing_subscriber::registry().with(layer).with(filter);
    tracing::subscriber::set_global_default(subscriber).whatever_context("setting up logger")?;
    info!("starting up");

    http_api::start("./fdb.cluster").await?;

    Ok(())
}
