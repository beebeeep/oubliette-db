use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use snafu::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum AppError {
    #[snafu(display("foundationDB error: {e}"), context(suffix(false)))]
    Fdb {
        e: String,
        source: foundationdb::FdbError,
    },

    #[snafu(display("commit error"), context(suffix(false)))]
    FdbTransactionCommit {
        source: foundationdb::TransactionCommitError,
    },

    #[snafu(whatever, display("{message}"))]
    Generic {
        message: String,
        #[snafu(source(from(Box<dyn std::error::Error>, Some)))]
        source: Option<Box<dyn std::error::Error>>,
    },
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::Fdb { e, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}: {source}")})),
            )
                .into_response(),
            AppError::FdbTransactionCommit { source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json! ({"error": format!("transaction commit failed: {source}")})),
            )
                .into_response(),
            AppError::Generic { message, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":
                    match source {
                        Some(e) => format!("{message}: {e}"),
                        None =>    format!("{message}"),
                }})),
            )
                .into_response(),
        }
    }
}
