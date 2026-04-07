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
    #[snafu(display("key decode error"), context(suffix(false)))]
    FdbTupleUnpack {
        source: foundationdb::tuple::PackError,
    },

    #[snafu(display("MessagePack encode error"), context(suffix(false)))]
    MPVEncode {
        e: String,
        source: rmpv::encode::Error,
    },

    #[snafu(display("MessagePack decode error"), context(suffix(false)))]
    MPVDecode {
        e: String,
        source: rmpv::decode::Error,
    },

    #[snafu(display("MessagePack encode error"), context(suffix(false)))]
    MPEncode {
        e: String,
        source: rmp_serde::encode::Error,
    },

    #[snafu(display("MessagePack decode error"), context(suffix(false)))]
    MPDecode {
        e: String,
        source: rmp_serde::decode::Error,
    },

    #[snafu(display("docID decoding error"), context(suffix(false)))]
    DocIDDecode { source: hex::FromHexError },

    #[snafu(display("request error: {e}"), context(suffix(false)))]
    BadRequest { e: String },

    #[snafu(display("validation error: {e}"), context(suffix(false)))]
    Validation { e: String },

    #[snafu(display("failed to parse query: {e}"), context(suffix(false)))]
    QueryParse {
        e: String,
        source: sexpression::ParseError,
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
            AppError::MPVEncode { e, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}: {source}")})),
            )
                .into_response(),
            AppError::MPVDecode { e, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}: {source}")})),
            )
                .into_response(),
            AppError::MPEncode { e, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}: {source}")})),
            )
                .into_response(),
            AppError::MPDecode { e, source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{e}: {source}")})),
            )
                .into_response(),
            AppError::DocIDDecode { source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": format!("{source}")})),
            )
                .into_response(),
            AppError::BadRequest { e } => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{e}")})),
            )
                .into_response(),
            AppError::Validation { e } => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{e}")})),
            )
                .into_response(),
            AppError::QueryParse { e, source } => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("{e}: {source}")})),
            )
                .into_response(),
            AppError::FdbTupleUnpack { source } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json! ({"error": format!("decoding key tuple: {source}")})),
            )
                .into_response(),
        }
    }
}
