use std::fmt::Debug;
use thiserror::Error;
use tower::BoxError;
use anyhow::anyhow;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("API error: {0}")]
    ApiError(ApiError),
    #[error("{0}")]
    Other(anyhow::Error),
}

impl From<BoxError> for ClientError {
    fn from(value: BoxError) -> Self {
        match value.downcast::<Self>() {
            Ok(e) => *e,
            Err(e) => Self::Other(anyhow!("unexpected error: {}", e)),
        }
    }
}

impl From<ApiError> for ClientError {
    fn from(value: ApiError) -> Self {
        Self::ApiError(value)
    }
}

impl From<anyhow::Error> for ClientError {
    fn from(value: anyhow::Error) -> Self {
        match value.downcast::<ApiError>() {
            Ok(e) => Self::ApiError(e),
            Err(e) => Self::Other(e),
        }
    }
}

#[derive(Error, Debug)]
pub enum ApiError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("service unavailable")]
    ServiceUnavailable,
    #[error("unauthorized")]
    Unauthorized,
    #[error("bad request, response: {body}")]
    BadRequest { body: String },
    #[error("not found")]
    NotFound,
    #[error("other: {status}")]
    Other { status: u16 },
    #[error("too many requests, retry after {retry_after}")]
    TooManyRequests { retry_after: usize },
}

// impl From<BoxError> for ApiError {
//     fn from(value: BoxError) -> Self {
//         match value.downcast::<Self>() {
//             Ok(e) => *e,
//             Err(e) => Self::Internal(format!("couldn't downcast from error: {}", e)),
//         }
//     }
// }
