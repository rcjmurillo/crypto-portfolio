use api_client::errors::{ApiError, ClientError};

use serde::Deserialize;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("unavailable symbol")]
    UnavailableSymbol,
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error("Other error: {0}")]
    Other(String),
}

impl From<Option<i16>> for Error {
    fn from(code: Option<i16>) -> Self {
        match code {
            Some(-11001) => Error::UnavailableSymbol,
            Some(-1021) => Error::InvalidTimestamp,
            Some(c) => Error::Other(format!("error with code {c}")),
            None => Error::Other("error response with no error code".to_string()),
        }
    }
}

impl From<&ClientError> for Error {
    fn from(api_error: &ClientError) -> Self {
        match api_error {
            ClientError::ApiError(ApiError::BadRequest { body }) => {
                #[derive(Deserialize)]
                struct ErrorResponse {
                    code: Option<i16>,
                }
                match serde_json::from_str::<ErrorResponse>(&body) {
                    Ok(ErrorResponse { code, .. }) => code.into(),
                    Err(_) => Error::Other(format!("couldn't parse error response: {}", body)),
                }
            }
            e => Error::Other(e.to_string()),
        }
    }
}

impl From<ClientError> for Error {
    fn from(api_error: ClientError) -> Self {
        Error::from(&api_error)
    }
}
