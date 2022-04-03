use std::fmt;

use api_client::errors::Error as ApiError;

use serde::Deserialize;

#[derive(Debug)]
pub enum Error {
    Api(ApiErrorKind),
    Other(String),
}

#[derive(Debug)]
pub enum ApiErrorKind {
    UnavailableSymbol,
    // add any other relevant error codes here
    Other(i16),
}

impl From<Option<i16>> for ApiErrorKind {
    fn from(code: Option<i16>) -> Self {
        match code {
            Some(-11001) => ApiErrorKind::UnavailableSymbol,
            Some(c) => ApiErrorKind::Other(c),
            _ => ApiErrorKind::Other(0),
        }
    }
}

impl From<ApiError> for Error {
    fn from(api_error: ApiError) -> Self {
        match api_error {
            ApiError::BadRequest { body } => {
                #[derive(Deserialize)]
                struct ErrorResponse {
                    code: Option<i16>,
                }
                match serde_json::from_str::<ErrorResponse>(&body) {
                    Ok(ErrorResponse { code, .. }) => Error::Api(code.into()),
                    Err(_) => Error::Other(format!("couldn't parse error response: {}", body)),
                }
            }
            e => Error::Other(e.to_string()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
