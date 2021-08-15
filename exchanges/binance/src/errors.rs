use std::fmt;

use api_client::errors::ErrorKind as ApiClientErrorKind;

use serde::Deserialize;
use serde_json;

#[derive(Debug)]
pub enum ErrorKind {
    ApiClient(ApiClientErrorKind),
    Api(ApiErrorKind),
    Parse,
    Other,
}

#[derive(Debug)]
pub enum ApiErrorKind {
    UnavailableSymbol,
    // add any other relevant error codes here
    Other,
}

impl From<Option<i16>> for ApiErrorKind {
    fn from(code: Option<i16>) -> Self {
        match code {
            Some(-11001) => ApiErrorKind::UnavailableSymbol,
            _ => ApiErrorKind::Other,
        }
    }
}

pub struct Error {
    pub reason: String,
    pub kind: ErrorKind,
}

impl Error {
    pub(crate) fn new(reason: String, kind: ErrorKind) -> Self {
        Self { reason, kind }
    }
}

impl From<api_client::errors::Error> for Error {
    fn from(err: api_client::errors::Error) -> Self {
        match err.kind {
            ApiClientErrorKind::BadRequest => {
                #[derive(Deserialize)]
                struct ErrorResponse {
                    code: Option<i16>,
                    msg: String,
                }

                match serde_json::from_slice::<ErrorResponse>(err.body.as_ref()) {
                    Ok(ErrorResponse { code, msg }) => Self {
                        reason: msg,
                        kind: ErrorKind::Api(code.into()),
                    },
                    Err(err) => Error::new(
                        format!("could not parse error response: {}", err),
                        ErrorKind::Parse,
                    ),
                }
            }
            _ => {
                let kind = match err.kind {
                    err @ ApiClientErrorKind::Internal
                    | err @ ApiClientErrorKind::ServiceUnavailable
                    | err @ ApiClientErrorKind::Unauthorized
                    | err @ ApiClientErrorKind::BadRequest => ErrorKind::ApiClient(err),
                    ApiClientErrorKind::Other => ErrorKind::Other,
                };
                Self {
                    reason: err.body,
                    kind,
                }
            }
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.reason)
    }
}