use std::fmt::Debug;

#[derive(Debug)]
pub enum ErrorKind {
    Internal,
    ServiceUnavailable,
    Unauthorized,
    BadRequest,
    Other,
}

pub struct Error {
    pub body: String,
    pub kind: ErrorKind,
}

impl Error {
    pub(crate) fn new(body: String, kind: ErrorKind) -> Self {
        Self { body, kind }
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self {
            body: format!("error={}, url={:?} status={:?}", err, err.url(), err.status()),
            kind: ErrorKind::Other,
        }
    }
}
