use std::{fmt::{Debug, Display, Formatter, Result}, error::Error as StdError};

#[derive(Debug)]
pub enum ErrorKind {
    Internal,
    ServiceUnavailable,
    Unauthorized,
    BadRequest,
    NotFound,
    Other,
}

#[derive(Debug)]
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

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "body={} kind={:?}", self.body, self.kind)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}
