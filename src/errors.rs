use std::fmt;

#[derive(Debug)]
pub enum ErrorKind {
    FetchFailed,
    Cli,
    Other
}

#[derive(Debug)]
pub struct Error {
    reason: String,
    kind: ErrorKind
}

impl Error {
    pub fn new(reason: String, kind: ErrorKind) -> Self {
        Self { reason, kind }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.reason)
    }
}

impl From<binance::Error> for Error {
    fn from(err: binance::Error) -> Self {
        match err.kind {
            binance::ErrorKind::Other => Error::new(err.reason, ErrorKind::Other),
            _ => Error::new(format!("({}): {:?}", err, err.kind), ErrorKind::FetchFailed),
        }
    }
}