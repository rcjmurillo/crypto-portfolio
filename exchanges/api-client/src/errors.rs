use std::fmt::{Debug, Display, Formatter, Result};
use std::error::Error as StdError;

#[derive(Debug)]
pub enum Error {
    Internal,
    ServiceUnavailable,
    Unauthorized,
    BadRequest { body: String },
    NotFound,
    Other { status: u16 },
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{:?}", self)
    }
}