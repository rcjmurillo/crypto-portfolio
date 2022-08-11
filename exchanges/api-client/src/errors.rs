use std::fmt::{Debug, Display, Formatter, Result};
use thiserror::Error;

#[derive(Debug)]
pub enum Error {
    Internal,
    ServiceUnavailable,
    Unauthorized,
    BadRequest { body: String },
    NotFound,
    Other { status: u16 },
    TooManyRequests { retry_after: usize },
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{:?}", self)
    }
}
