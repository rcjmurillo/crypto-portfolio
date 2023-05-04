use std::fmt;

#[derive(Debug)]
pub enum Error {
    FetchFailed,
    Cli,
    Other,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
