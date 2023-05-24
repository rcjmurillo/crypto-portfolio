use std::fmt;

#[derive(Debug)]
pub enum Error {
    Config,
    #[allow(dead_code)]
    FetchFailed,
    #[allow(dead_code)]
    Other,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
