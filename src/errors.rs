use reqwest;
use serde_json;

#[derive(Debug)]
pub struct Error {
    reason: String,
}

impl Error {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self {reason: err.to_string()}
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self {reason: err.to_string()}
    }
}