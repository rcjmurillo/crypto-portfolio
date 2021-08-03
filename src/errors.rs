use std::fmt;

use reqwest;
use serde_json;
use toml;

#[derive(Debug)]
pub struct Error {
    reason: String,
}

impl Error {
    pub fn new(reason: String) -> Self {
        Self { reason }
    }

    pub fn from_string(reason: String) -> Self {
        Self { reason }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.reason)
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

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self {reason: err.to_string()}
    }
}

impl From<toml::de::Error> for Error {
    fn from(err: toml::de::Error) -> Self {
        Self {reason: err.to_string()}
    }
}

impl From<&str> for Error {
    fn from(err: &str) -> Self {
        Self {reason: err.to_string()}
    }
}