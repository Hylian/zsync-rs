use snafu::{ensure, Backtrace, ErrorCompat, ResultExt, Snafu};
//use crate::rcksum::types::*;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("DataWindow out of bounds: pos {} > limit {}", position, limit))]
    DataOutOfBounds {
        position: usize,
        limit: usize,
    },

    #[snafu(display("I/O error: {:#?}", error))]
    Io {
        error: std::io::Error,
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io { error }
    }
}
