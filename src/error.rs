use thiserror::Error;
use tokio::io;

#[derive(Error, Debug)]
pub enum Grib1Error {
    #[error("IO Error")]
    IoError(#[from] io::Error),

    #[error("Wrong Grib1 header")]
    WrongHeader,

    #[error("Wrong Grib version. Only version 1 is supported")]
    WrongVersion(u8),

    #[error("Tried to decode more that than we have")]
    DataDecodeFailed,
}
