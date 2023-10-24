use thiserror::Error;
use tokio::io;

#[derive(Error, Debug)]
/// List of errors the library can return when reading a GRIB file
pub enum Grib1Error {
    #[error("IO Error")]
    /// An IO error occured while handling the supplied file
    IoError(#[from] io::Error),

    #[error("Wrong Grib1 header")]
    /// The header didn't match the expected GRIB header
    WrongHeader,

    #[error("Wrong Grib version. Only version 1 is supported")]
    /// The contained version number didn't match 0x01
    WrongVersion(u8),

    #[error("Tried to decode more data than we have")]
    /// The bitstream representing the data didn't have the expected length
    DataDecodeFailed,
}
