
#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    Bincode(Box<bincode::ErrorKind>),
    // WriteFileFail(usize, usize),
    SendError,
    ReadPastEnd,
    WebSocket(hyper_tungstenite::tungstenite::Error),
    JSON(serde_json::Error),
    YAML(serde_yaml::Error),
    ShardIndexDesync(u64, u64),
    OneShotRecv,
    JournalHeaderIdError(PathBuf, u32, u32),
    Configuration(confy::ConfyError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self))
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err)
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        Error::Bincode(err)
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Error::SendError
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for Error {
    fn from(_: tokio::sync::oneshot::error::RecvError) -> Self {
        Error::OneShotRecv
    }
}

impl From<hyper_tungstenite::tungstenite::Error> for Error {
    fn from(err: hyper_tungstenite::tungstenite::Error) -> Self {
        Error::WebSocket(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::JSON(err)
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(err: serde_yaml::Error) -> Self {
        Error::YAML(err)
    }
}

impl From<confy::ConfyError> for Error {
    fn from(err: confy::ConfyError) -> Self {
        Error::Configuration(err)
    }
}

// impl ToString for Error {
//     fn to_string(&self) -> String {
//         format!("{self:?}")
//     }
// }

use std::path::PathBuf;