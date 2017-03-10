use std::io::Error as IoError;

use futures::Future;

use coordinator::requests::*;
use model::Keeper;
use query::Coordinator;

#[derive(Debug)]
pub enum KeeperRegistrationError {
    KeeperAlreadyExists,
    IoError(IoError),
}

impl From<RegisterKeeperError> for KeeperRegistrationError {
    fn from(err: RegisterKeeperError) -> Self {
        match err {
            RegisterKeeperError::KeeperAlreadyExists => {
                KeeperRegistrationError::KeeperAlreadyExists
            }
        }
    }
}

impl From<IoError> for KeeperRegistrationError {
    fn from(err: IoError) -> Self {
        KeeperRegistrationError::IoError(err)
    }
}

impl<T, E> From<Result<T, E>> for KeeperRegistrationError
    where T: Into<KeeperRegistrationError>,
          E: Into<KeeperRegistrationError>
{
    fn from(err: Result<T, E>) -> Self {
        match err {
            Ok(err) => err.into(),
            Err(err) => err.into(),
        }
    }
}

#[derive(Debug)]
pub enum KeeperLookupError {
    KeeperNotFound,
    IoError(IoError),
}

impl From<LookupKeeperError> for KeeperLookupError {
    fn from(err: LookupKeeperError) -> Self {
        match err {
            LookupKeeperError::KeeperNotFound => KeeperLookupError::KeeperNotFound,
        }
    }
}

impl From<IoError> for KeeperLookupError {
    fn from(err: IoError) -> Self {
        KeeperLookupError::IoError(err)
    }
}

impl<T, E> From<Result<T, E>> for KeeperLookupError
    where T: Into<KeeperLookupError>,
          E: Into<KeeperLookupError>
{
    fn from(err: Result<T, E>) -> Self {
        match err {
            Ok(err) => err.into(),
            Err(err) => err.into(),
        }
    }
}

impl Coordinator {
    pub fn register_keeper(&self,
                           name: &str,
                           addr: (&str, u16))
                           -> Result<(), KeeperRegistrationError> {
        self.tx
            .request(&RegisterKeeper {
                          name: name.to_string(),
                          addr: (addr.0.to_string(), addr.1),
                      })
            .map_err(KeeperRegistrationError::from)
            .wait()
    }

    pub fn lookup_keeper(&self,
                         name: &str)
                         -> Result<Keeper, KeeperLookupError> {
        self.tx
            .request(&LookupKeeper { name: name.to_string() })
            .map_err(KeeperLookupError::from)
            .wait()
    }
}
