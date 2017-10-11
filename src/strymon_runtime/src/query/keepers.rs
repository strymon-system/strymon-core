use std::io::Error as IoError;
use std::net::ToSocketAddrs;

use futures::Future;

use coordinator::requests::*;
use query::Coordinator;

#[derive(Debug)]
pub enum KeeperWorkerRegistrationError {
    KeeperWorkerAlreadyExists,
    SocketAddrsNotValid,
    IoError(IoError),
}

impl From<AddKeeperWorkerError> for KeeperWorkerRegistrationError {
    fn from(err: AddKeeperWorkerError) -> Self {
        match err {
            AddKeeperWorkerError::WorkerAlreadyExists => {
                KeeperWorkerRegistrationError::KeeperWorkerAlreadyExists
            }
        }
    }
}

impl From<IoError> for KeeperWorkerRegistrationError {
    fn from(err: IoError) -> Self {
        KeeperWorkerRegistrationError::IoError(err)
    }
}

impl<T, E> From<Result<T, E>> for KeeperWorkerRegistrationError
    where T: Into<KeeperWorkerRegistrationError>,
          E: Into<KeeperWorkerRegistrationError>
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
    KeeperHasNoWorkers,
    IoError(IoError),
}

impl From<GetKeeperAddressError> for KeeperLookupError {
    fn from(err: GetKeeperAddressError) -> Self {
        match err {
            GetKeeperAddressError::KeeperNotFound => KeeperLookupError::KeeperNotFound,
            GetKeeperAddressError::KeeperHasNoWorkers => {
                KeeperLookupError::KeeperHasNoWorkers
            }
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

#[derive(Debug)]
pub enum WorkerDeregistrationError {
    KeeperDoesntExist,
    WorkerDoesntExist,
    IoError(IoError),
}

impl From<RemoveKeeperWorkerError> for WorkerDeregistrationError {
    fn from(err: RemoveKeeperWorkerError) -> Self {
        match err {
            RemoveKeeperWorkerError::KeeperDoesntExist => {
                WorkerDeregistrationError::KeeperDoesntExist
            }
            RemoveKeeperWorkerError::KeeperWorkerDoesntExist => {
                WorkerDeregistrationError::WorkerDoesntExist
            }
        }
    }
}

impl From<IoError> for WorkerDeregistrationError {
    fn from(err: IoError) -> Self {
        WorkerDeregistrationError::IoError(err)
    }
}

impl<T, E> From<Result<T, E>> for WorkerDeregistrationError
    where T: Into<WorkerDeregistrationError>,
          E: Into<WorkerDeregistrationError>
{
    fn from(err: Result<T, E>) -> Self {
        match err {
            Ok(err) => err.into(),
            Err(err) => err.into(),
        }
    }
}

impl Coordinator {
    /// If addr.to_socket_addrs() returns more than one address, then the first one is used.
    pub fn add_keeper_worker<A: ToSocketAddrs>
        (&self,
         name: &str,
         worker_num: usize,
         addr: A)
         -> Result<(), KeeperWorkerRegistrationError> {
        let addr = match addr.to_socket_addrs()?.next() {
            Some(addr) => addr,
            None => return Err(KeeperWorkerRegistrationError::SocketAddrsNotValid),
        };
        let addr = (addr.ip().to_string(), addr.port());
        self.tx
            .request(&AddKeeperWorker {
                          name: name.to_string(),
                          worker_num: worker_num,
                          addr: addr,
                      })
            .map_err(KeeperWorkerRegistrationError::from)
            .wait()
    }

    /// Returns the address of the requested Keeper.
    pub fn get_keeper_address(&self,
                              name: &str)
                              -> Result<(String, u16), KeeperLookupError> {
        self.tx
            .request(&GetKeeperAddress { name: name.to_string() })
            .map_err(KeeperLookupError::from)
            .wait()
    }

    /// To be called when unregistering.
    pub fn remove_keeper_worker(&self,
                                name: &str,
                                worker_num: usize)
                                -> Result<(), WorkerDeregistrationError> {
        self.tx
            .request(&RemoveKeeperWorker {
                          name: name.to_string(),
                          worker_num: worker_num,
                      })
            .map_err(WorkerDeregistrationError::from)
            .wait()
    }
}
