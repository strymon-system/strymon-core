use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::cell::RefCell;

use messaging::Transfer;

pub mod handler;
pub mod handshake;

pub trait Request: Transfer {
    type Success: Transfer;
    type Error: Transfer;
}

#[must_use]
pub struct Complete<R: Request> {
    tx: Sender<Result<R::Success, R::Error>>,
}

impl<R: Request> Complete<R> {
    fn new(tx: Sender<Result<R::Success, R::Error>>) -> Self {
        Complete { tx: tx }
    }

    pub fn success(self, s: R::Success) {
        self.result(Ok(s))
    }

    pub fn failed(self, e: R::Error) {
        self.result(Err(e))
    }

    pub fn result(self, result: Result<R::Success, R::Error>) {
        let _ = self.tx.send(result);
    }
}

#[must_use]
pub struct AsyncResult<T, E> {
    rx: Receiver<Result<T, E>>,
    buf: RefCell<Option<Result<T, E>>>,
}

impl<T, E> AsyncResult<T, E> {
    fn new(rx: Receiver<Result<T, E>>) -> Self {
        AsyncResult {
            rx: rx,
            buf: RefCell::new(None),
        }
    }

    fn try_recv(&self) -> Result<(), TryRecvError> {
        let mut buf = self.buf.borrow_mut();
        if buf.is_none() {
            match self.rx.try_recv() {
                Ok(result) => {
                    *buf = Some(result);
                    Ok(())
                }
                Err(err) => Err(err),
            }
        } else {
            Ok(())
        }
    }

    pub fn ready(&self) -> bool {
        if let Err(TryRecvError::Empty) = self.try_recv() {
            false
        } else {
            true
        }
    }

    pub fn canceled(&self) -> bool {
        if let Err(TryRecvError::Disconnected) = self.try_recv() {
            true
        } else {
            false
        }
    }

    pub fn await(mut self) -> Result<T, E> {
        let buf = self.buf.get_mut();
        if buf.is_none() {
            self.rx.recv().expect("computation was canceled!")
        } else {
            buf.take().unwrap()
        }
    }
}

pub fn promise<R: Request>() -> (Complete<R>, AsyncResult<R::Success, R::Error>) {
    let (tx, rx) = channel();
    (Complete::new(tx), AsyncResult::new(rx))
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Request for () {
        type Success = i32;
        type Error = bool;
    }

    #[test]
    fn promise_success() {
        let (tx, rx) = promise::<()>();
        tx.success(1);
        assert!(rx.ready());
        assert!(!rx.canceled());
        assert_eq!(rx.await(), Ok(1));
    }

    #[test]
    fn promise_failure() {
        let (tx, rx) = promise::<()>();
        assert!(!rx.ready());
        tx.failed(true);
        assert_eq!(rx.await(), Err(true));
    }

    #[test]
    fn promise_ready() {
        let (tx, rx) = promise::<()>();
        assert!(!rx.ready());
    }

    #[test]
    fn promise_canceled() {
        let (tx, rx) = promise::<()>();
        drop(tx);
        assert!(rx.canceled());
        assert!(rx.ready());
    }

    #[test]
    fn promise_not_canceled() {
        let (tx, rx) = promise::<()>();
        assert!(!rx.canceled());
        assert!(!rx.ready());
        drop(tx);
        assert!(rx.canceled());
    }
}
