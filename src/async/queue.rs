use std::sync::{Arc, Mutex};
use std::sync::mpsc;

use futures::{Async, Poll};
use futures::stream::Stream;
use futures::task::{self, Task};

pub fn channel<T, E>() -> (Sender<T, E>, Receiver<T, E>) {
    let (tx, rx) = mpsc::channel();
    let task = Arc::new(Mutex::new(None));
    let tx = Sender {
        task: task.clone(),
        tx: tx,
    };
    let rx = Receiver {
        task: task,
        rx: rx,
    };

    (tx, rx)
}

pub struct Sender<T, E> {
    task: Arc<Mutex<Option<Task>>>,
    tx: mpsc::Sender<Result<T, E>>,
}

impl<T, E> Clone for Sender<T, E> {
    fn clone(&self) -> Self {
        Sender {
            task: self.task.clone(),
            tx: self.tx.clone(),
        }
    }
}

#[derive(Debug)]
pub struct SendError<T, E>(pub Result<T, E>);

impl<T, E> Sender<T, E> {
    fn notify(&self) {
        let mut task = self.task.lock().unwrap();
        if let Some(task) = task.take() {
            task.unpark();
        }
    }

    pub fn send(&self, t: Result<T, E>) -> Result<(), SendError<T, E>> {
        if let Err(mpsc::SendError(t)) = self.tx.send(t) {
            Err(SendError(t))
        } else {
            Ok(self.notify())
        }
    }
}

impl<T, E> Drop for Sender<T, E> {
    fn drop(&mut self) {
        self.notify()
    }
}

#[must_use = "streams do nothing unless polled"]
pub struct Receiver<T, E> {
    task: Arc<Mutex<Option<Task>>>,
    rx: mpsc::Receiver<Result<T, E>>,
}

impl<T, E> Stream for Receiver<T, E> {
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<Option<T>, E> {
        use std::sync::mpsc::TryRecvError::*;

        let mut task = self.task.lock().unwrap();

        match self.rx.try_recv() {
            Ok(Ok(t)) => Ok(Async::Ready(Some(t))),
            Err(Disconnected) => Ok(Async::Ready(None)),
            Ok(Err(t)) => Err(t),
            Err(Empty) => {
                if task.is_none() {
                    *task = Some(task::park());
                }

                Ok(Async::NotReady)
            }
        }
    }
}

impl<T, E> Drop for Receiver<T, E> {
    fn drop(&mut self) {
        if let Ok(ref mut task) = self.task.lock() {
            drop(task.take())
        }
    }
}
