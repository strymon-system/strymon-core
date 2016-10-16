use futures::{self, Future, Poll, Async};
use std::sync::{Arc, Mutex};

pub fn promise<T, E>() -> (Complete<T, E>, Promise<T, E>) {
    let (tx, rx) = futures::oneshot();

    let rx = Promise {
        inner: rx
    };
    let tx = Complete {
        inner: Arc::new(Mutex::new(Some(tx))),
    };

    (tx, rx)
}

#[derive(Debug)]
pub struct Canceled;

pub struct Promise<T, E> {
    inner: futures::Oneshot<Result<T, E>>,
}

impl<T, E> Future for Promise<T, E> {
    type Item = T;
    type Error = Result<E, Canceled>;
    
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.poll() {
            Ok(Async::Ready(Ok(t))) => Ok(Async::Ready(t)),
            Ok(Async::Ready(Err(e))) => Err(Ok(e)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_) => Err(Err(Canceled)),
        }
    }
}

type SharedComplete<T, E> = Option<futures::Complete<Result<T, E>>>;

pub struct Complete<T, E> {
    inner: Arc<Mutex<SharedComplete<T, E>>>,
}

impl<T, E> Complete<T, E> {
    pub fn complete(self, res: Result<T, E>) {
        let mut inner = self.inner.lock().unwrap();
        let complete = inner.take().expect("tried to complete twice?!");
        complete.complete(res)
    }
    
    pub fn cancellation(&self) -> Cancellation 
        where T: 'static, E: 'static
    {
        Cancellation { inner: Box::new(self.inner.clone()) }
    }
}

impl<T, E> Drop for Complete<T, E> {
    fn drop(&mut self) {
        // drop inner, so any cancellation futures return as well
        if let Ok(mut complete) = self.inner.lock() {
            complete.take();
        }
    }
}

trait PollCancel {
    fn poll_cancel(&mut self) -> Poll<(), ()>;
}

impl<T, E> PollCancel for Arc<Mutex<SharedComplete<T, E>>> {
    fn poll_cancel(&mut self) -> Poll<(), ()> {
        let mut complete = self.lock().unwrap();
        if let Some(tx) = complete.as_mut() {
            // Ok(Ready::Async(())) means that Oneshot was dropped
            tx.poll_cancel()
        } else {
            // complete is no more, we have complemeted
            Err(())
        }
    }
}

pub struct Cancellation {
    inner: Box<PollCancel>,
}

// returns Ok(()) if Promise dropped, return Err(()) if completed successfully
impl Future for Cancellation {
    type Item = ();
    type Error = ();
    
    fn poll(&mut self) -> Poll<(), ()> {
        self.inner.poll_cancel()
    }
}
