use futures::{self, Future, Poll, Async};
use std::sync::{Arc, Mutex};

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

pub struct Complete<T, E> {
    inner: Arc<Mutex<Option<futures::Complete<Result<T, E>>>>>,
}

impl<T, E> Complete<T, E> {

}

pub struct Cancellation {

}
