use std::io;

use futures::{self, Future, Complete, Oneshot, Poll};

pub fn pair<T, E>() -> (Promise<T, E>, Response<T, E>) {
    let (tx, rx) = futures::oneshot();
    let promise = Promise { inner: tx };
    let response = Response { inner: rx };

    (promise, response)
}

pub struct Promise<T, E> {
    inner: Complete<Result<T, E>>,
}

impl<T, E> Promise<T, E> {
    pub fn fullfil(self, t: T) {
        self.complete(Ok(t))
    }

    pub fn failed(self, e: E) {
        self.complete(Err(e))
    }

    pub fn complete(self, result: Result<T, E>) {
        self.inner.complete(result)
    }
}

pub struct Response<T, E> {
    inner: Oneshot<Result<T, E>>,
}

impl<T, E> Future for Response<T, E> {
    type Item = T;
    type Error = E;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.poll() {
            Poll::Ok(Ok(t)) => Poll::Ok(t),
            Poll::Ok(Err(e)) => Poll::Err(e),
            Poll::NotReady => Poll::NotReady,
            Poll::Err(_) => panic!("promise got canceled!"),
        }
    }
}
