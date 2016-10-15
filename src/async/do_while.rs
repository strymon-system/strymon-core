use futures::{Future, Poll, Async};
use futures::stream::{Stream, MapErr, ForEach};

#[must_use = "streams do nothing unless polled"]
pub struct DoWhile<S: Stream, F> {
    inner: ForEach<MapErr<S, fn(S::Error) -> Stop<S::Error>>, F>,
}

impl<S: Stream, F> Future for DoWhile<S, F>
    where F: FnMut(S::Item) -> Result<(), Stop<S::Error>>
{
    type Item = ();
    type Error = S::Error;

    fn poll(&mut self) -> Poll<(), S::Error> {
        match self.inner.poll() {
            Err(Stop::Terminate) => Ok(Async::Ready(())),
            Err(Stop::Fail(e)) => Err(e),
            Ok(a) => Ok(a),
        }
    }
}

pub enum Stop<E> {
    Terminate,
    Fail(E),
}

impl<E> From<E> for Stop<E> {
    fn from(e: E) -> Self {
        Stop::Fail(e)
    }
}

pub trait DoWhileExt: Stream {
    fn do_while<F>(self, f: F) -> DoWhile<Self, F>
        where F: FnMut(Self::Item) -> Result<(), Stop<Self::Error>>,
              Self: Sized;
}

impl<T: Stream> DoWhileExt for T {
    fn do_while<F>(self, f: F) -> DoWhile<Self, F>
        where F: FnMut(Self::Item) -> Result<(), Stop<Self::Error>>,
              Self: Sized
    {
        DoWhile {
            inner: self.map_err(Stop::Fail as _)
                .for_each(f),
        }
    }
}
