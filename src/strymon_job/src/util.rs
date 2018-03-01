use futures::{Async, Poll};
use futures::stream::{Stream, StreamFuture, FuturesUnordered};

/// A merged set of streams. Similar to `FuturesUnordered`, but for homogenious
/// streams.
pub struct StreamsUnordered<S> {
    inner: FuturesUnordered<StreamFuture<S>>,
}

impl<S: Stream> StreamsUnordered<S> {
    /// Creates an empty polling set.
    pub fn new() -> Self {
        StreamsUnordered { inner: FuturesUnordered::new() }
    }

    /// Adds a stream to the polling set.
    pub fn push(&mut self, stream: S) {
        self.inner.push(stream.into_future())
    }
}

impl<S: Stream> Stream for StreamsUnordered<S> {
    type Item = S::Item;
    type Error = S::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match self.inner.poll() {
                Ok(Async::Ready(Some((Some(val), stream)))) => {
                    // stream returned a value, reregister and yield value
                    self.push(stream);
                    return Ok(Async::Ready(Some(val)));
                }
                Ok(Async::Ready(Some((None, stream)))) => {
                    // stream exhausted, drop it and try next one
                    drop(stream);
                    continue;
                }
                Ok(Async::Ready(None)) => {
                    // all streams exhausted
                    return Ok(Async::Ready(None));
                }
                Ok(Async::NotReady) => {
                    // all streams busy
                    return Ok(Async::NotReady);
                }
                Err((err, stream)) => {
                    // stream returned an error, reregister and yield error
                    self.push(stream);
                    return Err(err);
                }
            }
        }
    }
}
