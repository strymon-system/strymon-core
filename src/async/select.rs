use std::mem;

use futures::{Future, Async, Poll};
use futures::stream::Stream;

use void::Void;

enum Source<T> {
    Future(Box<Future<Item=(), Error=()>>),
    Stream(Box<Stream<Item=T, Error=()>>),
}

enum SourceAsync<T> {
    NotReady,
    Yield(Option<T>),
    Done,
}

impl<T> Source<T> {
    fn poll(&mut self) -> SourceAsync<T> {
        match *self {
            Source::Future(ref mut f) => {
                match f.poll() {
                    Ok(Async::NotReady) => SourceAsync::NotReady,
                    Ok(Async::Ready(())) | Err(()) => SourceAsync::Done,
                }
            },
            Source::Stream(ref mut s) => {
                match s.poll() {
                    Ok(Async::NotReady) => SourceAsync::NotReady,
                    Ok(Async::Ready(Some(o))) => SourceAsync::Yield(Some(o)),
                    Ok(Async::Ready(None)) => SourceAsync::Done,
                    Err(()) => SourceAsync::Yield(None),
                }
            }
        }
    }
}

struct SourceStream<T> {
    polled: Vec<Source<T>>,
    cursor: usize,
}

impl<T> SourceStream<T> {
    fn new() -> Self {
        SourceStream {
            polled: Vec::new(),
            cursor: 0,
        }
    }
    
    fn select(&mut self) -> Async<Option<T>> {
        let len = self.polled.len();
        assert!(len > 0, "we require something to poll here!");
        // poll every future/stream at most once
        for _ in 0..len {
            // ensure cursor wraps around
            self.cursor %= len;

            match self.polled[self.cursor].poll() {
                SourceAsync::NotReady => {
                    // this stream/future is not ready, try next one
                    self.cursor += 1;
                    
                    continue;
                }
                SourceAsync::Yield(t) => {
                    // this stream is ready, and will yield more
                    self.cursor += 1;
                    
                    return Async::Ready(t);
                }
                SourceAsync::Done => {
                    // this stream/future is done, remove it
                    drop(self.polled.swap_remove(self.cursor));

                    return Async::Ready(None);
                }
            }
        }
        
        // we've polled all of them, and none of them was ready!
        Async::NotReady
    }
}

impl<T> Stream for SourceStream<T> {
    type Item = Option<T>;
    type Error = Void;

    fn poll(&mut self) -> Poll<Option<Option<T>>, Void> {
        if self.polled.is_empty() {
            Ok(Async::Ready(None))
        } else {
            match self.select() {
                Async::NotReady => Ok(Async::NotReady),
                Async::Ready(o) => Ok(Async::Ready(Some(o)))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Empty;

pub struct Select<T> {
    stream: SourceStream<T>,
}

impl<T> Select<T> {
    pub fn new() -> Self {
        Select {
            stream: SourceStream::new(),
        }
    }

    pub fn ensure<F: Future<Item=(), Error=()> + 'static>(&mut self, f: F) {
        self.stream.polled.push(Source::Future(Box::new(f)))
    }

    pub fn drain<S: Stream<Item=T, Error=()> + 'static>(&mut self, s: S) {
        self.stream.polled.push(Source::Stream(Box::new(s)))
    }

    pub fn recv(&mut self) -> Result<Option<T>, Empty> {
        // temporarily take ownership over stream
        let stream = mem::replace(&mut self.stream, SourceStream::new());

        let (result, stream) = match stream.into_future().wait() {
            Ok((result, stream)) => (result, stream),
            Err(_) => unreachable!("SourceStream cannot not return error")
        };

        // and put the old value back
        mem::replace(&mut self.stream, stream);

        // turn None into Err(Empty)
        result.ok_or(Empty)
    }
}
