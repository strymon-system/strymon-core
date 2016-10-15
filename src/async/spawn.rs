

use futures::{self, Future, Poll, Async};
use std::cell::RefCell;
use std::mem;

pub type TaskFuture = Box<Future<Item = (), Error = ()>>;

thread_local! {
    static PENDING: RefCell<Option<Vec<TaskFuture>>> = RefCell::new(None)
}

pub fn spawn<F: Future<Item = (), Error = ()> + 'static>(f: F) {
    PENDING.with(move |pending| {
        pending.borrow_mut()
            .as_mut()
            .expect("not within finish()")
            .push(Box::new(f))
    });
}

// adaption of Vec::retain
fn retain<T, F>(vec: &mut Vec<T>, mut f: F)
    where F: FnMut(&mut T) -> bool
{
    let len = vec.len();
    let mut del = 0;
    {
        let v = &mut **vec;

        for i in 0..len {
            if !f(&mut v[i]) {
                del += 1;
            } else if del > 0 {
                v.swap(i - del, i);
            }
        }
    }
    if del > 0 {
        vec.truncate(len - del);
    }
}

struct Finish {
    running: Vec<TaskFuture>,
}

impl Finish {
    fn new() -> Self {
        Finish { running: Vec::new() }
    }

    fn pending(&self) -> bool {
        PENDING.with(|pending| {
            pending.borrow()
                .as_ref()
                .map(|vec| !vec.is_empty())
                .expect("not within finish()")
        })
    }

    fn schedule_pending(&mut self) {
        PENDING.with(|pending| {
            self.running.append(pending.borrow_mut()
                .as_mut()
                .expect("not within finish()"));
        });
    }
}

impl Future for Finish {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        loop {
            // this will drop all finished futures
            retain(&mut self.running, |future| {
                match future.poll() {
                    Ok(Async::NotReady) => true,
                    Ok(Async::Ready(())) | Err(()) => false,
                }
            });

            if self.pending() {
                // we've produced some more work
                self.schedule_pending();
            } else if !self.running.is_empty() {
                // at this point, there have been no more pending futures,
                // but there are still non-ready running futures
                return Ok(Async::NotReady);
            } else {
                // no more work to be done
                debug_assert!(self.running.is_empty());
                debug_assert!(!self.pending());
                return Ok(Async::Ready(()));
            }
        }


    }
}

struct Reset(Option<Vec<TaskFuture>>);

impl Drop for Reset {
    fn drop(&mut self) {
        PENDING.with(move |pending| {
            mem::swap(&mut *pending.borrow_mut(), &mut self.0)
        });
    }
}

pub fn finish<F: Future + 'static>(f: F) -> Result<F::Item, F::Error> {
    let (tx, rx) = futures::oneshot();
    let body = Box::new(f.then(move |res| Ok(tx.complete(res))));

    // save currently pending queue on stack, will be reset on drop
    let _reset = Reset(PENDING.with(move |pending| {
        mem::replace(&mut *pending.borrow_mut(), Some(vec![body]))
    }));

    // start the root future and wait for completion
    drop(Finish::new().wait());

    // return result
    rx.wait().expect("finish() result got canceled")
}

#[cfg(test)]
mod tests {
    use futures::{self, Future};
    use futures::stream::{self, Stream};
    use super::*;

    #[test]
    fn finish_spawn() {
        finish(futures::lazy(|| {
                let (tx, rx) = stream::channel::<i32, ()>();

                // receive in parallel
                spawn(rx.collect().and_then(|res| {
                    assert_eq!(res, vec![1, 2, 3]);
                    Ok(())
                }));

                tx.send(Ok(1))
                    .and_then(|tx| tx.send(Ok(2)))
                    .and_then(|tx| tx.send(Ok(3)))
                    .map(|_| ())
                    .map_err(|err| panic!("sender failed: {:?}", err))
            }))
            .unwrap()
    }

    #[test]
    fn nested_finish() {
        let res = finish(futures::lazy(|| {
                let (tx, rx) = futures::oneshot();

                tx.complete(23);
                finish(rx)
            }))
            .unwrap();
        assert_eq!(res, 23);
    }
}
