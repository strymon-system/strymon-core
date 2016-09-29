

use mio::{Events, Poll, PollOpt, Ready, Token};
use mio::channel::{Receiver, Sender, channel};
use std::collections::BTreeMap;
use std::io::Result;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

pub fn pair() -> (EventSender, EventReceiver) {
    let (tx, rx) = channel();
    let tx = EventSender { inner: tx };
    let rx = EventReceiver { inner: Arc::new(rx) };

    (tx, rx)
}

#[derive(Clone)]
pub struct EventSender {
    inner: Sender<()>,
}

impl EventSender {
    pub fn notify(&self) -> bool {
        self.inner.send(()).is_ok()
    }
}

#[derive(Clone)]
pub struct EventReceiver {
    inner: Arc<Receiver<()>>,
}

impl EventReceiver {
    fn ack(&self) -> bool {
        use ::std::sync::mpsc::TryRecvError::*;

        match self.inner.try_recv() {
            Ok(()) => true,
            Err(Empty) => false,
            Err(Disconnected) => panic!("event sender notified, but dropped?!"),
        }
    }

    fn token(&self) -> Token {
        // the address of an deref'd Arc will never change
        let stable_addr = self.inner.deref() as *const Receiver<()>;
        Token(stable_addr as usize)
    }
}

pub struct EventDispatch {
    poller: Poll,
    receiver: BTreeMap<Token, (EventReceiver, Box<Fn()>)>,
    events: Events,
}

impl EventDispatch {
    pub fn new() -> Result<Self> {
        Ok(EventDispatch {
            poller: Poll::new()?,
            receiver: BTreeMap::new(),
            events: Events::with_capacity(128),
        })
    }

    pub fn register<F: Fn() + 'static>(&mut self, event: EventReceiver, f: F) -> Result<()> {
        let token = event.token();
        self.poller.register(&*event.inner, token, Ready::readable(), PollOpt::edge())?;
        assert!(self.receiver.insert(token, (event, Box::new(f))).is_none());
        Ok(())
    }

    pub fn deregister(&mut self, event: EventReceiver) -> Result<()> {
        let token = event.token();
        self.poller.deregister(&*event.inner)?;
        self.receiver.remove(&token);

        Ok(())
    }

    pub fn dispatch(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.poller.poll(&mut self.events, timeout)?;
        trace!("poll returned {} events", self.events.len());
        for event in &self.events {
            let token = event.token();
            if let Some(&(ref rx, ref callback)) = self.receiver.get(&token) {
                // multiple sends will be collappsed into a single event,
                // thus we handle all of them in the following loop
                while rx.ack() {
                    callback();
                }
            } else {
                warn!("spurious event, no registered receiver found")
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_events() {
        use std::cell::Cell;
        use std::rc::Rc;

        let in_count = Rc::new(Cell::new(0i32));
        let out_count = in_count.clone();
        let (tx, rx) = pair();
        let mut waitset = EventDispatch::new().expect("failed to create eventdispatch");

        tx.notify();
        waitset.register(rx, move || {
            let cnt = in_count.get() + 1;
            in_count.set(cnt);
        });

        waitset.dispatch(None).unwrap();
        assert_eq!(1, out_count.get());

        tx.notify();
        tx.notify();
        waitset.dispatch(None).unwrap();
        assert_eq!(3, out_count.get());
    }
}
