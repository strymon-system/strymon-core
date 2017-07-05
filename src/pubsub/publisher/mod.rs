use std::io::{Result, Error, ErrorKind};
use std::mem;
use std::sync::Arc;

use futures::{Poll, Async};
use futures::stream::{Stream, Fuse};
use futures::executor::{self, Notify, Spawn};

use strymon_communication::Network;
use strymon_communication::transport::{Listener, Receiver, Sender};

pub mod item;
pub mod timely;
pub mod collection;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriberId(pub u32);

struct PublisherServer {
    listener: Fuse<Listener>,
    subscribers: Vec<(SubscriberId, Receiver)>,
    events: Vec<SubscriberEvent>,
    next_id: u32,
    addr: (String, u16),
}

impl PublisherServer {
    pub fn new(network: &Network) -> Result<Self> {
        let listener = network.listen(None)?;
        let addr = {
            let addr = listener.external_addr();
            (addr.0.to_string(), addr.1)
        };
        Ok(PublisherServer {
            listener: listener.fuse(),
            subscribers: Vec::new(),
            events: Vec::new(),
            next_id: 0,
            addr: addr,
        })
    }

    pub fn external_addr(&self) -> (&str, u16) {
        (&self.addr.0, self.addr.1)
    }

    fn poll_listener(&mut self) -> Result<()> {
        while let Async::Ready(Some((tx, rx))) = self.listener.poll()? {
            self.next_id += 1;
            let id = SubscriberId(self.next_id);

            self.events.push(SubscriberEvent::Accepted(id, tx));
            self.subscribers.push((id, rx));
        }

        Ok(())
    }

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

    fn poll_subscribers(&mut self) {
        let events = &mut self.events;
        PublisherServer::retain(&mut self.subscribers, move |&mut (id, ref mut rx)| {
            match rx.poll() {
                Ok(Async::NotReady) => true,
                Ok(Async::Ready(Some(_))) => {
                    error!("unexpected message from subscriber {:?}", id);
                    false
                }
                Ok(Async::Ready(None)) => {
                    events.push(SubscriberEvent::Disconnected(id));
                    false
                }
                Err(err) => {
                    events.push(SubscriberEvent::Error(id, err));
                    false
                }
            }
        });
    }
}

pub enum SubscriberEvent {
    Accepted(SubscriberId, Sender),
    Error(SubscriberId, Error),
    Disconnected(SubscriberId),
}

impl Stream for PublisherServer {
    type Item = Vec<SubscriberEvent>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // these functions populate self.events
        if !self.listener.is_done() {
            // accepting new subscribers
            self.poll_listener()?;
        }
        // removing old ones
        self.poll_subscribers();

        if !self.events.is_empty() {
            // return current list of events
            let events = mem::replace(&mut self.events, Vec::new());
            Ok(Async::Ready(Some(events)))
        } else if self.listener.is_done() && self.subscribers.is_empty() {
            // we're done
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

struct Nop;
impl Notify for Nop {
    fn notify(&self, _: usize) {}
}

struct PollServer {
    server: Spawn<PublisherServer>,
    notify: Arc<Nop>,
}

impl PollServer {
    fn poll_events(&mut self) -> Result<Vec<SubscriberEvent>> {
        match self.server.poll_stream_notify(&self.notify, 0)? {
            Async::Ready(Some(events)) => Ok(events),
            Async::NotReady => Ok(Vec::new()),
            Async::Ready(None) => {
                Err(Error::new(ErrorKind::NotConnected, "server closed"))
            }
        }
    }
}

impl From<PublisherServer> for PollServer {
    fn from(server: PublisherServer) -> Self {
        PollServer {
            server: executor::spawn(server),
            notify: Arc::new(Nop),
        }
    }
}
