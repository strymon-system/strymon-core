use std::io::{Result, Error, ErrorKind};
use std::any::Any;
use std::marker::PhantomData;
use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;

use futures::{Poll, Async};
use futures::stream::{Stream, Fuse};
use futures::task::{self, Unpark, Spawn};
use abomonation::Abomonation;

use network::{Network, Listener, Receiver, Sender};
use network::message::abomonate::{Abomonate, NonStatic};
use network::message::MessageBuf;

pub mod item;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriberId(pub u32);

pub struct PublisherServer {
    listener: Fuse<Listener>,
    subscribers: Vec<(SubscriberId, Fuse<Receiver>)>,
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
            self.subscribers.push((id, rx.fuse()));
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
                Ok(Async::NotReady) => return true,
                Ok(Async::Ready(_)) => {
                    error!("unexpected message from subscriber {:?}", id);
                    return false;
                }
                Err(err) => {
                    events.push(SubscriberEvent::Disconnected(id, err));
                    return false;
                }
            }
        });
    }
}

pub enum SubscriberEvent {
    Accepted(SubscriberId, Sender),
    Disconnected(SubscriberId, Error),
}

impl Stream for PublisherServer {
    type Item = Vec<SubscriberEvent>;
    type Error = Error;
    
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // these will fill self.events
        if !self.listener.is_done() {
            self.poll_listener()?;
        }
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
impl Unpark for Nop {
    fn unpark(&self) { }
}

struct PollServer {
    server: Spawn<PublisherServer>,
    unpark: Arc<Unpark>,
}

impl PollServer {
    fn poll_events(&mut self) -> Result<Vec<SubscriberEvent>> {
        match self.server.poll_stream(self.unpark.clone())? {
            Async::Ready(Some(events)) => Ok(events),
            Async::NotReady => Ok(Vec::new()),
            Async::Ready(None) => Err(Error::new(ErrorKind::NotConnected, "disconnected")),
        }
    }
}

impl From<PublisherServer> for PollServer {
    fn from(server: PublisherServer) -> Self {
        PollServer {
            server: task::spawn(server),
            unpark: Arc::new(Nop),
        }
    }
}
