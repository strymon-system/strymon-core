use std::io::{Result, Error};
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use abomonation::Abomonation;
use futures::{Future, Poll, Async};
use futures::task::{self, Unpark, Spawn};
use futures::stream::Stream;
use void::{self, Void};

use async::queue;

use network::{Network, Sender};
use network::message::abomonate::{Abomonate, NonStatic};
use network::message::MessageBuf;

use super::{Nop, PublisherServer, SubscriberId, SubscriberEvent};

pub struct CollectionPublisher<D> {
    server: PublisherServer,
    subscribers: BTreeMap<SubscriberId, Sender>,
    source: queue::Receiver<Vec<(D, i32)>, Void>,
    collection: Vec<(D, i32)>,
}

impl<D: Abomonation + Any + Clone + Eq + NonStatic> CollectionPublisher<D> {
    pub fn new(network: &Network) -> Result<((String, u16), Mutator<D>, Self)> {
        let server = PublisherServer::new(network)?;
        let addr = {
            let (host, port) = server.external_addr();
            (host.to_string(), port)
        };

        let (tx, rx) = queue::channel();

        let sink = Mutator { sink: tx };

        let publisher = CollectionPublisher {
            server: server,
            subscribers: BTreeMap::new(),
            collection: Vec::new(),
            source: rx,
        };

        Ok((addr, sink, publisher))
    }

    fn update_from(&mut self, updates: Vec<(D, i32)>) {
        // warning: this is a naive nested loop join,
        //          we might want to use specialization at some point
        for (new, delta) in updates {
            let position = self.collection.iter().position(|&(ref old, _)| old == &new);
            if let Some(pos) = position {
                // update count in already existing entry
                self.collection[pos].1 += delta;
                let count = self.collection[pos].1;
                if count == 0 {
                    self.collection.swap_remove(pos);
                }

                assert!(count >= 0, "negative amount in collection");
            } else if delta > 0 {
                // add a new one
                self.collection.push((new, delta));
            }
        }
    }

    pub fn spawn(self) -> SpawnedPublisher {
        SpawnedPublisher {
            publisher: task::spawn(Box::new(self)),
            unpark: Arc::new(Nop),
        }
    }
}

pub struct SpawnedPublisher {
    publisher: Spawn<Box<Future<Item = (), Error = Error>>>,
    unpark: Arc<Unpark>,
}

impl SpawnedPublisher {
    pub fn poll(&mut self) -> Result<()> {
        self.publisher.poll_future(self.unpark.clone()).map(|_| ())
    }
}

impl<D: Abomonation + Any + Clone + Eq + NonStatic> Future for CollectionPublisher<D> {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // step 1: check for collection updates
        let updates = match self.source.poll() {
            // done
            Ok(Async::Ready(None)) => return Ok(Async::Ready(())),
            Ok(Async::Ready(updates)) => updates,
            Ok(Async::NotReady) => None,
            Err(v) => void::unreachable(v),
        };

        // step 2: check for changes in the subscriber list
        let events = match self.server.poll()? {
            Async::Ready(None) => return Ok(Async::Ready(())),
            Async::Ready(Some(subs)) => subs,
            Async::NotReady => Vec::new(),
        };

        // step 3: update local subscriber lists
        let mut accepted = Vec::new();
        for event in events {
            match event {
                SubscriberEvent::Accepted(id, tx) => {
                    accepted.push((id, tx));
                }
                SubscriberEvent::Disconnected(id) |
                SubscriberEvent::Error(id, _) => {
                    self.subscribers.remove(&id);
                }
            }
        }

        // step 4: send updates to those who understand them
        if !self.subscribers.is_empty() && updates.is_some() {
            let updates = updates.as_ref().unwrap();
            let mut buf = MessageBuf::empty();
            buf.push::<Abomonate, Vec<(D, i32)>>(updates).unwrap();
            for sub in self.subscribers.values() {
                sub.send(buf.clone())
            }
        }

        // step 5: merge updates with local collection copy
        if let Some(updates) = updates {
            self.update_from(updates);
        };

        // step 6: inform incoming subscribers about current collection state
        if !accepted.is_empty() {
            let mut buf = MessageBuf::empty();
            buf.push::<Abomonate, Vec<(D, i32)>>(&self.collection).unwrap();
            for &(_, ref sub) in accepted.iter() {
                sub.send(buf.clone())
            }
            self.subscribers.extend(accepted);
        }

        Ok(Async::NotReady)
    }
}

pub struct Mutator<D> {
    sink: queue::Sender<Vec<(D, i32)>, Void>,
}

impl<D> Mutator<D> {
    pub fn update_from(&self, updates: Vec<(D, i32)>) {
        if let Err(_) = self.sink.send(Ok(updates)) {
            panic!("collection publisher disappeared")
        }
    }

    pub fn insert(&mut self, elem: D) {
        self.update_from(vec![(elem, 1)])
    }

    pub fn remove(&mut self, elem: D) {
        self.update_from(vec![(elem, -1)])
    }
}
