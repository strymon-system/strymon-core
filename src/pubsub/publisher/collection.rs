use std::io::{Result};
use std::any::Any;
use std::collections::BTreeMap;
use std::slice::Iter as VecIter;

use abomonation::Abomonation;

use network::{Network, Sender};
use network::message::abomonate::{Abomonate, NonStatic};
use network::message::MessageBuf;

use super::{PollServer, PublisherServer, SubscriberId, SubscriberEvent};

pub struct CollectionPublisher<D> {
    server: PollServer,
    subscribers: BTreeMap<SubscriberId, Sender>,
    collection: Vec<(D, i32)>,
}

impl<D: Abomonation + Any + Clone + Eq + NonStatic> CollectionPublisher<D> {
    pub fn new(network: &Network) -> Result<((String, u16), Self)> {
        let server = PublisherServer::new(network)?;
        let addr = {
            let (host, port) = server.external_addr();
            (host.to_string(), port)
        };

        Ok((addr, CollectionPublisher {
            server: PollServer::from(server),
            subscribers: BTreeMap::new(),
            collection: Vec::new(),
        }))
    }

    fn update_from(&mut self, updates: &Vec<(D, i32)>) {
        // warning: this is a naive nested loop join,
        //          we might want to use specialization at some point
        for &(ref new, delta) in updates.iter() {
            let position = self.collection.iter().position(|&(ref old, _)| old == new);
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
                self.collection.push((new.clone(), delta));
            }
        }
    }

    pub fn publish(&mut self, updates: &Vec<(D, i32)>) -> Result<()> {   
        let mut accepted = Vec::new();
        for event in self.server.poll_events()? {
            match event {
                SubscriberEvent::Accepted(id, tx) => {
                    accepted.push((id, tx));
                }
                SubscriberEvent::Disconnected(id, _) => {
                    self.subscribers.remove(&id);
                }
            }
        }

        self.update_from(updates);

        // send whole collection new the newbies
        if !accepted.is_empty() {
            let mut buf = MessageBuf::empty();
            buf.push::<Abomonate, Vec<(D, i32)>>(&self.collection).unwrap();
            for &(_, ref sub) in accepted.iter() {
                sub.send(buf.clone())
            }
        }

        // send just the updates to the old ones
        if !self.subscribers.is_empty() {
            let mut buf = MessageBuf::empty();
            buf.push::<Abomonate, Vec<(D, i32)>>(updates).unwrap();
            for sub in self.subscribers.values() {
                sub.send(buf.clone())
            }
        }
        
        self.subscribers.extend(accepted);

        Ok(())
    }
}

impl<'a, D> IntoIterator for &'a CollectionPublisher<D> {
    type Item = &'a D;
    type IntoIter = Iter<'a, D>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            inner: self.collection.iter(),
            current: None,
        }
    }
}

pub struct Iter<'a, T: 'a> {
    inner: VecIter<'a, (T, i32)>,
    current: Option<(&'a T, i32)>,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;
    
    fn next(&mut self) -> Option<Self::Item> {
        // move iterator if current is empty or exhausted
        self.current = match self.current {
            None | Some((_ , 0)) => {
                self.inner.next().map(|&(ref d, count)| (d, count))
            },
            _ => self.current,
        };

        if let Some((ref data, ref mut count)) = self.current {
            *count -= 1;
            Some(data)
        } else {
            None
        }
    }
}
