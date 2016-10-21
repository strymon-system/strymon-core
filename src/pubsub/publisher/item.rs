use std::io::{Result, Error, ErrorKind};
use std::any::Any;
use std::marker::PhantomData;
use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;

use abomonation::Abomonation;

use network::{Network, Listener, Receiver, Sender};
use network::message::abomonate::{Abomonate, NonStatic};
use network::message::MessageBuf;

use super::{PollServer, PublisherServer, SubscriberId, SubscriberEvent};

pub struct ItemPublisher<D> {
    server: PollServer,
    subscribers: BTreeMap<SubscriberId, Sender>,
    marker: PhantomData<D>,
}

impl<D: Abomonation + Any + Clone + NonStatic> ItemPublisher<D> {
    pub fn new(network: &Network) -> Result<((String, u16), Self)> {
        let server = PublisherServer::new(network)?;
        let addr = {
            let (host, port) = server.external_addr();
            (host.to_string(), port)
        };

        Ok((addr, ItemPublisher {
            server: PollServer::from(server),
            subscribers: BTreeMap::new(),
            marker: PhantomData,
        }))
    }

    pub fn publish(&mut self, item: &Vec<D>) -> Result<()> {
        for event in self.server.poll_events()? {
            match event {
                SubscriberEvent::Accepted(id, tx) => {
                    self.subscribers.insert(id, tx);
                }
                SubscriberEvent::Disconnected(id, _) => {
                    self.subscribers.remove(&id);
                }
            }
        }

        let mut buf = MessageBuf::empty();
        buf.push::<Abomonate, Vec<D>>(item).unwrap();
        for sub in self.subscribers.values() {
            sub.send(buf.clone())
        }

        Ok(())
    }
}

