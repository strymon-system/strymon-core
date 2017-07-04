use std::io::Result;
use std::any::Any;
use std::marker::PhantomData;
use std::collections::BTreeMap;

use serde::ser::Serialize;

use strymon_communication::Network;
use strymon_communication::transport::Sender;
use strymon_communication::message::MessageBuf;

use super::{PollServer, PublisherServer, SubscriberId, SubscriberEvent};

pub struct Publisher<D> {
    server: PollServer,
    subscribers: BTreeMap<SubscriberId, Sender>,
    marker: PhantomData<D>,
}

impl<D: Serialize> Publisher<D> {
    pub fn new(network: &Network) -> Result<((String, u16), Self)> {
        let server = PublisherServer::new(network)?;
        let addr = {
            let (host, port) = server.external_addr();
            (host.to_string(), port)
        };

        Ok((addr,
            Publisher {
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
                SubscriberEvent::Disconnected(id) |
                SubscriberEvent::Error(id, _) => {
                    self.subscribers.remove(&id);
                }
            }
        }

        if !self.subscribers.is_empty() {
            let mut buf = MessageBuf::empty();
            buf.push::<&[D]>(item.as_slice()).unwrap();
            for sub in self.subscribers.values() {
                sub.send(buf.clone())
            }
        }

        Ok(())
    }
}
