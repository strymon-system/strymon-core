use std::io::Result;
use std::any::Any;
use std::marker::PhantomData;
use std::collections::BTreeMap;

use serde::ser::Serialize;

use strymon_communication::Network;
use strymon_communication::transport::Sender;
use strymon_communication::message::MessageBuf;

use super::{PollServer, PublisherServer, SubscriberId, SubscriberEvent};

pub struct TimelyPublisher<T, D> {
    server: PollServer,
    subscribers: BTreeMap<SubscriberId, Sender>,
    marker: PhantomData<(T, D)>,
}

impl<T, D> TimelyPublisher<T, D>
    where T: Serialize,
          D: Serialize
{
    pub fn new(network: &Network) -> Result<((String, u16), Self)> {
        let server = PublisherServer::new(network)?;
        let addr = {
            let (host, port) = server.external_addr();
            (host.to_string(), port)
        };

        Ok((addr,
            TimelyPublisher {
                server: PollServer::from(server),
                subscribers: BTreeMap::new(),
                marker: PhantomData,
            }))
    }

    pub fn publish(&mut self, frontier: &[T], time: &T, item: &[D]) -> Result<()> {
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
            buf.push::<&[D]>(item).unwrap();
            buf.push::<&T>(time).unwrap();
            buf.push::<&[T]>(frontier).unwrap();

            for sub in self.subscribers.values() {
                sub.send(buf.clone())
            }
        }
        Ok(())
    }
}
