use std::io::{Result};
use std::any::Any;
use std::marker::PhantomData;
use std::collections::BTreeMap;
use std::time::Instant;

use abomonation::Abomonation;

use network::{Network, Sender};
use network::message::abomonate::{Abomonate, NonStatic};
use network::message::MessageBuf;

use super::{PollServer, PublisherServer, SubscriberId, SubscriberEvent};

pub struct TimelyPublisher<T, D> {
    server: PollServer,
    subscribers: BTreeMap<SubscriberId, Sender>,
    marker: PhantomData<(T, D)>,
}

impl<T, D> TimelyPublisher<T, D>
    where T: Abomonation + Any + Clone + NonStatic,
          D: Abomonation + Any + Clone + NonStatic
{
    pub fn new(network: &Network) -> Result<((String, u16), Self)> {
        let server = PublisherServer::new(network)?;
        let addr = {
            let (host, port) = server.external_addr();
            (host.to_string(), port)
        };

        Ok((addr, TimelyPublisher {
            server: PollServer::from(server),
            subscribers: BTreeMap::new(),
            marker: PhantomData,
        }))
    }

    pub fn publish(&mut self, frontier: &[T], time: &T, item: &Vec<D>) -> Result<()> {
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

        let start = Instant::now();
        let mut buf = MessageBuf::empty();
        buf.push::<Abomonate, Vec<T>>(&frontier.to_owned()).unwrap();
        buf.push::<Abomonate, T>(time).unwrap();
        buf.push::<Abomonate, Vec<D>>(item).unwrap();
        let duration = start.elapsed();
        let nanos = duration.as_secs() * 1e9 as u64 + duration.subsec_nanos() as u64;
        info!("serialize,{}", nanos);

        for sub in self.subscribers.values() {
            sub.send(buf.clone())
        }

        Ok(())
    }
}
