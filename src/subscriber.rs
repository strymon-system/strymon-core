use std::io::{Error as IoError, Result as IoResult};
use std::sync::mpsc;

use timely::Data;

use coordinator::catalog::request::SubscribeError as CatalogError;
use messaging::{self, Message, Sender};
use messaging::decoder::Decoder;

use worker::coordinator::Catalog;
use topic::{Topic, TypeId};

#[derive(Debug)]
pub enum SubscribeError {
    Catalog(CatalogError),
    Io(IoError),
}

impl From<IoError> for SubscribeError {
    fn from(io: IoError) -> Self {
        SubscribeError::Io(io)
    }
}

impl From<CatalogError> for SubscribeError {
    fn from(c: CatalogError) -> Self {
        SubscribeError::Catalog(c)
    }
}

pub struct Subscriber<D> {
    topic: Topic,
    catalog: Catalog,
    current: Vec<D>,
    publisher_rx: mpsc::Receiver<IoResult<Message>>,
    _publisher_tx: Sender,
}

impl<D: Data> Subscriber<D> {
    pub fn from(catalog: &Catalog, name: &str) -> Result<Self, SubscribeError> {
        let topic = catalog.subscribe(name.to_string(), true).await()?;
        assert_eq!(topic.dtype, TypeId::of::<D>(), "topic has wrong dtype");

        let (net_tx, net_rx) = messaging::connect(&topic.addr)?;
        let (event_tx, event_rx) = mpsc::channel();
        net_rx.detach(move |res| drop(event_tx.send(res)));

        Ok(Subscriber {
            topic: topic,
            catalog: catalog.clone(),
            current: Vec::with_capacity(0),
            publisher_rx: event_rx,
            _publisher_tx: net_tx,
        })
    }
}

impl<D: Data> Iterator for Subscriber<D> {
    type Item = D;
    fn next(&mut self) -> Option<D> {
        while self.current.is_empty() {
            trace!("subscriber: waiting for data on {:?}", self.topic);
            match self.publisher_rx.recv().unwrap() {
                Ok(msg) => {
                    Decoder::from(msg)
                        .when::<Vec<D>, _>(|vec| self.current = vec)
                        .expect("failed to parse publisher message");
                }
                Err(err) => {
                    info!("subscriber: publisher disconnected: {:?}", err);
                    break;
                }
            }
        }

        self.current.pop()
    }
}

impl<D> Drop for Subscriber<D> {
    fn drop(&mut self) {
        drop(self.catalog.unsubscribe(self.topic.id))
    }
}
