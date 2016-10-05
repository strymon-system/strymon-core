use std::io::{Result as IoResult};
use std::env;
use std::collections::BTreeMap;
use std::sync::mpsc;

use timely::Data;
use timely::progress::CountMap;

use messaging::{self, Message, Receiver, Sender};

use publisher::PublishError;
use executor::executable;
use worker::coordinator::Catalog;
use model::{Topic, TopicType};

use util::Generator;

type SubscriberId = u64;

enum Event {
    NewSubscriber(IoResult<(Sender, Receiver)>),
    Subscriber(SubscriberId, IoResult<Message>),
}

pub struct CollectionPublisher<D> {
    catalog: Catalog,
    collection: CountMap<D>,
    topic: Topic,
    subscribers: BTreeMap<SubscriberId, Sender>,
    subscriber_id: Generator<SubscriberId>,
    event_rx: mpsc::Receiver<Event>,
    event_tx: mpsc::Sender<Event>,
}

impl<D: Data + Eq> CollectionPublisher<D> {
    pub fn new(catalog: &Catalog, name: String) -> Result<Self, PublishError> {
        let listener = messaging::listen(None)?;
        let host = env::var(executable::HOST).expect("unable to get external hostname");
        let addr = listener.external_addr(&host)?;
        
        let ttype = TopicType::of::<(D, i64)>();
        let topic = catalog.publish(name, addr, ttype).await()?;

        let (event_tx, event_rx) = mpsc::channel();
        let accept_tx = event_tx.clone();
        listener.detach(move |res| drop(accept_tx.send(Event::NewSubscriber(res))));

        Ok(CollectionPublisher {
            catalog: catalog.clone(),
            collection: CountMap::new(),
            topic: topic,
            event_tx: event_tx,
            event_rx: event_rx,
            subscribers: BTreeMap::new(),
            subscriber_id: Generator::new(),
        })
    }

    fn handle_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                Event::NewSubscriber(Ok((tx, rx))) => {
                    let event_tx = self.event_tx.clone();
                    let id = self.subscriber_id.generate();
                    rx.detach(move |res| drop(event_tx.send(Event::Subscriber(id, res))));
                    
                    // TODO this is incredebly inefficient
                    let tuples = self.collection.clone().into_inner();
                    tx.send(&tuples);

                    self.subscribers.insert(id, tx);
                }
                Event::NewSubscriber(Err(err)) => panic!("publisher listener failed: {:?}", err),
                Event::Subscriber(id, Err(err)) => {
                    info!("removing subscriber {:?} because of {:?}", id, err);
                    self.subscribers.remove(&id);
                }
                Event::Subscriber(id, Ok(_)) => {
                    panic!("unexpected message from subscriber {:?}", id)
                }
            }
        }
    }

    pub fn update(&mut self, element: D, delta: i64) {
        self.handle_events();
        
        self.collection.update(&element, delta);

        let data: Vec<(D, i64)> = vec![(element, delta)];
        for sub in self.subscribers.values() {
            sub.send(&data)
        }
    }
}

impl<D> Drop for CollectionPublisher<D> {
    fn drop(&mut self) {
        drop(self.catalog.unpublish(self.topic.id))
    }
}
