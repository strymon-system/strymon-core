use std::io::{Error as IoError, Result as IoResult};
use std::env;
use std::marker::PhantomData;
use std::collections::BTreeMap;
use std::sync::mpsc;

use timely::Data;
use timely::progress::Timestamp;
use timely::dataflow::channels::Content;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::{Exchange, Pipeline, ParallelizationContract};

use timely_communication::{Allocate, Push, Pull};

use coordinator::catalog::request::PublishError as CatalogError;
use messaging::{self, Sender, Receiver, Message};

use executor::executable;
use worker::coordinator::Catalog;
use topic::{Topic, TypeId};

use util::Generator;

#[derive(Debug)]
pub enum PublishError {
    Catalog(CatalogError),
    Io(IoError),
}

impl From<IoError> for PublishError {
    fn from(io: IoError) -> Self {
        PublishError::Io(io)
    }
}

impl From<CatalogError> for PublishError{
    fn from(c: CatalogError) -> Self {
        PublishError::Catalog(c)
    }
}

pub struct Publisher {
    catalog: Catalog,
    per_worker: bool,
}

const PUBLISH_WORKER_ID: usize = 0;

enum Pact<D,  F: Fn(&D) -> u64 + 'static> {
    Pipeline(Pipeline),
    Exchange(Exchange<D, F>),
}

impl<T: Timestamp, D: Data, F: Fn(&D) -> u64 + 'static> ParallelizationContract<T, D> for Pact<D, F> {
    fn connect<A: Allocate>(self, allocator: &mut A, identifier: usize) -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>) {
        match self {
            Pact::Pipeline(pipeline) => pipeline.connect(allocator, identifier),
            Pact::Exchange(exchange) => exchange.connect(allocator, identifier),
        }
    }
}

impl Publisher {
    pub fn new(catalog: &Catalog) -> Self {
        Publisher {
            catalog: catalog.clone(),
            per_worker: false,
        }
    }
    
    pub fn per_worker(&mut self, flag: bool) -> &mut Self {
        self.per_worker = flag;
        self
    }
    
    pub fn publish<S: Scope, D: Data>(self, name: &str, stream: &Stream<S, D>) -> Result<Stream<S, D>, PublishError> {
        let worker_index = stream.scope().index();

        let (name, pact) = if self.per_worker {
            // every worker will publish its own topic
            let name = format!("{}.{}", name, stream.scope().index());
            let pact = Pact::Pipeline(Pipeline);
            (name, pact)
        } else {
            // collect all data at a single worker
            let name = name.to_string();
            let pact = Pact::Exchange(Exchange::new(|_| PUBLISH_WORKER_ID as u64));
            (name, pact)
        };

        // if we don't publisher per worker, we only need a single instance
        let mut publisher = if self.per_worker || worker_index == PUBLISH_WORKER_ID {
            Some(PublisherState::new(self.catalog, name)?)
        } else {
            None
        };

        let stream = stream.unary_stream::<D, _, _>(pact, "publisher", move |input, output| {
            input.for_each(|time, data| {
                if let Some(ref mut publisher) = publisher {
                    publisher.publish(data);
                }

                output.session(&time).give_content(data);
            });
        });

        Ok(stream)
    }

}

type SubscriberId = u64;

enum Event {
    NewSubscriber(IoResult<(Sender, Receiver)>),
    Subscriber(SubscriberId, IoResult<Message>),
}

struct PublisherState<D> {
    topic: Topic,
    subscribers: BTreeMap<SubscriberId, Sender>,
    subscriber_id: Generator<SubscriberId>,
    catalog: Catalog,
    event_rx: mpsc::Receiver<Event>,
    event_tx: mpsc::Sender<Event>,
    marker: PhantomData<D>,
}

impl<D: Data> PublisherState<D> {
    fn new(catalog: Catalog, name: String) -> Result<Self, PublishError> {
        let listener = messaging::listen(None)?;
        let host  = env::var(executable::HOST).expect("unable to get external hostname");
        let addr = listener.external_addr(&host)?;
        let topic = catalog.publish(name, addr, TypeId::of::<D>()).await()?;

        let (event_tx, event_rx) = mpsc::channel();
        let accept_tx = event_tx.clone();
        listener.detach(move |res| {
            drop(accept_tx.send(Event::NewSubscriber(res)))
        });

        Ok(PublisherState {
            topic: topic,
            event_tx: event_tx,
            event_rx: event_rx,
            subscribers: BTreeMap::new(),
            subscriber_id: Generator::new(),
            catalog: catalog,
            marker: PhantomData,
        })
    }

    fn handle_events(&mut self) {
        while let Ok(event) = self.event_rx.try_recv() {
            match event {
                Event::NewSubscriber(Ok((tx, rx))) => {
                    let event_tx = self.event_tx.clone();
                    let id = self.subscriber_id.generate();
                    rx.detach(move |res| {
                        drop(event_tx.send(Event::Subscriber(id, res)))
                    });
                    self.subscribers.insert(id, tx);
                },
                Event::NewSubscriber(Err(err)) => {
                    panic!("publisher listener failed: {:?}", err)
                },
                Event::Subscriber(id, Err(err)) => {
                    info!("removing subscriber {:?} because of {:?}", id, err);
                    self.subscribers.remove(&id);
                },
                Event::Subscriber(id, Ok(_)) => {
                    panic!("unexpected message from subscriber {:?}", id)
                },
            }
        }
    }

    fn publish(&mut self, data: &mut Content<D>) {
        self.handle_events();

        let data: &Vec<D> = &*data;
        for sub in self.subscribers.values() {
            sub.send(data)
        }
    }
}

impl<D> Drop for PublisherState<D> {
    fn drop(&mut self) {
        drop(self.catalog.unpublish(self.topic.id))
    }
}

pub trait Publish<S: Scope, D: Data> {
    fn publish(&self, name: &str, catalog: &Catalog) -> Result<Stream<S, D>, PublishError>;
}

impl<S: Scope, D: Data> Publish<S, D> for Stream<S, D> {
    fn publish(&self, name: &str, catalog: &Catalog) -> Result<Stream<S, D>, PublishError> {
        Publisher::new(&catalog).publish(name, self)
    }
}
