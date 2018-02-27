// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::{Entry, Values};

use futures::{Future, Stream, Sink, Poll};
use futures::sync::mpsc;
use tokio_core::reactor::Handle;
use strymon_model::*;
use strymon_rpc::coordinator::*;
use strymon_rpc::coordinator::catalog::*;
use strymon_communication::Network;
use strymon_communication::rpc::RequestBuf;

use super::util::Generator;

type Addr = (String, u16);

#[derive(Debug, Clone, Default)]
pub struct Catalog {
    generator: Generator<TopicId>,
    directory: HashMap<String, TopicId>,

    topics: HashMap<TopicId, Topic>,
    executors: HashMap<ExecutorId, Executor>,
    queries: HashMap<QueryId, Query>,

    publications: HashSet<Publication>,
    subscriptions: HashSet<Subscription>,

    keepers: HashMap<KeeperId, Keeper>,
}

impl Catalog {
    pub fn new(addr: Addr) -> Self {
        let mut generator = Generator::<TopicId>::new();
        let mut directory = HashMap::<String, TopicId>::new();
        let mut topics = HashMap::<TopicId, Topic>::new();

        let catalog_topic = Topic {
            id: generator.generate(),
            name: String::from("$catalog"),
            addr: addr,
            schema: TopicSchema::Service(TopicType::of::<CatalogRPC>()),
        };

        directory.insert(catalog_topic.name.clone(), catalog_topic.id);
        topics.insert(catalog_topic.id, catalog_topic);

        Catalog {
           generator: generator,
           directory: directory,
           topics: topics,
           .. Default::default()
       }
    }

    pub fn add_executor(&mut self, executor: Executor) {
        debug!("add_executor: {:?}", executor);
        self.executors.insert(executor.id, executor);
    }

    pub fn remove_executor(&mut self, id: ExecutorId) {
        debug!("remove_executor: {:?}", id);
        self.executors.remove(&id);
    }

    pub fn executors<'a>(&'a self) -> Executors<'a> {
        Executors { inner: self.executors.values() }
    }

    pub fn add_query(&mut self, query: Query) {
        debug!("add_query: {:?}", query);
        self.queries.insert(query.id, query);
    }

    pub fn remove_query(&mut self, id: QueryId) {
        debug!("remove_query: {:?}", id);
        self.queries.remove(&id);
    }

    pub fn publish(&mut self,
                   query: QueryId,
                   name: String,
                   addr: (String, u16),
                   schema: TopicSchema)
                   -> Result<Topic, PublishError> {
        // TODO(swicki): Check if query actually exists
        match self.directory.entry(name.clone()) {
            Entry::Occupied(_) => Err(PublishError::TopicAlreadyExists),
            Entry::Vacant(entry) => {
                let id = self.generator.generate();
                let publication = Publication(query, id);
                let topic = Topic {
                    id: id,
                    name: name,
                    addr: addr,
                    schema: schema,
                };

                debug!("publish: {:?}", publication);

                self.topics.insert(id, topic.clone());
                self.publications.insert(publication);
                entry.insert(id);

                Ok(topic)
            }
        }
    }

    pub fn unpublish(&mut self,
                     query_id: QueryId,
                     topic: TopicId)
                     -> Result<(), UnpublishError> {
        let publication = Publication(query_id, topic);
        debug!("unpublish: {:?}", publication);

        if let Some(name) = self.topics.get(&topic).map(|t| &*t.name) {
            self.directory.remove(name);
        }

        self.topics.remove(&topic);
        self.publications.remove(&publication);
        Ok(())
    }

    pub fn lookup(&self, name: &str) -> Option<Topic> {
        if let Some(id) = self.directory.get(name) {
            self.topics.get(&id).cloned()
        } else {
            None
        }
    }

    pub fn subscribe(&mut self, query_id: QueryId, topic: TopicId) {
        let subscription = Subscription(query_id, topic);
        debug!("subscribe: {:?}", subscription);
        self.subscriptions.insert(subscription);
    }

    pub fn unsubscribe(&mut self,
                       query_id: QueryId,
                       topic: TopicId)
                       -> Result<(), UnsubscribeError> {
        let subscription = Subscription(query_id, topic);
        debug!("unsubscribe: {:?}", subscription);
        self.subscriptions.remove(&subscription);
        Ok(())
    }

    pub fn add_keeper(&mut self, keeper: Keeper) {
        self.keepers.insert(keeper.id, keeper);
    }

    pub fn add_keeper_worker(&mut self,
                             keeper_id: &KeeperId,
                             worker_num: usize,
                             addr: (String, u16)) -> Result<(), String> {
        let mut keeper = match self.keepers.remove(keeper_id) {
            Some(keeper) => keeper,
            None => return Err("No such Keeper".to_string()),
        };
        keeper.workers.push((worker_num, addr));
        self.keepers.insert(keeper.id, keeper);
        Ok(())
    }

    pub fn remove_keeper(&mut self, id: &KeeperId) -> Option<Keeper> {
        self.keepers.remove(id)
    }

    pub fn request(&self, req: RequestBuf<CatalogRPC>) -> io::Result<()> {
        match *req.name() {
            CatalogRPC::AllTopics => {
                let (_, resp) = req.decode::<AllTopics>()?;
                resp.respond(Ok(self.topics.values().cloned().collect()));
            },
            CatalogRPC::AllExecutors => {
                let (_, resp) = req.decode::<AllExecutors>()?;
                resp.respond(Ok(self.executors.values().cloned().collect()));
            },
            CatalogRPC::AllQueries => {
                let (_, resp) = req.decode::<AllQueries>()?;
                resp.respond(Ok(self.queries.values().cloned().collect()));
            },
            CatalogRPC::AllPublications => {
                let (_, resp) = req.decode::<AllPublications>()?;
                resp.respond(Ok(self.publications.iter().cloned().collect()));
            },
            CatalogRPC::AllSubscriptions => {
                let (_, resp) = req.decode::<AllSubscriptions>()?;
                resp.respond(Ok(self.subscriptions.iter().cloned().collect()));
            },
        }
        Ok(())
    }
}

pub struct Executors<'a> {
    inner: Values<'a, ExecutorId, Executor>,
}

impl<'a> Iterator for Executors<'a> {
    type Item = &'a Executor;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

pub struct Service {
    stream: mpsc::UnboundedReceiver<RequestBuf<CatalogRPC>>,
}

impl Service {
    pub fn new(network: &Network, handle: &Handle) -> io::Result<(Addr, Self)> {
        let server = network.server::<CatalogRPC, _>(None)?;
        let addr = {
            let (host, port) = server.external_addr();
            (host.to_string(), port)
        };

        let service_handle = handle.clone();
        let (sink, stream) = mpsc::unbounded();
        let service = server.for_each(move |(_, rx)| {
            let sink = sink.clone();
            // forward any request to the sink, to be returned by `Service`
            let sink = sink.sink_map_err(|_| io::ErrorKind::Other);
            let forwarder = rx.forward(sink).then(|res| {
                if let Err(err) = res {
                    error!("Catalog client error: {:?}", err);
                }
                Ok(())
            });

            service_handle.spawn(forwarder);
            Ok(())
        }).map_err(|err| {
            error!("Catalog server error: {:?}", err);
        });

        // run the service in the background
        handle.spawn(service);

        Ok((addr, Service { stream }))
    }
}

impl Stream for Service {
    type Item = RequestBuf<CatalogRPC>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.stream.poll()
    }
}
