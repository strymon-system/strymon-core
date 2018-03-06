// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The catalog contains meta-data about the current state of the system.
//!
//! It can be queried by jobs and external entities using the
//! [`CatalogRPC`](../strymon_rpc/coordinator/catalog/index.html) interface, which is exposed by
//! by the [`Service`](struct.Service.html). Incoming catalog requests are processed
//! by the [`Catalog::request`](struct.Catalog.html#method.request) method.
//! The other public methods are for use in the [`Coordinator`](../handler/struct.Coordinator.html)
//! implementation.

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

/// A representation of the current catalog state.
#[derive(Debug, Clone, Default)]
pub struct Catalog {
    generator: Generator<TopicId>,
    directory: HashMap<String, TopicId>,

    topics: HashMap<TopicId, Topic>,
    executors: HashMap<ExecutorId, Executor>,
    jobs: HashMap<JobId, Job>,

    publications: HashSet<Publication>,
    subscriptions: HashSet<Subscription>,
}

impl Catalog {
    /// Creates a new catalog instance, containing a self-referring topic.
    ///
    /// The created instance will contain a single topic announcing the address of
    /// the catalog itself (given through the `addr` argument). The created initial topic will
    /// have the name `"$catalog"` and is of schema `Service(CatalogRPC)`.
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

    /// Inserts a new available executor into the catalog.
    pub fn add_executor(&mut self, executor: Executor) {
        debug!("add_executor: {:?}", executor);
        self.executors.insert(executor.id, executor);
    }

    /// Removes an existing executor from the catalog.
    pub fn remove_executor(&mut self, id: ExecutorId) {
        debug!("remove_executor: {:?}", id);
        self.executors.remove(&id);
    }

    /// Returns an iterator over all existing executors.
    pub fn executors<'a>(&'a self) -> Executors<'a> {
        Executors { inner: self.executors.values() }
    }

    /// Adds a new completely spawned job.
    pub fn add_job(&mut self, job: Job) {
        debug!("add_job: {:?}", job);
        self.jobs.insert(job.id, job);
    }

    /// Removes a job from the catalog.
    pub fn remove_job(&mut self, id: JobId) {
        debug!("remove_job: {:?}", id);
        self.jobs.remove(&id);
    }

    /// Creates and announces a new topic.
    ///
    /// This fails if a topic with the same name already exists.
    pub fn publish(&mut self,
                   job: JobId,
                   name: String,
                   addr: (String, u16),
                   schema: TopicSchema)
                   -> Result<Topic, PublishError> {
        // TODO(swicki): Check if job actually exists
        match self.directory.entry(name.clone()) {
            Entry::Occupied(_) => Err(PublishError::TopicAlreadyExists),
            Entry::Vacant(entry) => {
                let id = self.generator.generate();
                let publication = Publication(job, id);
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

    /// Removes a publication from the catalog.
    pub fn unpublish(&mut self,
                     job_id: JobId,
                     topic: TopicId)
                     -> Result<(), UnpublishError> {
        let publication = Publication(job_id, topic);
        debug!("unpublish: {:?}", publication);

        if let Some(name) = self.topics.get(&topic).map(|t| &*t.name) {
            self.directory.remove(name);
        }

        self.topics.remove(&topic);
        self.publications.remove(&publication);
        Ok(())
    }

    /// Finds a topic by name.
    ///
    /// Returns `None` if the topic does not exist.
    pub fn lookup(&self, name: &str) -> Option<Topic> {
        if let Some(id) = self.directory.get(name) {
            self.topics.get(&id).cloned()
        } else {
            None
        }
    }

    /// Adds a new subscription to the catalog.
    pub fn subscribe(&mut self, job_id: JobId, topic: TopicId) {
        let subscription = Subscription(job_id, topic);
        debug!("subscribe: {:?}", subscription);
        self.subscriptions.insert(subscription);
    }

    /// Removes a subscription from the catalog.
    pub fn unsubscribe(&mut self,
                       job_id: JobId,
                       topic: TopicId)
                       -> Result<(), UnsubscribeError> {
        let subscription = Subscription(job_id, topic);
        debug!("unsubscribe: {:?}", subscription);
        self.subscriptions.remove(&subscription);
        Ok(())
    }

    /// Decodes and responds to an incoming catalog service request.
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
            CatalogRPC::AllJobs => {
                let (_, resp) = req.decode::<AllJobs>()?;
                resp.respond(Ok(self.jobs.values().cloned().collect()));
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

/// An iterator over all the executors of the catalog.
pub struct Executors<'a> {
    inner: Values<'a, ExecutorId, Executor>,
}

impl<'a> Iterator for Executors<'a> {
    type Item = &'a Executor;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// The service for the catalog.
///
/// Handles connection to incoming clients and forwards their `CatalogRPC` requests.
pub struct Service {
    stream: mpsc::UnboundedReceiver<RequestBuf<CatalogRPC>>,
}

impl Service {
    /// Creates a new service, returning the address of its endpoint.
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
