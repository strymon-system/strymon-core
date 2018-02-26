// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Types used for managing publications.

use std::io;

use timely::ExchangeData;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::dataflow::operators::{Capture, Exchange};
use timely::dataflow::operators::capture::{Event, EventPusher};

use serde::ser::Serialize;
use futures::Future;
use typename::TypeName;

use strymon_model::{Topic, TopicId, TopicType, TopicSchema};
use strymon_rpc::coordinator::*;

use Coordinator;
use protocol::RemoteTimestamp;
use publisher::Publisher;

/// Failure states of a publication.
#[derive(Debug)]
pub enum PublicationError {
    /// A topic with the same name already exists.
    TopicAlreadyExists,
    /// Tried to unpublish a non-existing topic.
    TopicNotFound,
    /// Tried to unpublish a topic not owned by the current job.
    AuthenticationFailure,
    /// Networking error occured.
    IoError(io::Error),
}

impl From<PublishError> for PublicationError {
    fn from(err: PublishError) -> Self {
        match err {
            PublishError::TopicAlreadyExists => PublicationError::TopicAlreadyExists,
            err => panic!("failed to publish: {:?}", err), // auth failure?!
        }
    }
}

impl From<io::Error> for PublicationError {
    fn from(err: io::Error) -> Self {
        PublicationError::IoError(err)
    }
}

impl From<UnpublishError> for PublicationError {
    fn from(err: UnpublishError) -> Self {
        match err {
            UnpublishError::InvalidTopicId => PublicationError::TopicNotFound,
            UnpublishError::AuthenticationFailure => {
                PublicationError::AuthenticationFailure
            }
        }
    }
}

impl<T, E> From<Result<T, E>> for PublicationError
    where T: Into<PublicationError>,
          E: Into<PublicationError>
{
    fn from(err: Result<T, E>) -> Self {
        match err {
            Ok(err) => err.into(),
            Err(err) => err.into(),
        }
    }
}

/// Marker to specify topic partitioning.
///
/// A single logical Timely Dataflow stream will have a partition on each worker.
/// When publishing a logical stream, the publishing job can decide to either
/// publish each stream partition indivdually, or have them be merged by a
/// single worker, resulting in a single topic.
#[derive(Debug, Copy, Clone)]
pub enum Partition {
    /// Publish one topic per worker.
    ///
    /// The resulting topic name will have the local worker identifier as suffix,
    /// e.g. `name.1` for the stream produced by worker 1.
    PerWorker,
    /// Merge all streams and publish a single topic.
    ///
    /// No worker identifier is appended to the name.
    Merge,
}

/// By default, worker 0 publish the merged topic
const PUBLISH_WORKER_ID: u64 = 0;

impl Partition {
    /// If the partition settings results in a publication by the local worker,
    /// return the name of the published topic.
    fn name(&self, name: &str, worker_id: u64) -> Option<String> {
        match *self {
            Partition::PerWorker => Some(format!("{}.{}", name, worker_id)),
            Partition::Merge if worker_id == PUBLISH_WORKER_ID => {
                Some(String::from(name))
            }
            _ => None,
        }
    }
}

/// A publication is a publisher + a topic in the catalog.
///
/// This type will unpublish the topic when dropped.
enum Publication<T, D> {
    /// Publication exists on local worker.
    Local(Topic, Coordinator, Publisher<T, D>),
    /// The publication is on a remote worker.
    Remote,
}

impl<T, D> EventPusher<T, D> for Publication<T, D>
    where T: RemoteTimestamp, D: ExchangeData + Serialize
{
    fn push(&mut self, event: Event<T, D>) {
        if let Publication::Local(_, _, ref mut publisher) = *self {
            publisher.push(event)
        }
    }
}

impl<T, D> Drop for Publication<T, D> {
    fn drop(&mut self) {
        if let Publication::Local(ref topic, ref coord, _) = *self {
            if let Err(err) = coord.unpublish(topic.id) {
                warn!("failed to unpublish: {:?}", err)
            }
        }
    }
}

impl Coordinator {
    /// Submit a publication request to the coordinator and block.
    pub(crate) fn publish_request(&self,
                       name: String,
                       schema: TopicSchema,
                       addr: (String, u16))
                       -> Result<Topic, PublicationError>
    {
        self.tx
            .request(&Publish {
                name: name,
                token: self.token,
                schema: schema,
                addr: addr,
            })
            .map_err(PublicationError::from)
            .wait()
    }

    /// Publishes a local stream and creates a topic in the catalog.
    ///
    /// This will block the current worker until the coordinator has processed
    /// the publication request.
    ///
    /// Each published stream must have a globally unique name, which used to
    /// create a topic in the catalog. This method injects a publisher operator
    /// into the Timely dataflow which forwards all data and progress messages
    /// put into `stream`. The created topic is deregistered when the frontier
    /// of the input stream becomes empty. Upon deregisteration, the current
    /// worker is blocked until the queues of any still connected subscribers
    /// are drained.
    ///
    /// If the `Partition::Merge` strategy is used, a single topic is created
    /// whose name is specified in `name`. If a `Partition::PerWorker` partitioning
    /// scheme is used, one topic is created for each worker, with the worker's
    /// index appended, e.g. `foobar.1`.
    pub fn publish<S, D>(&self,
                         name: &str,
                         stream: &Stream<S, D>,
                         partition: Partition)
                         -> Result<Stream<S, D>, PublicationError>
        where D: ExchangeData + Serialize + TypeName,
              S: Scope,
              S::Timestamp: RemoteTimestamp,
              <S::Timestamp as RemoteTimestamp>::Remote: TypeName
    {
        // if we have an assigned topic name, we need to create a publisher
        let worker_id = stream.scope().index() as u64;
        let publication = if let Some(name) = partition.name(name, worker_id) {
            let (addr, publisher) = Publisher::<S::Timestamp, D>::new(&self.network)?;
            let item = TopicType::of::<D>();
            let time = TopicType::of::<<S::Timestamp as RemoteTimestamp>::Remote>();
            let schema = TopicSchema::Stream(item, time);

            // announce the publication to the coordinator
            let topic = self.publish_request(name, schema, addr)?;
            Publication::Local(topic, self.clone(), publisher)
        } else {
            Publication::Remote
        };

        let stream = if let Partition::Merge = partition {
            stream.exchange(|_| PUBLISH_WORKER_ID)
        } else {
            stream.clone()
        };

        stream.capture_into(publication);

        Ok(stream)
    }

    /// Submits a depublication request.
    pub(crate) fn unpublish(&self, topic: TopicId) -> Result<(), PublicationError> {
        self.tx
            .request(&Unpublish {
                topic: topic,
                token: self.token,
            })
            .map_err(PublicationError::from)
            .wait()
    }
}
