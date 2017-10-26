// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io::Error as IoError;

use timely::ExchangeData;
use timely::progress::Timestamp;
use timely::dataflow::channels::Content;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely_communication::{Allocate, Pull, Push};

use serde::ser::Serialize;
use futures::Future;

use query::{Coordinator, PubSubTimestamp};
use coordinator::requests::*;
use model::{Topic, TopicId, TopicType, TopicSchema};
use pubsub::publisher::timely::TimelyPublisher;
use pubsub::publisher::collection::CollectionPublisher;

#[derive(Debug)]
pub enum PublicationError {
    TopicAlreadyExists,
    TopicNotFound,
    AuthenticationFailure,
    TypeIdMismatch,
    IoError(IoError),
}

impl From<PublishError> for PublicationError {
    fn from(err: PublishError) -> Self {
        match err {
            PublishError::TopicAlreadyExists => PublicationError::TopicAlreadyExists,
            err => panic!("failed to publish: {:?}", err),
        }
    }
}

impl From<IoError> for PublicationError {
    fn from(err: IoError) -> Self {
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

#[derive(Debug, Copy, Clone)]
pub enum Partition {
    PerWorker,
    Merge,
}

const PUBLISH_WORKER_ID: u64 = 0;

impl<T: Timestamp, D: ExchangeData> ParallelizationContract<T, D> for Partition {
    type Pusher = Box<Push<(T, Content<D>)>>;
    type Puller = Box<Pull<(T, Content<D>)>>;

    fn connect<A: Allocate>
        (self,
         allocator: &mut A,
         identifier: usize)
         -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>) {
        match self {
            Partition::PerWorker => {
                let (push, pull) = Pipeline.connect(allocator, identifier);
                (Box::new(push), Box::new(pull))
            }
            Partition::Merge => {
                let (push, pull) = Exchange::new(|_| PUBLISH_WORKER_ID).connect(allocator, identifier);
               (push, Box::new(pull))
            }
        }
    }
}

impl Partition {
    fn name(&self, name: &str, worker_id: u64) -> Option<String> {
        match *self {
            Partition::PerWorker => Some(format!("{}.{}", name, worker_id)),
            Partition::Merge if (worker_id == PUBLISH_WORKER_ID) => {
                Some(String::from(name))
            }
            _ => None,
        }
    }
}

struct Publication {
    topic: Topic,
    coord: Coordinator,
}

impl Drop for Publication {
    fn drop(&mut self) {
        if let Err(err) = self.coord.unpublish(self.topic.id) {
            warn!("failed to unpublish: {:?}", err)
        }
    }
}

impl Coordinator {
    fn publish_request(&self,
                       name: String,
                       schema: TopicSchema,
                       addr: (String, u16))
                       -> Result<Publication, PublicationError> {
        let topic = self.tx
            .request(&Publish {
                name: name,
                token: self.token,
                schema: schema,
                addr: addr,
            })
            .map_err(PublicationError::from)
            .wait()?;

        Ok(Publication {
            topic: topic,
            coord: self.clone(),
        })
    }

    pub fn publish<S, D>(&self,
                         name: &str,
                         stream: &Stream<S, D>,
                         partition: Partition)
                         -> Result<Stream<S, D>, PublicationError>
        where D: ExchangeData + Serialize,
              S: Scope,
              S::Timestamp: PubSubTimestamp
    {
        let worker_id = stream.scope().index() as u64;
        let name = partition.name(name, worker_id);

        let (addr, mut publisher) = if name.is_some() {
            let (addr, publisher) =
                TimelyPublisher::<<S::Timestamp as PubSubTimestamp>::Converted, D>::new(&self.network)?;
            (Some(addr), Some(publisher))
        } else {
            (None, None)
        };

        let publication = if name.is_some() {
            // local worker hosts a publication
            let item = TopicType::of::<D>();
            let time = TopicType::of::<S::Timestamp>();
            let schema = TopicSchema::Stream(item, time);

            Some(self.publish_request(name.unwrap(), schema, addr.unwrap())?)
        } else {
            None
        };

        let output = stream.unary_notify(partition,
                                         "timelypublisher",
                                         Vec::new(),
                                         move |input, output, notif| {
            // ensure publication handle is moved into the closure/operator
            let ref _guard = publication;

            // publish data on input
            let frontier: Vec<_> = notif
                .frontier(0).iter()
                .map(|t| t.to_pubsub())
                .collect();
            input.for_each(|time, data| {
                let pubsub_time = time.time().to_pubsub();
                if let Some(ref mut publisher) = publisher {
                    publisher.publish(&frontier, &pubsub_time, data).unwrap();
                }
                output.session(&time).give_content(data);
            });
        });

        Ok(output)
    }

    pub fn publish_collection<S, D>(&self,
                                    name: &str,
                                    stream: &Stream<S, (D, i32)>,
                                    partition: Partition)
                                    -> Result<Stream<S, (D, i32)>, PublicationError>
        where D: ExchangeData + Eq + Serialize,
              S: Scope
    {
        let worker_id = stream.scope().index() as u64;
        let name = partition.name(name, worker_id);

        let (addr, mut mutator, mut publisher) = if name.is_some() {
            let (addr, mutator, publisher) =
                CollectionPublisher::<D>::new(&self.network)?;
            (Some(addr), Some(mutator), Some(publisher.spawn()))
        } else {
            (None, None, None)
        };

        let publication = if name.is_some() {
            // local worker hosts a publication
            let item = TopicType::of::<D>();
            let schema = TopicSchema::Collection(item);
            Some(self.publish_request(name.unwrap(), schema, addr.unwrap())?)
        } else {
            None
        };

        let output =
            stream.unary_stream(partition, "collectionpublisher", move |input, output| {
                // ensure publication handle is moved into the closure/operator
                let ref _guard = publication;

                // publication logic
                input.for_each(|time, data| {
                    if let Some(ref mut mutator) = mutator {
                        mutator.update_from(data.drain(..).collect());
                    }
                    output.session(&time).give_content(data);
                });

                // ensure publisher future is polled
                if let Some(ref mut publisher) = publisher {
                    publisher.poll().unwrap();
                }
            });

        Ok(output)
    }

    fn unpublish(&self, topic: TopicId) -> Result<(), PublicationError> {
        self.tx
            .request(&Unpublish {
                topic: topic,
                token: self.token,
            })
            .map_err(PublicationError::from)
            .wait()
    }
}
