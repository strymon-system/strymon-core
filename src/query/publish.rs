use std::io::Error as IoError;

use timely::Data;
use timely::progress::Timestamp;
use timely::dataflow::channels::Content;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::{Exchange, ParallelizationContract, Pipeline};
use timely_communication::{Allocate, Pull, Push};
use futures::{Future, self};

use coordinator::requests::*;
use network::message::abomonate::NonStatic;

use pubsub::publisher::timely::TimelyPublisher;
use pubsub::publisher::collection::CollectionPublisher;
use model::{Topic, TopicType, TopicSchema};
use query::Coordinator;

#[derive(Debug)]
pub enum PublicationError {
    TopicAlreadyExists,
    TypeIdMismatch,
    IoError(IoError)
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

#[derive(Debug, Copy, Clone)]
pub enum Partition {
    PerWorker,
    Merge,
}

const PUBLISH_WORKER_ID: u64 = 0;

impl<T: Timestamp, D: Data> ParallelizationContract<T, D> for Partition
{
    fn connect<A: Allocate>(self,
                            allocator: &mut A,
                            identifier: usize)
                            -> (Box<Push<(T, Content<D>)>>, Box<Pull<(T, Content<D>)>>) {
        match self {
            Partition::PerWorker => Pipeline.connect(allocator, identifier),
            Partition::Merge => Exchange::new(|_| PUBLISH_WORKER_ID).connect(allocator, identifier),
        }
    }
}

impl Partition {
    fn name(&self, name: &str, worker_id: u64) -> Option<String> {
        match *self {
            Partition::PerWorker => {
                Some(format!("{}.{}", name, worker_id))
            }
            Partition::Merge if (worker_id == PUBLISH_WORKER_ID) => {
                Some(String::from(name))
            },
            _ => None,
        }
    }
}

impl Coordinator {
    fn publish_request(&self, name: String, schema: TopicSchema, addr: (String, u16)) -> Result<Topic, PublicationError> {
        self.tx.request(&Publish {
            name: name,
            token: self.token,
            schema: schema,
            addr: addr,
        }).map_err(|err| {
            match err {
                Ok(err) => PublicationError::from(err),
                Err(err) => PublicationError::from(err),
            }
        }).wait()
    }

    pub fn publish<S, D>(&self, name: &str, stream: &Stream<S, D>, partition: Partition) -> Result<Stream<S, D>, PublicationError>
        where D: Data + NonStatic, S: Scope, S::Timestamp: NonStatic,
    {
        let worker_id = stream.scope().index() as u64;
        let name = partition.name(name, worker_id);

        let (addr, mut publisher) = if name.is_some() {
            let (addr, publisher) = TimelyPublisher::<S::Timestamp, D>::new(&self.network)?;
            (Some(addr), Some(publisher))
        } else {
            (None, None)
        };

        if name.is_some() {
            // local worker hosts a publication
            let item = TopicType::of::<D>();
            let time = TopicType::of::<S::Timestamp>();
            let schema = TopicSchema::Stream(item, time);
            self.publish_request(name.unwrap(), schema, addr.unwrap())?;
        }

        let output = stream.unary_notify(partition, "timelypublisher", Vec::new(), move |input, output, notif| {
            let frontier = notif.frontier(0);
            input.for_each(|time, data| {
                if let Some(ref mut publisher) = publisher {
                    publisher.publish(frontier, &time, data).unwrap();
                }
                output.session(&time).give_content(data);
            });
        });
        
        Ok(output)
    }

    pub fn publish_collection<S, D>(&self, name: &str, stream: &Stream<S, (D, i32)>, partition: Partition) -> Result<Stream<S, (D, i32)>, PublicationError>
        where D: Data + Eq + NonStatic, S: Scope
    {
        let worker_id = stream.scope().index() as u64;
        let name = partition.name(name, worker_id);

        let (addr, mut mutator, mut publisher) = if name.is_some() {
            let (addr, mutator, publisher) = CollectionPublisher::<D>::new(&self.network)?;
            (Some(addr), Some(mutator), Some(publisher.spawn()))
        } else {
            (None, None, None)
        };

        if name.is_some() {
            // local worker hosts a publication
            let item = TopicType::of::<D>();
            let schema = TopicSchema::Collection(item);
            self.publish_request(name.unwrap(), schema, addr.unwrap())?;
        }

        let output = stream.unary_stream(partition, "collectionpublisher", move |input, output| {
            input.for_each(|time, data| {
                if let Some(ref mut mutator) = mutator {
                    mutator.update_from(data.clone().into_typed());
                }
                output.session(&time).give_content(data);
            });
            
            if let Some(ref mut publisher) = publisher {
                publisher.poll().unwrap();
            }
        });

        Ok(output)
    }
}
