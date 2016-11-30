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

impl Coordinator {
    pub fn publish<S, D>(&self, name: &str, stream: &Stream<S, D>, partition: Partition) -> Result<Stream<S, D>, PublicationError>
        where D: Data + NonStatic, S: Scope, S::Timestamp: NonStatic,
    {
        let worker_id = stream.scope().index() as u64;
        let name = match partition {
            Partition::PerWorker => {
                Some(format!("{}.{}", name, worker_id))
            }
            Partition::Merge if (worker_id == PUBLISH_WORKER_ID) => {
                Some(String::from(name))
            },
            _ => None,
        };

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
            self.tx.request(&Publish {
                name: name.unwrap(),
                token: self.token,
                schema: schema,
                addr: addr.unwrap(),
            }).map_err(|err| {
                match err {
                    Ok(err) => PublicationError::from(err),
                    Err(err) => PublicationError::from(err),
                }
            }).wait()?;
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
}
