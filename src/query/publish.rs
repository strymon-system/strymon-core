use std::io::Error as IoError;

use timely::{Data};
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use futures::{Future, self};

use coordinator::requests::*;
use network::message::abomonate::NonStatic;

use pubsub::publisher::item::ItemPublisher as StreamPublisher;
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

impl Coordinator {
    // TODO(swicki): Merge contract
    pub fn publish<D, S, N>(&self, name: N, stream: &Stream<S, D>) -> Result<Topic, PublicationError>
        where D: Data + NonStatic, N: Into<String>, S: Scope
    {
        let (addr, publisher) = StreamPublisher::<D>::new(&self.network)?;

        let mut publisher = Some(publisher);

        stream.unary_stream::<D, _, _>(Pipeline, "publisher", move |input, output| {
            input.for_each(|time, data| {
                if let Some(ref mut publisher) = publisher {
                    publisher.publish(data).unwrap();
                }

                output.session(&time).give_content(data);
            });
        });

        let item = TopicType::of::<D>();
        let schema = TopicSchema::Item(item);
        self.tx.request(&Publish {
            name: name.into(),
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

    pub fn publish_timely<D, S, N>(&self, name: N, stream: &Stream<S, D>) -> Result<Topic, PublicationError>
        where D: Data + NonStatic, N: Into<String>, S: Scope, S::Timestamp: NonStatic,
    {
        let (addr, publisher) = TimelyPublisher::<S::Timestamp, D>::new(&self.network)?;

        let mut publisher = Some(publisher);

        stream.unary_notify(Pipeline, "timelypublisher", Vec::new(), move |input, output, notif| {
            let frontier = notif.frontier(0);
            input.for_each(|time, data| {
                if let Some(ref mut publisher) = publisher {
                    publisher.publish(frontier, &time, data).unwrap();
                }

                output.session(&time).give_content(data);
            });
        });

        let item = TopicType::of::<D>();
        let time = TopicType::of::<S::Timestamp>();
        let schema = TopicSchema::Timely(item, time);
        self.tx.request(&Publish {
            name: name.into(),
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
}
