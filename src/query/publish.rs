use std::io::Error as IoError;

use timely::{Data};
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use futures::{Future, self};

use coordinator::requests::*;
use network::message::abomonate::NonStatic;

use pubsub::publisher::StreamPublisher;
use model::{Topic, TopicType};
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

        self.tx.request(&Publish {
            name: name.into(),
            token: self.token,
            kind: TopicType::of::<D>(),
            addr: addr,
        }).map_err(|err| {
            match err {
                Ok(err) => PublicationError::from(err),
                Err(err) => PublicationError::from(err),
            }
        }).wait()
    }
}
