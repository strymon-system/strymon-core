use std::io::Error as IoError;

use timely::{Data};
use futures::Future;
use futures::stream::{Stream, Wait};

use coordinator::requests::*;
use network::message::abomonate::NonStatic;

use pubsub::subscriber::Subscriber;
use model::{Topic, TopicType};
use query::Coordinator;

// TODO(swicki) impl drop
pub struct Subscription<D: Data + NonStatic> {
    sub: Subscriber<D>,
    topic: Topic,
    coord: Coordinator,
}

impl<D: Data + NonStatic> IntoIterator for Subscription<D> {
    type Item = Vec<D>;
    type IntoIter = IntoIter<D>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            sub: self.sub.wait(),
            topic: self.topic,
            coord: self.coord,
        }
    }
}

pub struct IntoIter<D: Data + NonStatic> {
    sub: Wait<Subscriber<D>>,
    topic: Topic,
    coord: Coordinator,
}

impl<D: Data + NonStatic> Iterator for IntoIter<D> {
    type Item = Vec<D>;

    fn next(&mut self) -> Option<Self::Item> {
        // we ignore any network errors here on purpose
        self.sub.next().and_then(Result::ok)
    }
}

#[derive(Debug)]
pub enum SubscriptionError {
    TopicNotFound,
    TypeIdMismatch,
    IoError(IoError)
}

impl From<SubscribeError> for SubscriptionError {
    fn from(err: SubscribeError) -> Self {
        match err {
            SubscribeError::TopicNotFound => SubscriptionError::TopicNotFound,
            err => panic!("failed to subscribe: {:?}", err),
        }
    }
}

impl From<IoError> for SubscriptionError {
    fn from(err: IoError) -> Self {
        SubscriptionError::IoError(err)
    }
}

impl Coordinator {
    fn do_subscribe<D>(&self, name: String, blocking: bool) -> Result<Subscription<D>, SubscriptionError>
        where D: Data + NonStatic {

        let coord = self.clone();
        self.tx.request(&Subscribe {
            name: name,
            token: self.token,
            blocking: blocking,
        }).map_err(|err| {
            match err {
                Ok(err) => SubscriptionError::from(err),
                Err(err) => SubscriptionError::from(err),
            }
        }).and_then(move |topic| {
            if topic.kind != TopicType::of::<D>() {
                Err(SubscriptionError::TypeIdMismatch)
            } else {
                let sub = {
                    let addr = (&*topic.addr.0, topic.addr.1);
                    Subscriber::<D>::connect(&addr, &coord.network)?
                };
                Ok(Subscription {
                    sub: sub,
                    topic: topic,
                    coord: coord,
                })
            }
        }).wait()
    }

    pub fn subscribe<D, N>(&self, name: N) -> Result<Subscription<D>, SubscriptionError>
        where N: Into<String>, D: Data + NonStatic {
        self.do_subscribe(name.into(), false)
    }

    pub fn blocking_subscribe<D, N>(&self, name: N) -> Result<Subscription<D>, SubscriptionError>
        where N: Into<String>, D: Data + NonStatic {
        self.do_subscribe(name.into(), true)
    }
}
