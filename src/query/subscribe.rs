use std::io::Error as IoError;

use timely::{Data};
use futures::Future;
use futures::stream::{Stream, Wait};

use coordinator::requests::*;
use network::message::abomonate::NonStatic;

use pubsub::subscriber::{Subscriber, TimelySubscriber};
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

use timely::progress::Timestamp;
use timely::dataflow::operators::Capability;

// TODO(swicki) unsubscribe on drop
pub struct TimelySubscription<T: Timestamp + NonStatic, D: Data + NonStatic> {
    sub: Wait<TimelySubscriber<T, D>>,
    topic: Topic,
    coord: Coordinator,
    frontier: Vec<Capability<T>>,
}

impl<T: Timestamp + NonStatic, D: Data + NonStatic> Iterator for TimelySubscription<T, D> {
    type Item = (Capability<T>, Vec<D>);

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.sub.next().and_then(Result::ok);
        let (frontier, time, data) = if next.is_some() {
            next.unwrap()
        } else {
            return None;
        };

        let mut time_cap = None;
        let mut new_frontier = vec![];
        for cap in self.frontier.iter() {
            // get capability for resulting tuple
            if time_cap.is_none() && time >= cap.time() {
                time_cap = Some(cap.delayed(&time));
            }
            
            // upgrade capability for new frontier
            for t in frontier.iter() {
                if *t >= cap.time() {
                    new_frontier.push(cap.delayed(t));
                }
            }
        };
        
        self.frontier = new_frontier;
        let time = time_cap.expect("failed to get capability for tuple");
        
        Some((time, data))
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

    pub fn subscribe<T, D>(&self, name: &str, root: Capability<T>) -> Result<TimelySubscription<T, D>, SubscriptionError>
        where T: Timestamp + NonStatic, D: Data + NonStatic {
        let name = name.to_string();
        let coord = self.clone();
        self.tx.request(&Subscribe {
            name: name,
            token: self.token,
            blocking: true,
        }).map_err(|err| {
            match err {
                Ok(err) => SubscriptionError::from(err),
                Err(err) => SubscriptionError::from(err),
            }
        }).and_then(move |topic| {
            let sub = TimelySubscriber::<T, D>::connect(&topic, &coord.network)?;
            Ok(TimelySubscription {
                sub: sub.wait(),
                topic: topic,
                coord: coord,
                frontier: vec![root],
            })
        }).wait()
    }

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
            let sub = Subscriber::<D>::connect(&topic, &coord.network)?;
            Ok(Subscription {
                sub: sub,
                topic: topic,
                coord: coord,
            })
        }).wait()
    }

    pub fn subscribe_collection<D, N>(&self, name: N) -> Result<Subscription<D>, SubscriptionError>
        where N: Into<String>, D: Data + NonStatic {
        self.do_subscribe(name.into(), false)
    }

    pub fn blocking_subscribe_item<D, N>(&self, name: N) -> Result<Subscription<D>, SubscriptionError>
        where N: Into<String>, D: Data + NonStatic {
        self.do_subscribe(name.into(), true)
    }
}
