use std::io::Error as IoError;

use timely::{Data};
use timely::progress::Timestamp;
use timely::dataflow::operators::Capability;

use futures::{Future, Poll, Async};
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

impl<D: Data + NonStatic> Stream for Subscription<D> {
    type Item = Vec<D>;
    type Error = IoError;
    
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.sub.poll()
    }
}

impl<D: Data + NonStatic> IntoIterator for Subscription<D> {
    type Item = Vec<D>;
    type IntoIter = IntoIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.wait()
        }
    }
}

impl<D: Data + NonStatic> Drop for Subscription<D> {
    fn drop(&mut self) {

    }
}

pub struct IntoIter<I> {
    inner: Wait<I>,
}

impl<S: Stream> Iterator for IntoIter<S> {
    type Item = S::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().and_then(Result::ok)
    }
}


// TODO(swicki) unsubscribe on drop
pub struct TimelySubscription<T: Timestamp + NonStatic, D: Data + NonStatic> {
    sub: TimelySubscriber<T, D>,
    topic: Topic,
    coord: Coordinator,
    frontier: Vec<Capability<T>>,
}

impl<T: Timestamp + NonStatic, D: Data + NonStatic> Stream for TimelySubscription<T, D> {
    type Item = (Capability<T>, Vec<D>);
    type Error = IoError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let next = try_ready!(self.sub.poll());
    
        let (frontier, time, data) = if next.is_some() {
            next.unwrap()
        } else {
            return Ok(Async::Ready(None));
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
        
        Ok(Async::Ready(Some((time, data))))
    }
}

impl<T: Timestamp + NonStatic, D: Data + NonStatic> IntoIterator for TimelySubscription<T, D> {
    type Item = (Capability<T>, Vec<D>);
    type IntoIter = IntoIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.wait()
        }
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
    fn timely<T, D>(&self, name: String, root: Capability<T>, blocking: bool) -> Result<TimelySubscription<T, D>, SubscriptionError>
        where T: Timestamp + NonStatic, D: Data + NonStatic {
        let name = name.to_string();
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
            let sub = TimelySubscriber::<T, D>::connect(&topic, &coord.network)?;
            Ok(TimelySubscription {
                sub: sub,
                topic: topic,
                coord: coord,
                frontier: vec![root],
            })
        }).wait()
    }

    pub fn subscribe<T, D>(&self, name: &str, root: Capability<T>) -> Result<TimelySubscription<T, D>, SubscriptionError>
        where T: Timestamp + NonStatic, D: Data + NonStatic {
        self.timely(name.to_string(), root, false)
    }

    pub fn subscribe_nonblocking<T, D>(&self, name: &str, root: Capability<T>) -> Result<TimelySubscription<T, D>, SubscriptionError>
            where T: Timestamp + NonStatic, D: Data + NonStatic {
        self.timely(name.to_string(), root, true)
    }

    fn collection<D>(&self, name: String, blocking: bool) -> Result<Subscription<D>, SubscriptionError>
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

    pub fn subscribe_collection<D>(&self, name: &str) -> Result<Subscription<D>, SubscriptionError>
        where D: Data + NonStatic
    {
        self.collection(name.to_string(), false)
    }

    pub fn subscribe_collection_nonblocking<D>(&self, name: &str) -> Result<Subscription<D>, SubscriptionError>
       where D: Data + NonStatic
    {
        self.collection(name.to_string(), true)
    }
}
