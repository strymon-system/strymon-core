use std::io;

use timely::Data;
use timely::dataflow::operators::Capability;

use futures::{Future, Poll, Async};
use futures::stream::{Stream, Wait};

use serde::de::DeserializeOwned;

use coordinator::requests::*;

use pubsub::subscriber::{Subscriber, TimelySubscriber};
use model::{Topic, TopicId};
use query::{Coordinator, PubSubTimestamp};

pub struct Subscription<D: Data + DeserializeOwned> {
    sub: Subscriber<D>,
    topic: Topic,
    coord: Coordinator,
}

impl<D: Data + DeserializeOwned> Stream for Subscription<D> {
    type Item = Vec<D>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.sub.poll()
    }
}

impl<D: Data + DeserializeOwned> IntoIterator for Subscription<D> {
    type Item = Vec<D>;
    type IntoIter = IntoIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { inner: self.wait() }
    }
}

impl<D: Data + DeserializeOwned> Drop for Subscription<D> {
    fn drop(&mut self) {
        if let Err(err) = self.coord.unsubscribe(self.topic.id) {
            warn!("failed to unsubscribe: {:?}", err)
        }
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


pub struct TimelySubscription<T: PubSubTimestamp, D: Data + DeserializeOwned> {
    sub: TimelySubscriber<T::Converted, D>,
    topic: Topic,
    coord: Coordinator,
    frontier: Vec<Capability<T>>,
}

impl<T: PubSubTimestamp, D: Data + DeserializeOwned> Stream for TimelySubscription<T, D> {
    type Item = (Capability<T>, Vec<D>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let next = try_ready!(self.sub.poll());

        let (frontier, time, data) = if next.is_some() {
            next.unwrap()
        } else {
            return Ok(Async::Ready(None));
        };

        // convert frontier and timestamp
        let time = PubSubTimestamp::from_pubsub(time);
        let frontier: Vec<_> = frontier.into_iter()
            .map(PubSubTimestamp::from_pubsub)
            .collect();

        let mut time_cap = None;
        let mut new_frontier = vec![];
        for cap in self.frontier.iter() {
            // get capability for resulting tuple
            if time_cap.is_none() && cap.time().less_equal(&time) {
                time_cap = Some(cap.delayed(&time));
            }

            // upgrade capability for new frontier
            for t in frontier.iter() {
                if cap.time().less_equal(t) {
                    new_frontier.push(cap.delayed(t));
                }
            }
        }

        self.frontier = new_frontier;
        let time = time_cap.expect("failed to get capability for tuple");

        Ok(Async::Ready(Some((time, data))))
    }
}

impl<T, D> IntoIterator for TimelySubscription<T, D>
    where T: PubSubTimestamp,
          D: Data + DeserializeOwned
{
    type Item = (Capability<T>, Vec<D>);
    type IntoIter = IntoIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { inner: self.wait() }
    }
}

impl<T: PubSubTimestamp, D: Data + DeserializeOwned> Drop for TimelySubscription<T, D> {
    fn drop(&mut self) {
        if let Err(err) = self.coord.unsubscribe(self.topic.id) {
            warn!("failed to unsubscribe: {:?}", err)
        }
    }
}

#[derive(Debug)]
pub enum SubscriptionError {
    TopicNotFound,
    TypeIdMismatch,
    AuthenticationFailure,
    IoError(io::Error),
}

impl From<SubscribeError> for SubscriptionError {
    fn from(err: SubscribeError) -> Self {
        match err {
            SubscribeError::TopicNotFound => SubscriptionError::TopicNotFound,
            SubscribeError::AuthenticationFailure => {
                SubscriptionError::AuthenticationFailure
            }
        }
    }
}

impl From<UnsubscribeError> for SubscriptionError {
    fn from(err: UnsubscribeError) -> Self {
        match err {
            UnsubscribeError::InvalidTopicId => SubscriptionError::TopicNotFound,
            UnsubscribeError::AuthenticationFailure => {
                SubscriptionError::AuthenticationFailure
            }
        }
    }
}

impl From<io::Error> for SubscriptionError {
    fn from(err: io::Error) -> Self {
        SubscriptionError::IoError(err)
    }
}

impl<T, E> From<Result<T, E>> for SubscriptionError
    where T: Into<SubscriptionError>,
          E: Into<SubscriptionError>
{
    fn from(err: Result<T, E>) -> Self {
        match err {
            Ok(err) => err.into(),
            Err(err) => err.into(),
        }
    }
}

impl Coordinator {
    fn unsubscribe(&self, topic: TopicId) -> Result<(), SubscriptionError> {
        self.tx
            .request(&Unsubscribe {
                topic: topic,
                token: self.token,
            })
            .map_err(SubscriptionError::from)
            .wait()
    }

    fn timely<T, D>(&self,
                    name: String,
                    root: Capability<T>,
                    blocking: bool)
                    -> Result<TimelySubscription<T, D>, SubscriptionError>
        where T: PubSubTimestamp,
              D: Data + DeserializeOwned
    {
        let name = name.to_string();
        let coord = self.clone();
        self.tx
            .request(&Subscribe {
                name: name,
                token: self.token,
                blocking: blocking,
            })
            .map_err(SubscriptionError::from)
            .and_then(move |topic| {
                if !topic.schema.is_stream() {
                    return Err(SubscriptionError::TypeIdMismatch);
                }

                let sub = TimelySubscriber::<T::Converted, D>::connect(&topic, &coord.network)?;
                Ok(TimelySubscription {
                    sub: sub,
                    topic: topic,
                    coord: coord,
                    frontier: vec![root],
                })
            })
            .wait()
    }

    pub fn subscribe<T, D>(&self,
                           name: &str,
                           root: Capability<T>)
                           -> Result<TimelySubscription<T, D>, SubscriptionError>
        where T: PubSubTimestamp,
              D: Data + DeserializeOwned
    {
        self.timely(name.to_string(), root, true)
    }

    pub fn subscribe_nonblocking<T, D>
        (&self,
         name: &str,
         root: Capability<T>)
         -> Result<TimelySubscription<T, D>, SubscriptionError>
        where T: PubSubTimestamp,
              D: Data + DeserializeOwned
    {
        self.timely(name.to_string(), root, false)
    }

    fn collection<D>(&self,
                     name: String,
                     blocking: bool)
                     -> Result<Subscription<D>, SubscriptionError>
        where D: Data + DeserializeOwned
    {

        let coord = self.clone();
        self.tx
            .request(&Subscribe {
                name: name,
                token: self.token,
                blocking: blocking,
            })
            .map_err(SubscriptionError::from)
            .and_then(move |topic| {
                if !topic.schema.is_collection() {
                    return Err(SubscriptionError::TypeIdMismatch);
                }

                let sub = Subscriber::<D>::connect(&topic, &coord.network)?;
                Ok(Subscription {
                    sub: sub,
                    topic: topic,
                    coord: coord,
                })
            })
            .wait()
    }

    pub fn subscribe_collection<D>(&self,
                                   name: &str)
                                   -> Result<Subscription<D>, SubscriptionError>
        where D: Data + DeserializeOwned
    {
        self.collection(name.to_string(), true)
    }

    pub fn subscribe_collection_nonblocking<D>
        (&self,
         name: &str)
         -> Result<Subscription<D>, SubscriptionError>
        where D: Data + DeserializeOwned
    {
        self.collection(name.to_string(), false)
    }
}
