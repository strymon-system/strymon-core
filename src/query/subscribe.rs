use std::io::Error as IoError;

use timely::Data;
use timely::progress::Timestamp;
use timely::dataflow::operators::Capability;

use futures::{Future, Poll, Async};
use futures::stream::{Stream, Wait};

use coordinator::requests::*;
use network::message::abomonate::NonStatic;

use pubsub::subscriber::{Subscriber, TimelySubscriber};
use model::{Topic, TopicId};
use query::Coordinator;

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
        IntoIter { inner: self.wait() }
    }
}

impl<D: Data + NonStatic> Drop for Subscription<D> {
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
        }

        self.frontier = new_frontier;
        let time = time_cap.expect("failed to get capability for tuple");

        Ok(Async::Ready(Some((time, data))))
    }
}

impl<T, D> IntoIterator for TimelySubscription<T, D>
    where T: Timestamp + NonStatic,
          D: Data + NonStatic
{
    type Item = (Capability<T>, Vec<D>);
    type IntoIter = IntoIter<Self>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { inner: self.wait() }
    }
}

impl<T: Timestamp + NonStatic, D: Data + NonStatic> Drop for TimelySubscription<T, D> {
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
    IoError(IoError),
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

impl From<IoError> for SubscriptionError {
    fn from(err: IoError) -> Self {
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
        where T: Timestamp + NonStatic,
              D: Data + NonStatic
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
                if !topic.schema.is_stream::<T, D>() {
                    return Err(SubscriptionError::TypeIdMismatch);
                }

                let sub = TimelySubscriber::<T, D>::connect(&topic, &coord.network)?;
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
        where T: Timestamp + NonStatic,
              D: Data + NonStatic
    {
        self.timely(name.to_string(), root, true)
    }

    pub fn subscribe_nonblocking<T, D>
        (&self,
         name: &str,
         root: Capability<T>)
         -> Result<TimelySubscription<T, D>, SubscriptionError>
        where T: Timestamp + NonStatic,
              D: Data + NonStatic
    {
        self.timely(name.to_string(), root, false)
    }

    fn collection<D>(&self,
                     name: String,
                     blocking: bool)
                     -> Result<Subscription<D>, SubscriptionError>
        where D: Data + NonStatic
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
                if !topic.schema.is_collection::<D>() {
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
        where D: Data + NonStatic
    {
        self.collection(name.to_string(), true)
    }

    pub fn subscribe_collection_nonblocking<D>
        (&self,
         name: &str)
         -> Result<Subscription<D>, SubscriptionError>
        where D: Data + NonStatic
    {
        self.collection(name.to_string(), false)
    }
}
