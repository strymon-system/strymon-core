// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Types for reading from topic subscriptions.

use std::io;

use timely::progress::Timestamp;

use futures::{Future, Poll};
use futures::stream::{Stream, Wait, futures_ordered};

use typename::TypeName;
use serde::de::DeserializeOwned;

use strymon_rpc::coordinator::*;
use strymon_model::{Topic, TopicId, TopicType, TopicSchema};

use strymon_communication::transport::{Sender, Receiver};

use Coordinator;
use protocol::RemoteTimestamp;
use subscriber::SubscriberGroup;
pub use subscriber::SubscriptionEvent;

/// A subscription handle used to receive data from a topic.
///
/// A subscription can be optained by calling the `Coordinator::subscribe` method.
/// The subscription can be read by using the type as an iterator, e.g. in a `for`
/// loop. To inspect the frontier of an subscription, e.g. to manage capabilities,
/// use the `frontier()` method.
///
/// In addition, this type also supports the asynchronous `futures::stream::Stream` trait,
/// allowing users to block on multiple topics at once.
pub struct Subscription<T: Timestamp, D> {
    sub: SubscriberGroup<T, D>,
    topics: Vec<Topic>,
    coord: Coordinator,
}

impl<T, D> Subscription<T, D>
where
    T: RemoteTimestamp,
    D: DeserializeOwned,
{
    /// The current frontier at the subscribed topics.
    pub fn frontier(&self) -> &[T] {
        self.sub.frontier()
    }
}

impl<T, D> Stream for Subscription<T, D>
where
    T: RemoteTimestamp,
    D: DeserializeOwned,
{
    type Item = SubscriptionEvent<T, D>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.sub.poll()
    }
}

/// A blocking iterator, yielding `io::Result<T, Vec<D>)>` tuples.
pub struct IntoIter<T: Timestamp, D> {
    inner: Wait<Subscription<T, D>>,
}

impl<T, D> IntoIter<T, D>
where
    T: RemoteTimestamp,
    D: DeserializeOwned,
{
    /// The current frontier at the subscribed topics.
    pub fn frontier(&self) -> &[T] {
        self.inner.get_ref().frontier()
    }
}

impl<T, D> IntoIterator for Subscription<T, D>
where
    T: RemoteTimestamp,
    D: DeserializeOwned,
{
    type Item = io::Result<SubscriptionEvent<T, D>>;
    type IntoIter = IntoIter<T, D>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { inner: self.wait() }
    }
}

impl<T, D> Iterator for IntoIter<T, D>
where
    T: RemoteTimestamp,
    D: DeserializeOwned,
{
    type Item = io::Result<SubscriptionEvent<T, D>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<T: Timestamp, D> Drop for Subscription<T, D> {
    fn drop(&mut self) {
        for topic in self.topics.iter() {
            if let Err(err) = self.coord.unsubscribe(topic.id) {
                error!("failed to unsubscribe from {:?}: {:?}", topic, err)
            }
        }
    }
}

/// Failure states when subscribting to a topic.
#[derive(Debug)]
pub enum SubscriptionError {
    /// The requested topic does not exist.
    TopicNotFound,
    /// The requested topic exists, but its timestamp or data type does not match.
    TypeMismatch,
    /// The current job does is not authenticated to subscribe to topics.
    AuthenticationFailure,
    /// A networking error occured.
    IoError(io::Error),
}

impl From<SubscribeError> for SubscriptionError {
    fn from(err: SubscribeError) -> Self {
        match err {
            SubscribeError::TopicNotFound => SubscriptionError::TopicNotFound,
            SubscribeError::AuthenticationFailure => SubscriptionError::AuthenticationFailure,
        }
    }
}

impl From<UnsubscribeError> for SubscriptionError {
    fn from(err: UnsubscribeError) -> Self {
        match err {
            UnsubscribeError::InvalidTopicId => SubscriptionError::TopicNotFound,
            UnsubscribeError::AuthenticationFailure => SubscriptionError::AuthenticationFailure,
        }
    }
}

impl From<io::Error> for SubscriptionError {
    fn from(err: io::Error) -> Self {
        SubscriptionError::IoError(err)
    }
}

impl<T, E> From<Result<T, E>> for SubscriptionError
where
    T: Into<SubscriptionError>,
    E: Into<SubscriptionError>,
{
    fn from(err: Result<T, E>) -> Self {
        match err {
            Ok(err) => err.into(),
            Err(err) => err.into(),
        }
    }
}

impl Coordinator {
    /// Unsubscribes from a topic.
    pub(crate) fn unsubscribe(&self, topic: TopicId) -> Result<(), SubscriptionError> {
        self.tx
            .request(&Unsubscribe {
                topic: topic,
                token: self.token,
            })
            .map_err(SubscriptionError::from)
            .wait()
    }


    pub(crate) fn subscribe_request(
        &self,
        name: &str,
        blocking: bool,
    ) -> Result<Topic, SubscriptionError> {
        self.tx
            .request(&Subscribe {
                name: name.to_string(),
                token: self.token,
                blocking: blocking,
            })
            .map_err(SubscriptionError::from)
            .wait()
    }

    fn request_multiple<I>(&self, names: I, blocking: bool) -> Result<Vec<Topic>, SubscriptionError>
    where
        I: IntoIterator,
        I::Item: ToString,
    {
        let pending = names.into_iter().map(|n| {
            self.tx
                .request(&Subscribe {
                    name: n.to_string(),
                    token: self.token,
                    blocking: blocking,
                })
                .map_err(SubscriptionError::from)
        });

        futures_ordered(pending).collect().wait()
    }

    fn connect_all<T, D>(
        &self,
        topics: &[Topic],
    ) -> Result<Vec<(Sender, Receiver)>, SubscriptionError>
    where
        T: RemoteTimestamp,
        D: DeserializeOwned + TypeName,
        T::Remote: TypeName,
    {
        topics
            .iter()
            .map(|topic| {
                let item = TopicType::of::<D>();
                let time = TopicType::of::<T::Remote>();
                let schema = TopicSchema::Stream(item, time);
                if topic.schema != schema {
                    Err(SubscriptionError::TypeMismatch)
                } else {
                    self.network
                        .connect((&*topic.addr.0, topic.addr.1))
                        .map_err(SubscriptionError::from)
                }
            })
            .collect()
    }

    /// Creates a new subscription for a single topic.
    ///
    /// This method requests a subscription for a topic called `name` from
    /// the coordinator. The requested topic must be published with the same
    /// data and timestamp types `T`and `D` respectively.
    ///
    /// In to obtain progress tracking information from the upstream
    /// Timely computation(s), the subscription handle exposes a `frontier()`
    /// method which allows inspection of the current frontier.
    ///
    /// When `blocking` is true, this call blocks until a remote publisher
    /// creates a topic with a suitable name. If `blocking` is false, the call
    /// returns with an error if the catalog does not contain a topic with a
    /// matching name.
    pub fn subscribe<T, D>(
        &self,
        name: &str,
        blocking: bool,
    ) -> Result<Subscription<T, D>, SubscriptionError>
    where
        T: RemoteTimestamp,
        D: DeserializeOwned + TypeName,
        T::Remote: TypeName,
    {
        let topics = self.request_multiple(Some(name), blocking)?;
        let sockets = self.connect_all::<T, D>(&topics)?;
        let sub = SubscriberGroup::<T, D>::new(sockets).wait()?;
        Ok(Subscription {
            sub: sub,
            topics: topics,
            coord: self.clone(),
        })
    }

    /// Create a new subscription for a partitioned topic.
    ///
    /// This method requests a subscription for each provided partition and
    /// merges their stream. The partition number is attached to each topic name,
    /// e.g. for `prefix = "foo"` and `partitions = [0, 2, 5]`, the resulting
    /// subscriptions are for `"foo.0", "foo.2", "foo.5"`.
    ///
    /// See the `subscribe` method for more details.
    pub fn subscribe_group<T, D, I>(
        &self,
        prefix: &str,
        partitions: I,
        blocking: bool,
    ) -> Result<Subscription<T, D>, SubscriptionError>
    where
        T: RemoteTimestamp,
        D: DeserializeOwned + TypeName,
        T::Remote: TypeName,
        I: IntoIterator<Item = usize>,
    {
        let names = partitions.into_iter().map(|i| format!("{}.{}", prefix, i));
        let topics = self.request_multiple(names, blocking)?;
        let sockets = self.connect_all::<T, D>(&topics)?;
        let sub = SubscriberGroup::<T, D>::new(sockets).wait()?;
        Ok(Subscription {
            sub: sub,
            topics: topics,
            coord: self.clone(),
        })
    }
}
