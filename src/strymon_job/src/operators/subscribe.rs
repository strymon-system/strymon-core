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
use timely::dataflow::operators::Capability;

use futures::{Future, Poll};
use futures::stream::{Stream, Wait};

use typename::TypeName;
use serde::de::DeserializeOwned;

use strymon_rpc::coordinator::*;
use strymon_model::{Topic, TopicId, TopicType, TopicSchema};

use Coordinator;
use subscriber::Subscriber;
use protocol::RemoteTimestamp;

/// A subscription handle used to receive data from a topic.
///
/// A subscription can be optained by calling the `Coordinator::subscribe` method.
/// The subscription can be read by using the type as an iterator, e.g. in a `for`
/// loop. The iterator will yield the necessary capabilities and data batches to
/// to be used in combination with Timely's `unordered_input` or `source` operator.
///
/// In addition, this type also supports the asynchronous `futures::stream::Stream` trait,
/// allowing users to block on multiple topics at once.
pub struct Subscription<T: Timestamp, D> {
    sub: Subscriber<T, D>,
    topic: Topic,
    coord: Coordinator,
}

impl<T, D> Stream for Subscription<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
{
    type Item = (Capability<T>, Vec<D>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.sub.poll()
    }
}

/// A blocking iterator, yielding `io::Result<(Capability<T>, Vec<D>)>` tuples.
pub struct IntoIter<T: Timestamp, D> {
    inner: Wait<Subscription<T, D>>,
}

impl<T, D> IntoIterator for Subscription<T, D>
    where T: RemoteTimestamp,
          D: DeserializeOwned,
{
    type Item = io::Result<(Capability<T>, Vec<D>)>;
    type IntoIter = IntoIter<T, D>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { inner: self.wait() }
    }
}

impl<T, D> Iterator for IntoIter<T, D>
    where T: RemoteTimestamp,
          D: DeserializeOwned,
{
    type Item = io::Result<(Capability<T>, Vec<D>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<T: Timestamp, D> Drop for Subscription<T, D> {
    fn drop(&mut self) {
        if let Err(err) = self.coord.unsubscribe(self.topic.id) {
            error!("failed to unsubscribe: {:?}", err)
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
    /// Unsubscribes from a topic.
    fn unsubscribe(&self, topic: TopicId) -> Result<(), SubscriptionError> {
        self.tx
            .request(&Unsubscribe {
                topic: topic,
                token: self.token,
            })
            .map_err(SubscriptionError::from)
            .wait()
    }

    /// Create a new subscription for a topic.
    ///
    /// This method requests a subscription for a topic called `name` from
    /// the coordinator. The requested topic must be published with the same
    /// data and timestamp types `T`and `D` respectively.
    ///
    /// In order to forward progress tracking information to the downstream
    /// Timely computation, the subscription requires an initial root capability.
    /// It can be obtained either through the `unordered_input` or the `source`
    /// Timely input operators.
    ///
    /// When `blocking` is true, this call blocks until a remote publisher
    /// creates a topic with a suitable name. If `blocking` is false, the call
    /// returns with an error if the catalog does not contain a topic with a
    /// matching name.
    pub fn subscribe<T, D>(&self,
                    name: &str,
                    root: Capability<T>,
                    blocking: bool)
                    -> Result<Subscription<T, D>, SubscriptionError>
        where T: RemoteTimestamp ,
              D: DeserializeOwned + TypeName,
              T::Remote: TypeName,
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
                let item = TopicType::of::<D>();
                let time = TopicType::of::<T::Remote>();
                let schema = TopicSchema::Stream(item, time);
                if topic.schema != schema {
                    return Err(SubscriptionError::TypeMismatch);
                }

                let socket = self.network.connect((&*topic.addr.0, topic.addr.1))?;
                let sub = Subscriber::<T, D>::new(socket, root);
                Ok(Subscription {
                    sub: sub,
                    topic: topic,
                    coord: coord,
                })
            })
            .wait()
    }
}
