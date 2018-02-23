// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The subscriber logic.

use std::io;
use std::marker::PhantomData;

use futures::{Async, Poll};
use futures::stream::{futures_unordered, Stream, FuturesUnordered};
use futures::future::Future;

use serde::de::DeserializeOwned;

use timely::progress::Timestamp;
use timely::progress::frontier::MutableAntichain;

use strymon_communication::transport::{Sender, Receiver};

use protocol::{Message, InitialSnapshot, RemoteTimestamp};
use publisher::progress::{UpperFrontier};
use util::StreamsUnordered;

/// Manages a group of subscribers.
///
/// Pulls messages from each underlying socket and updates the frontier
/// accordingly. Messages with a timestamp smaller or equal to the filter
/// antichain are discarded without decoding the payload.
pub struct SubscriberGroup<T: Timestamp, D> {
    streams: StreamsUnordered<Subscriber<T, D>>,
    frontier: MutableAntichain<T>,
    filter: Filter<T>,
}

impl<T, D> SubscriberGroup<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
{
    /// Creates a new driver for managing a subscriber group.
    ///
    /// The `root` capability is used to maintain the input frontier, it is
    /// typically the one returned by `unordered_input`.
    /// Returns a future which resolves once all connections have received
    /// their `InitialSnapshot`.
    pub fn new<I>(connections: I) -> ConnectingGroup<T, D>
        where I: IntoIterator<Item=(Sender, Receiver)>,
    {
        let connecting = connections.into_iter().map(Connecting::new);
        ConnectingGroup {
            waiting: futures_unordered(connecting),
            builder: Some(GroupBuilder {
                ready: StreamsUnordered::new(),
                frontier: MutableAntichain::new(),
                upper: UpperFrontier::empty(),
            })
        }
    }

    pub fn frontier(&self) -> &[T] {
        self.frontier.frontier()
    }

    /// Drives the subscriber logic based on a received message.
    fn process_message(&mut self, msg: Message<T, D>)
        -> io::Result<Option<(T, Vec<D>)>> {
        match msg {
            Message::LowerFrontierUpdate { update } => {
                self.frontier.update_iter(update);
                let frontier = self.frontier.frontier();
                self.filter.remove(frontier);

                Ok(None)
            },
            Message::DataMessage { time, data } => {
                if !self.filter.contains(&time) {
                    Ok(Some((time, data.decode()?)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

impl<T, D> Stream for SubscriberGroup<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
{
    type Item = (T, Vec<D>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        while let Some(message) = try_ready!(self.streams.poll()) {
            if let Some(item) = self.process_message(message)? {
                return Ok(Async::Ready(Some(item)));
            }
        }

        Ok(Async::Ready(None))
    }
}

/// A futures which resolves into a `SubscriberGroup<T, D>` once all underlying
/// sockets have received their initial snapshot.
pub struct ConnectingGroup<T: Timestamp, D> {
    waiting: FuturesUnordered<Connecting<T, D>>,
    builder: Option<GroupBuilder<T, D>>,
}

impl<T, D> Future for ConnectingGroup<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
{
    type Item = SubscriberGroup<T, D>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Some((snapshot, sub)) = try_ready!(self.waiting.poll()) {
            self.builder
                .as_mut()
                .expect("cannot connect twice")
                .add_subscriber(snapshot, sub);
        }

        assert!(self.waiting.is_empty());
        let builder = self.builder.take().unwrap();
        Ok(Async::Ready(builder.build()))
    }
}

/// State for partially connected subscribers.
struct GroupBuilder<T: Timestamp, D> {
    ready: StreamsUnordered<Subscriber<T, D>>,
    frontier: MutableAntichain<T>,
    upper: UpperFrontier<T>,
}

impl<T, D> GroupBuilder<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned
{
    /// Marks a subscriber as connected (provided we have its initial state).
    ///
    /// Adds the topic's upper/lower frontiers to the shared filter and frontier.
    fn add_subscriber(&mut self, snapshot: InitialSnapshot<T>, sub: Subscriber<T, D>) {
        self.ready.push(sub);

        for t in snapshot.upper {
            self.upper.insert(t);
        }

        self.frontier.update_iter(snapshot.lower.into_iter().map(|t| (t, 1)));
    }

    /// Builds the subscriber group, assuming all clients are connected.
    fn build(self) -> SubscriberGroup<T, D> {
        let filter = Filter { filter: self.upper.elements().to_owned() };

        SubscriberGroup {
            streams: self.ready,
            frontier: self.frontier,
            filter: filter,
        }
    }
}

/// A future which resolves once the underlying socket receives its initial snapshot.
struct Connecting<T, D> {
    socket: Option<(Sender, Receiver)>,
    marker: PhantomData<(T, D)>,
}

impl<T, D> Connecting<T, D> {
    fn new(socket: (Sender, Receiver)) -> Self {
        Connecting {
            socket: Some(socket),
            marker: PhantomData,
        }
    }
}

impl<T, D> Future for Connecting<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
{
    type Item = (InitialSnapshot<T>, Subscriber<T, D>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(buf) = try_ready!(self.socket.as_mut().unwrap().1.poll()) {
            let snapshot = InitialSnapshot::decode(buf)?;
            let socket = self.socket.take().unwrap();
            Ok(Async::Ready((snapshot, Subscriber {
                socket: socket,
                marker: self.marker,
            })))
        } else {
            Err(io::Error::from(io::ErrorKind::UnexpectedEof))
        }
    }
}

/// A connected subscriber (i.e. a subscriber which has already received its)
/// initial snapshot.
struct Subscriber<T, D> {
    socket: (Sender, Receiver),
    marker: PhantomData<(T, D)>,
}

impl<T, D> Stream for Subscriber<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
{
    type Item = Message<T, D>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(buf) = try_ready!(self.socket.1.poll()) {
            Ok(Async::Ready(Some(Message::decode(buf)?)))
        } else {
            Ok(Async::Ready(None))
        }
    }
}

/// Represents an antichain of epochs which we do not want to propagate.
#[derive(Debug)]
struct Filter<T> {
    filter: Vec<T>,
}

impl<T: Timestamp> Filter<T> {
    /// Removes all elements from the filter which are less or equal than any element in `frontier`
    fn remove(&mut self, frontier: &[T]) {
        self.filter.retain(|t| {
            !frontier.iter().any(|f| t.less_equal(f))
        })
    }

    /// Returns `true` if `time` is less or equal than any element in the filter.
    fn contains(&mut self, time: &T) -> bool {
        self.filter.iter().any(|f| time.less_equal(f))
    }
}
