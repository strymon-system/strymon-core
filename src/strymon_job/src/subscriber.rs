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
use futures::stream::Stream;

use serde::de::DeserializeOwned;

use timely::progress::Timestamp;
use timely::dataflow::operators::{Capability, CapabilitySet};

use strymon_communication::transport::{Sender, Receiver};

use protocol::{Message, RemoteTimestamp};

pub struct Subscriber<T, D> where T: Timestamp {
    frontier: CapabilitySet<T>,
    filter: Option<Filter<T>>,
    rx: Receiver,
    _tx: Sender,
    marker: PhantomData<(T, D)>,
}

impl<T, D> Subscriber<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
{
    /// Creates a new subscriber driver.
    ///
    /// The `root` capability is used to maintain the input frontier, it is
    /// typically the one returned by `unordered_input`.
    pub fn new((tx, rx): (Sender, Receiver), root: Capability<T>) -> Self {
        let mut frontier = CapabilitySet::new();
        frontier.insert(root);
        Subscriber {
            frontier: frontier,
            filter: None,
            rx: rx,
            _tx: tx,
            marker: PhantomData,
        }
    }

    /// Drives the subscriber logic based on a received message
    fn process_message(&mut self, msg: Message<T, D>) -> Option<(Capability<T>, Vec<D>)> {
        match msg {
            Message::LowerFrontier { lower } => {
                self.frontier.downgrade(&lower);
                if let Some(ref mut filter) = self.filter {
                    filter.remove(&lower);
                }
            },
            Message::UpperFrontier { upper } => {
                if self.filter.is_none() {
                    self.filter = Some(Filter {
                        filter: upper.into_owned(),
                    });
                }
            },
            Message::DataMessage { time, data } => {
                if let Some(ref mut filter) = self.filter {
                    if !filter.contains(&time) {
                        let cap = self.frontier.delayed(&time);
                        return Some((cap, data));
                    }
                }
            }
        };

        None
    }
}

impl<T, D> Stream for Subscriber<T, D>
    where T: RemoteTimestamp, D: DeserializeOwned,
{
    type Item = (Capability<T>, Vec<D>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        while let Some(mut message) = try_ready!(self.rx.poll()) {
            let message = message.pop::<Message<T, D>>()?;
            if let Some(item) = self.process_message(message) {
                return Ok(Async::Ready(Some(item)));
            }
        }

        // stream terminated, make sure to drop any rememaining capabilities
        self.frontier.downgrade(&[]);

        Ok(Async::Ready(None))
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
