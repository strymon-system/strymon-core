// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Adapters between the Timely and the Futures world, allowing an external
//! futures event loop to process the output of a Timely stream.

use timely::dataflow::operators::capture::{Event, EventPusher};

use futures::Poll;
use futures::sync::mpsc;
use futures::stream::Stream;

/// Creates a new Sink/Stream pair. The sink is to be used together with
/// Timely's `capture_into` operator, while the stream emits the captures events.
pub fn pair<T, D>() -> (EventSink<T, D>, EventStream<T, D>) {
    let (tx, rx) = mpsc::unbounded();

    (EventSink { tx }, EventStream { rx })
}

/// A wrapper an unbounded futures mpsc channel which implements Timely's
/// `EventPusher` interface. Note that this does not implement `Clone` on
/// purpose, as there must only be one producer feeding this sink.
pub struct EventSink<T, D> {
    tx: mpsc::UnboundedSender<Event<T, D>>,
}

impl<T, D> EventPusher<T, D> for EventSink<T, D> {
    fn push(&mut self, event: Event<T, D>) {
        self.tx.unbounded_send(event).unwrap();
    }
}

/// A asynchronous `Stream` receiving Timely events put into the corresponding
/// sink.
pub struct EventStream<T, D> {
    rx: mpsc::UnboundedReceiver<Event<T, D>>,
}

impl<T, D> Stream for EventStream<T, D> {
    type Item = Event<T, D>;
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Event<T, D>>, ()> {
        self.rx.poll()
    }
}
