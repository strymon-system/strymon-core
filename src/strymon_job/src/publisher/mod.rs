// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The publisher logic and the interfaces used to control it.

use std::io;
use std::thread;
use std::sync::{Arc, Mutex, Condvar};

use slab::Slab;
use serde::Serialize;

use timely::ExchangeData;
use timely::progress::timestamp::Timestamp;
use timely::dataflow::operators::capture::event::{Event as TimelyEvent, EventPusher};
use tokio_core::reactor::{Core, Handle};

use strymon_communication::Network;
use strymon_communication::transport::{Listener, Sender, Receiver};
use strymon_communication::message::MessageBuf;

use futures::future::Future;
use futures::stream::{self, Stream};
use futures::unsync::mpsc;

use protocol::{Message, InitialSnapshot, RemoteTimestamp};

use self::progress::{LowerFrontier, UpperFrontier};
use self::sink::{EventSink, EventStream};

pub mod sink;
pub mod progress;

type SubscriberId = usize;

enum Event<T, D> {
    Timely(TimelyEvent<T, D>),
    Accepted((Sender, Receiver)),
    Disconnected(SubscriberId),
    Error(SubscriberId, io::Error),
    ShutdownRequested,
}

/// State and logic of the publisher.
///
/// Maintains the upper and lower frontier of a Timely stream and broadcasts
/// their updated versions and any incoming data tuples to subscribed clients.
struct PublisherServer<T: Timestamp, D> {
    // progress tracking state
    lower: LowerFrontier<T>,
    upper: UpperFrontier<T>,
    // connected subscribers
    subscribers: Slab<Sender>,
    count: AtomicCounter,
    // tokio event loop
    events: Box<Stream<Item = Event<T, D>, Error = io::Error>>,
    notificator: mpsc::UnboundedSender<Event<T, D>>,
    core: Core,
    handle: Handle,
}

impl<T: RemoteTimestamp, D: ExchangeData + Serialize> PublisherServer<T, D> {
    /// Creates a new publisher, accepting subscribers on `socket`, publishing
    /// the Timely events observed on `stream`.
    fn new(socket: Listener, stream: EventStream<T, D>, count: AtomicCounter) -> io::Result<Self> {
        let core = Core::new()?;
        let handle = core.handle();

        // queue for disconnection events from subscribers
        let (notificator, subscribers) = mpsc::unbounded();

        // we have three event sources:
        let listener = socket.map(Event::Accepted);
        let timely = stream
            .map(Event::Timely)
            .map_err(|_| unreachable!())
            .chain(stream::once(Ok(Event::ShutdownRequested)));
        let subscribers = subscribers.map_err(|_| unreachable!());
        // all of which we merge into a single stream
        let events = listener.select(subscribers).select(timely);

        Ok(PublisherServer {
            lower: LowerFrontier::default(),
            upper: UpperFrontier::empty(),
            subscribers: Slab::new(),
            count: count,
            events: Box::new(events),
            notificator: notificator,
            core: core,
            handle: handle,
        })
    }

    fn next_event(&mut self) -> io::Result<Event<T, D>> {
        // run tokio reactor until we get the next event
        let next_msg = self.events.by_ref().into_future();
        match self.core.run(next_msg) {
            Ok((msg, _)) => Ok(msg.unwrap()),
            Err((err, _)) => Err(err),
        }
    }

    /// Starts serving subscribers, blocks until the Timely stream completes
    /// (or an error happens).
    fn serve(mut self) -> io::Result<()> {
        loop {
            match self.next_event()? {
                // processing incoming timely events
                Event::Timely(ev) => self.timely_event(ev)?,
                // handle networking events
                Event::Accepted(sub) => self.add_subscriber(sub)?,
                Event::Disconnected(id) => self.remove_subscriber(id),
                Event::Error(id, err) => {
                    // subscriber errors should not be fatal. we just log
                    // them and forget about it.
                    error!("Subscriber {}: {}", id, err);
                }
                Event::ShutdownRequested => {
                    // this drops self, and thus drain the queues of
                    // all still connected subscribers
                    return Ok(());
                }
            }
        }
    }

    /// Sends `msg` to all connected subscribers.
    fn broadcast(&self, msg: MessageBuf) -> io::Result<()> {
        if self.subscribers.len() == 0 {
            // nothing to do here
            return Ok(());
        }

        let last = self.subscribers.len() - 1;
        for (id, sub) in self.subscribers.iter() {
            if id < last {
                sub.send(msg.clone());
            } else {
                // this case is a hint to the compiler that for the last
                // iteration we can move `msg` directly, no need to clone
                sub.send(msg);
                break;
            }
        }

        Ok(())
    }

    /// Processes a single Timely event, might cause multiple messages to be
    /// sent to connected subscribers.
    fn timely_event(&mut self, event: TimelyEvent<T, D>) -> io::Result<()> {
        match event {
            TimelyEvent::Progress(mut updates) => {
                self.lower.update(&mut updates);
                if !updates.is_empty() {
                    self.broadcast(Message::<T, D>::frontier_update(updates)?)?;
                }
            }
            TimelyEvent::Messages(time, data) => {
                self.upper.insert(time.clone());
                self.broadcast(Message::<T, D>::data_message(time, data)?)?;
            }
        };

        Ok(())
    }

    /// Registers a new subscriber.
    ///
    /// Installs a "monitor" for the subscriber, making sure we get notified
    /// when it disconnects.
    fn add_subscriber(&mut self, (tx, rx): (Sender, Receiver)) -> io::Result<()> {
        // inform new subscriber about current state of progress
        let snapshot = InitialSnapshot::encode(self.lower.elements(), self.upper.elements())?;
        tx.send(snapshot);

        // add it to the list of listening subscribers
        self.count.increment();
        let id = self.subscribers.insert(tx);

        // register event handler for disconnection
        let notificator = self.notificator.clone();
        let subscriber = rx.for_each(|_| {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected message",
            ))
        }).then(move |res| {
                let event = match res {
                    Ok(()) => Event::Disconnected(id),
                    Err(err) => Event::Error(id, err),
                };

                notificator.unbounded_send(event).map_err(|_| ())
            });

        self.handle.spawn(subscriber);
        Ok(())
    }

    /// Removes a subscriber from the broadcasting list.
    ///
    /// This does not cancel the subscriber monitor registered above, so if the
    /// subscriber is still alive, it will still emit events on errors or
    /// when it disconnects.
    fn remove_subscriber(&mut self, id: SubscriberId) {
        self.count.decrement();
        self.subscribers.remove(id);
    }
}

impl<T: Timestamp, D> Drop for PublisherServer<T, D> {
    fn drop(&mut self) {
        self.subscribers.clear();
        self.count.invalidate();
    }
}

/// The host and port on which the publisher is accepting subscribers.
pub type Addr = (String, u16);

/// A handle for spawned publisher.
///
/// This implements `EventPusher`, so it can be used with Timely's `capture`.
/// When dropped, will block and drain any subscriber queues.
pub struct Publisher<T, D> {
    /// Handle for events to be published by this instance.
    sink: Option<EventSink<T, D>>,
    /// A join handle for the spawned thread.
    thread: Thread,
    // The current subscriber count (wrapped in a mutex, so we can block on it)
    subscribers: AtomicCounter,
}

impl<T, D> Publisher<T, D>
where
    T: RemoteTimestamp,
    D: ExchangeData + Serialize,
{
    /// Spawns a new publisher thread on a ephemerial network port.
    ///
    /// The corresponding address can be obtained from the first member of the
    /// tuple. The publisher handle itself is used to send events into the
    /// topic.
    pub fn new(network: &Network) -> io::Result<(Addr, Self)> {
        // the queue between the Timely operator and this publisher thread
        let (timely_sink, timely_stream) = sink::pair();

        // the network socket on which subscribers are accepted
        let listener = network.listen(None)?;
        let addr = {
            let (host, port) = listener.external_addr();
            (String::from(host), port)
        };

        let subscribers = AtomicCounter::new();
        let count = subscribers.clone();

        // main event loop of the publisher thread
        let handle = thread::spawn(move || {
            PublisherServer::new(listener, timely_stream, count)
                .and_then(|publisher| publisher.serve())
        });

        let publisher = Publisher {
            sink: Some(timely_sink),
            thread: Thread::new(handle),
            subscribers: subscribers,
        };

        Ok((addr, publisher))
    }

    /// Blocks the current thread until some subscribers have connected.
    ///
    /// Returns the number of currently connected subscribers. Note that this
    /// does not actually guarantee that the subscribers are still connected,
    /// only that there was some recent point in time when there were some
    /// connected subscribers. This is mostly intended for testing purposes.
    #[allow(dead_code)]
    pub fn subscriber_barrier(&self) -> io::Result<usize> {
        // important: this must unblock when the thread dies, so we make
        // sure to call `count.invalidate()` in the publisher thread when it drops
        let count = self.subscribers.wait_nonzero();
        if count == COUNTER_INVALID {
            Err(io::Error::new(io::ErrorKind::Other, "publisher terminated"))
        } else {
            Ok(count)
        }
    }
}

impl<T, D> EventPusher<T, D> for Publisher<T, D>
where
    T: RemoteTimestamp,
    D: ExchangeData + Serialize,
{
    fn push(&mut self, event: TimelyEvent<T, D>) {
        self.sink.as_mut().unwrap().push(event)
    }
}

impl<T, D> Drop for Publisher<T, D> {
    fn drop(&mut self) {
        // Note the the drop order is important here: The event `EventSink` must be
        // dropped before `Thread` in order to avoid a deadlock: Dropping `EventSink`
        // indicates to the publisher thread that it has to shut down, which will block
        // the join operation until the shutdown is complete.
        drop(self.sink.take());
        if let Err(err) = self.thread.join() {
            error!("failed to drain subscriber queues: {}", err);
        }
    }
}

type ThreadHandle = thread::JoinHandle<io::Result<()>>;

/// A join handle for the publisher thread.
///
/// This can be used to ensure all subscriber queues are drained properly.
struct Thread(Option<ThreadHandle>);

impl Thread {
    fn new(handle: ThreadHandle) -> Self {
        Thread(Some(handle))
    }

    fn join(&mut self) -> io::Result<()> {
        match self.0.take().map(|t| t.join()) {
            Some(Ok(res)) => res,
            Some(Err(_)) => Err(io::Error::new(io::ErrorKind::Other, "thread panicked")),
            None => Err(io::Error::new(io::ErrorKind::Other, "already joined")),
        }
    }
}

/// A counter which can block readers when it reaches zero.
#[derive(Debug, Clone)]
struct AtomicCounter(Arc<(Mutex<usize>, Condvar)>);

const COUNTER_INVALID: usize = ::std::usize::MAX;

impl AtomicCounter {
    fn new() -> Self {
        AtomicCounter(Default::default())
    }

    fn lock<'a>(&'a self) -> (::std::sync::MutexGuard<'a, usize>, &'a Condvar) {
        let AtomicCounter(ref inner) = *self;
        (
            inner.0.lock().expect("publisher thread poisioned counter"),
            &inner.1,
        )
    }

    fn increment(&self) {
        let (mut count, nonzero) = self.lock();
        *count += 1;
        nonzero.notify_all();
    }

    fn decrement(&self) {
        let (mut count, _) = self.lock();
        debug_assert!(*count > 0);
        *count -= 1;
    }

    fn invalidate(&self) {
        let (mut count, nonzero) = self.lock();
        *count = COUNTER_INVALID;
        nonzero.notify_all();
    }

    fn wait_nonzero(&self) -> usize {
        let (mut count, nonzero) = self.lock();
        while *count == 0 {
            count = nonzero.wait(count).unwrap();
        }

        *count
    }
}
