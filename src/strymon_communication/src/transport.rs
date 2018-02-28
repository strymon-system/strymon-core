// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Asynchronous point-to-point message channels.
//!
//! The implementation is a thin wrapper around TCP sockets and thus follows
//! the same semantics: One peer needs to act as a [`Listener`](struct.Listener.html),
//! accepting new incoming connections.
//!
//! A connection conists of a pair of queue handles:
//!
//!   1. A non-blocking [`Sender`](struct.Sender.html) which enqueues
//!     [`MessageBuf`](../message/struct.MessageBuf.html) objects to be eventually
//!     sent over the network.
//!   2. A non-blocking [`Receiver`](struct.Receiver.html) receiving the messages
//!     in the same order they were sent.
//!
//! # Examples
//!
//! ```rust
//! extern crate futures;
//! extern crate strymon_communication;
//!
//! use std::io;
//! use std::thread;
//! use futures::stream::Stream;
//! use strymon_communication::Network;
//! use strymon_communication::message::MessageBuf;
//!
//! fn run_example() -> io::Result<String> {
//!     let network = Network::new(String::from("localhost"))?;
//!     let listener = network.listen(None)?;
//!     let (_, port) = listener.external_addr();
//!     thread::spawn(move || {
//!         let mut blocking = listener.wait();
//!         while let Some(Ok((tx, _rx))) = blocking.next() {
//!             tx.send(MessageBuf::new("Hello").unwrap());
//!         }
//!     });
//!
//!     let (_tx, rx) = network.connect(("localhost", port))?;
//!     let mut msg = rx.wait().next().unwrap()?;
//!     msg.pop::<String>()
//! }
//!
//! fn main() {
//!     assert_eq!("Hello", run_example().expect("I/O failure"));
//! }
//! ```
use std::io::{self, BufReader};
use std::net::{TcpListener, TcpStream, Shutdown, ToSocketAddrs};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};

use futures::{Future, Poll, Async};
use futures::stream::Stream;
use futures::sink::Sink;
use futures::sync::mpsc as futures_mpsc;

use Network;
use message::MessageBuf;

impl Network {
    /// Connects to a socket returns two queue handles.
    ///
    /// The endpoint socket is typically specified a `(host, port)` pair. The returned
    /// queue handles can be used to send and receive `MessageBuf` objects on that socket.
    /// Please refer to the [`transport`](transport/index.html) module level documentation
    /// for more details.
    pub fn connect<E: ToSocketAddrs>(&self, endpoint: E) -> io::Result<(Sender, Receiver)> {
        channel(TcpStream::connect(endpoint)?)
    }

    /// Opens a new socket and returns a handle to receive incomming clients.
    ///
    /// If the `port` is not specified, a random ephemerial port is chosen.
    /// Please refer to the [`transport`](transport/index.html) module level documentation
    /// for more details.
    pub fn listen<P: Into<Option<u16>>>(&self, port: P) -> io::Result<Listener> {
        Listener::new(self.clone(), port.into().unwrap_or(0))
    }
}

fn channel(stream: TcpStream) -> io::Result<(Sender, Receiver)> {
    let instream = stream.try_clone()?;
    let outstream = stream;

    let sender = Sender::new(outstream);
    let receiver = Receiver::new(instream);

    Ok((sender, receiver))
}

/// A queue handle to send messages on the channel.
///
/// ## Drop behavior:
/// This will close the underlying channel (including the receiving side) when
/// dropped. In addition if the socket is still open, `drop` will block until
/// the sender queue is drained.
#[derive(Clone)]
pub struct Sender {
    tx: Option<mpsc::Sender<MessageBuf>>,
    thr: Arc<Option<JoinHandle<()>>>,
}

impl Sender {
    pub(crate) fn new(mut outstream: TcpStream) -> Self {
        let (sender_tx, sender_rx) = mpsc::channel::<MessageBuf>();
        let thr = thread::spawn(move || {
            while let Ok(msg) = sender_rx.recv() {
                if let Err(err) = msg.write(&mut outstream) {
                    info!("unexpected error while writing bytes: {:?}", err);
                    break;
                }
            }

            drop(outstream.shutdown(Shutdown::Both));
        });

        Sender {
            tx: Some(sender_tx),
            thr: Arc::new(Some(thr)),
        }
    }

    /// Enqueues an outgoing message.
    ///
    /// The message might still be dropped if the underlying socket is closed
    /// by the remote receiver before the message is dequeued.
    pub fn send<T: Into<MessageBuf>>(&self, msg: T) {
        drop(self.tx.as_ref().unwrap().send(msg.into()));
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        // make sure to drain the queue if the other side is still connected
        drop(self.tx.take());
        if let Some(handle) = Arc::get_mut(&mut self.thr).and_then(Option::take) {
            drop(handle.join());
        }
    }
}

/// A queue handle for receiving messages on the channel.
///
/// This implements the `futures::stream::Stream` trait for non-blocking receives.
/// ## Drop behavior:
/// This will close the underlying channel (including the sending side) when
/// dropped.
pub struct Receiver {
    rx: futures_mpsc::UnboundedReceiver<io::Result<MessageBuf>>,
}

impl Receiver {
    fn new(instream: TcpStream) -> Self {
        let (tx, rx) = futures_mpsc::unbounded();
        thread::spawn(move || {
            let mut instream = BufReader::new(instream);
            let mut stop = false;
            while !stop {
                let message = match MessageBuf::read(&mut instream) {
                    Ok(Some(msg)) => Ok(msg),
                    Ok(None) => break,
                    Err(err) => {
                        stop = true;
                        Err(err)
                    }
                };

                match tx.unbounded_send(message) {
                    Ok(tx) => tx,
                    Err(_) => break,
                };
            }

            drop(instream.get_ref().shutdown(Shutdown::Both));
        });

        Receiver { rx: rx }
    }
}

pub(crate) fn poll_receiver<S, T>(mut stream: S) -> Poll<Option<T>, io::Error>
    where S: Stream<Item = io::Result<T>, Error = ()>
{
    // currently, the implementation never returns an error
    match stream.poll().unwrap() {
        Async::Ready(Some(Ok(e))) => Ok(Async::Ready(Some(e))),
        Async::Ready(Some(Err(e))) => Err(e),
        Async::Ready(None) => Ok(Async::Ready(None)),
        Async::NotReady => Ok(Async::NotReady),
    }
}

impl Stream for Receiver {
    type Item = MessageBuf;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<MessageBuf>, io::Error> {
        poll_receiver(&mut self.rx)
    }
}

/// spawns an acceptor thread which accepts new client on the provided tcp server
/// socket. converts each socket with the provided function before pushing it
/// into the receiver sink.
pub(crate) fn accept<T, F>(listener: TcpListener, mut f: F) -> futures_mpsc::Receiver<io::Result<T>>
    where F: FnMut(TcpStream) -> io::Result<T>,
          F: Send + 'static,
          T: Send + 'static
{
    let (tx, rx) = futures_mpsc::channel(0);
    thread::spawn(move || {
        let mut tx = tx;
        let mut is_ok = true;
        while is_ok {
            let stream = listener.accept();
            is_ok = stream.is_ok();
            let res = stream.and_then(|(s, _)| f(s));
            tx = match tx.send(res).wait() {
                Ok(tx) => tx,
                Err(_) => break,
            }
        }
        debug!("listener thread is exiting");
    });

    rx
}

/// A queue handle accepting incoming connections.
///
/// ## Drop behavior:
/// This will close the underlying server socket when dropped.
pub struct Listener {
    external: Arc<String>,
    port: u16,
    rx: futures_mpsc::Receiver<io::Result<(Sender, Receiver)>>,
}

impl Listener {
    fn new(network: Network, port: u16) -> io::Result<Self> {
        let sockaddr = ("0.0.0.0", port);
        let listener = TcpListener::bind(&sockaddr)?;
        let external = network.hostname.clone();
        let port = listener.local_addr()?.port();
        let rx = accept(listener, channel);

        Ok(Listener {
            external: external,
            port: port,
            rx: rx,
        })
    }

    /// Returns the address of the socket in the form of `(hostname, port)`.
    pub fn external_addr(&self) -> (&str, u16) {
        (&*self.external, self.port)
    }
}

impl Stream for Listener {
    type Item = (Sender, Receiver);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<(Sender, Receiver)>, io::Error> {
        poll_receiver(&mut self.rx)
    }
}


fn _assert() {
    fn _is_send<T: Send>() {}
    _is_send::<Sender>();
    _is_send::<Receiver>();
    _is_send::<Listener>();
    _is_send::<Network>();
}

#[cfg(test)]
mod tests {

    use futures::stream::Stream;
    use message::MessageBuf;
    use Network;
    use std::io::Result;

    fn assert_io<F: FnOnce() -> Result<()>>(f: F) {
        f().expect("I/O test failed")
    }

    #[test]
    fn network_integration() {
        assert_io(|| {
            let network = Network::new(None)?;
            let listener = network.listen(None)?;
            let (tx, rx) = network.connect(listener.external_addr())?;

            let mut ping = MessageBuf::empty();
            ping.push(&String::from("Ping")).unwrap();
            tx.send(ping);

            // process one single client
            listener.and_then(|(tx, rx)| {
                    let mut ping = rx.wait().next().unwrap()?;
                    assert_eq!("Ping", ping.pop::<String>().unwrap());

                    let mut pong = MessageBuf::empty();
                    pong.push(&String::from("Pong")).unwrap();
                    tx.send(pong);
                    Ok(())
                })
                .wait()
                .next()
                .unwrap()?;

            let mut pong = rx.wait().next().unwrap()?;
            assert_eq!("Pong", pong.pop::<String>().unwrap());

            Ok(())
        });
    }
}
