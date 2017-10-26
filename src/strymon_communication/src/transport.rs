// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;
use std::net::{TcpListener, TcpStream, Shutdown, ToSocketAddrs};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};

use futures::{Future, Poll, Async};
use futures::stream::Stream;
use futures::sink::Sink;
use futures::sync::mpsc::{Receiver as BoundedReceiver, channel as bounded};

use Network;
use message::MessageBuf;

impl Network {
    /// Connects to a socket specified by `endpoint` and returns two queue handles
    /// to send and receive MessageBuf objects on that socket
    pub fn connect<E: ToSocketAddrs>(&self, endpoint: E) -> io::Result<(Sender, Receiver)> {
        channel(TcpStream::connect(endpoint)?)
    }

    /// Opens a new socket on the optionally specified port and returns a handle
    /// to receive incomming clients.
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

pub struct Receiver {
    rx: BoundedReceiver<io::Result<MessageBuf>>,
}

impl Receiver {
    fn new(mut instream: TcpStream) -> Self {
        let (receiver_tx, receiver_rx) = bounded(0);
        thread::spawn(move || {
            let mut tx = receiver_tx;
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

                tx = match tx.send(message).wait() {
                    Ok(tx) => tx,
                    Err(_) => break,
                };
            }

            drop(instream.shutdown(Shutdown::Both));
        });

        Receiver { rx: receiver_rx }
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
pub(crate) fn accept<T, F>(listener: TcpListener, mut f: F) -> BoundedReceiver<io::Result<T>>
    where F: FnMut(TcpStream) -> io::Result<T>,
          F: Send + 'static,
          T: Send + 'static
{
    let (tx, rx) = bounded(0);
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

pub struct Listener {
    external: Arc<String>,
    port: u16,
    rx: BoundedReceiver<io::Result<(Sender, Receiver)>>,
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
            let network = Network::init()?;
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
