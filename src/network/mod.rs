use std::io::{Result, Error};
use std::net::{TcpListener, TcpStream, Shutdown, ToSocketAddrs};
use std::sync::{mpsc, Arc};
use std::thread::{self, JoinHandle};
use std::env;

use futures::{Future, Poll, Async};
use futures::stream::Stream;
use futures::sink::Sink;
use futures::sync::mpsc::{Receiver as BoundedReceiver, channel as bounded};

use network::message::buf::{MessageBuf, read, write};

pub mod message;
pub mod reqrep;
pub mod fetch;

#[derive(Clone)]
pub struct Network {
    external: Arc<String>,
}

impl Network {
    fn external() -> String {
        // try to guess external hostname
        if let Ok(hostname) = env::var("TIMELY_SYSTEM_HOSTNAME") {
            return hostname;
        }

        warn!("unable to retrieve external hostname of machine.");
        warn!("falling back to 'localhost', set TIMELY_SYSTEM_HOSTNAME to override");

        String::from("localhost")
    }

    pub fn init() -> Result<Self> {
        Ok(Network { external: Arc::new(Self::external()) })
    }

    pub fn hostname(&self) -> String {
        (*self.external).clone()
    }

    pub fn connect<E: ToSocketAddrs>(&self, endpoint: E) -> Result<(Sender, Receiver)> {
        channel(TcpStream::connect(endpoint)?)
    }

    pub fn listen<P: Into<Option<u16>>>(&self, port: P) -> Result<Listener> {
        Listener::new(self.clone(), port.into().unwrap_or(0))
    }
}

fn channel(stream: TcpStream) -> Result<(Sender, Receiver)> {
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
    fn new(mut outstream: TcpStream) -> Self {
        let (sender_tx, sender_rx) = mpsc::channel();
        let thr = thread::spawn(move || {
            while let Ok(msg) = sender_rx.recv() {
                if let Err(err) = write(&mut outstream, &msg) {
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
    rx: BoundedReceiver<Result<MessageBuf>>,
}

impl Receiver {
    fn new(mut instream: TcpStream) -> Self {
        let (receiver_tx, receiver_rx) = bounded(0);
        thread::spawn(move || {
            let mut tx = receiver_tx;
            let mut stop = false;
            while !stop {
                let message = match read(&mut instream) {
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

fn poll_bounded<T, E>(rx: &mut BoundedReceiver<::std::result::Result<T, E>>) -> Poll<Option<T>, E> {
    match rx.poll().unwrap() {
        Async::Ready(Some(Ok(e))) => Ok(Async::Ready(Some(e))),
        Async::Ready(Some(Err(e))) => Err(e),
        Async::Ready(None) => Ok(Async::Ready(None)),
        Async::NotReady => Ok(Async::NotReady),
    }
}

impl Stream for Receiver {
    type Item = MessageBuf;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<MessageBuf>, Error> {
        poll_bounded(&mut self.rx)
    }
}

fn accept<T, F>(listener: TcpListener, mut f: F) -> BoundedReceiver<Result<T>>
    where F: FnMut(TcpStream) -> Result<T>,
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
    rx: BoundedReceiver<Result<(Sender, Receiver)>>,
}

impl Listener {
    fn new(network: Network, port: u16) -> Result<Self> {
        let sockaddr = ("0.0.0.0", port);
        let listener = TcpListener::bind(&sockaddr)?;
        let external = network.external.clone();
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
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<(Sender, Receiver)>, Error> {
        poll_bounded(&mut self.rx)
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
    use network::message::MessageBuf;
    use network::message::abomonate::Abomonate;
    use network::*;
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
            ping.push::<Abomonate, _>(&String::from("Ping")).unwrap();
            tx.send(ping);

            // process one single client
            listener.and_then(|(tx, rx)| {
                    let mut ping = rx.wait().next().unwrap()?;
                    assert_eq!("Ping", ping.pop::<Abomonate, String>().unwrap());

                    let mut pong = MessageBuf::empty();
                    pong.push::<Abomonate, _>(&String::from("Pong")).unwrap();
                    tx.send(pong);
                    Ok(())
                })
                .wait()
                .next()
                .unwrap()?;

            let mut pong = rx.wait().next().unwrap()?;
            assert_eq!("Pong", pong.pop::<Abomonate, String>().unwrap());

            Ok(())
        });
    }
}
