#![feature(question_mark)]

extern crate mio;
#[macro_use]
extern crate log;
extern crate slab;
extern crate abomonation;

use std::io::{Error, ErrorKind, Result};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;

use mio::{Poll, Token, Ready, PollOpt, Events};
use mio::channel::{Receiver as MioReceiver, SendError as MioSendError, Sender as MioSender};
use mio::tcp::{TcpListener, TcpStream};

use slab::Slab;

pub mod event;
pub mod message;

pub struct Message;

pub struct Service {
    poller: Poll,
    remote: MioReceiver<Register>,
    events: Events,
}

const REMOTE: Token = Token(0);

impl Service {
    pub fn init(external: Option<String>) -> Result<(Service, Remote)> {
        let external = external.unwrap_or(String::from("localhost"));

        let poller = Poll::new()?;
        let (tx, rx) = mio::channel::channel();
        poller.register(&rx, REMOTE, Ready::readable(), PollOpt::edge())?;

        let service = Service {
            poller: poller,
            remote: rx,
            events: Events::with_capacity(1024),
        };

        let remote = Remote {
            external: external,
            service: tx,
        };

        Ok((service, remote))
    }

    fn remote_register(&mut self, register: Register) -> Result<()> {
        match register {
            Register::Stream(stream, senderpull, receiverpush) => {
                let token = Token(1);
                //self.poller.register(&stream, )
            }
            Register::Listener(listener, listenerpush) => {
            
            }
        }
        
        Ok(())
    }
    
    fn remote_recv(&mut self) -> Result<()> {
        use ::std::sync::mpsc::TryRecvError::*;

        match self.remote.try_recv() {
            Ok(req) => self.remote_register(req),
            Err(Empty) => {
                warn!("spurious remote wakeup on network service");
                Ok(())
            },
            Err(Disconnected) => {
                Err(Error::new(ErrorKind::Other, "service shutdown"))
            }
        }
    }

    pub fn run(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.poller.poll(&mut self.events, timeout)?;
        Ok(())
    }
}

struct Writer {
    
}

struct Reader {

}

type SenderPush = MioSender<Message>;
type SenderPull = MioReceiver<Message>;
type ReceiverPush = MioSender<Result<Message>>;
type ReceiverPull = MioReceiver<Result<Message>>;
type ListenerPush = MioSender<Result<(Sender, Receiver)>>;
type ListenerPull = MioReceiver<Result<(Sender, Receiver)>>;

enum Register {
    Stream(TcpStream, SenderPull, ReceiverPush),
    Listener(TcpListener, ListenerPush),
}

fn convert_send_error<T>(err: MioSendError<T>) -> Error {
    match err {
        MioSendError::Io(err) => err,
        MioSendError::Disconnected(_) => {
            Error::new(ErrorKind::Other, "network service unreachable")
        }
    }
}

fn each_addr<A: ToSocketAddrs, F, T>(addr: A, mut f: F) -> Result<T>
    where F: FnMut(&SocketAddr) -> Result<T>
{
    let mut last_err = None;
    for addr in addr.to_socket_addrs()? {
        match f(&addr) {
            Ok(l) => return Ok(l),
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err.unwrap_or_else(|| {
        Error::new(ErrorKind::InvalidInput,
                   "could not resolve to any addresses")
    }))
}

#[derive(Clone)]
pub struct Remote {
    external: String,
    service: MioSender<Register>,
}

impl Remote {
    pub fn connect<E: ToSocketAddrs>(&self, endpoint: E) -> Result<(Sender, Receiver)> {
        let stream = each_addr(endpoint, TcpStream::connect)?;
        let (sendpush, sendpull) = mio::channel::channel();
        let (recvpush, recvpull) = mio::channel::channel();

        self.service
            .send(Register::Stream(stream, sendpull, recvpush))
            .map_err(convert_send_error)?;

        let tx = Sender { tx: sendpush };
        let rx = Receiver { rx: recvpull };

        Ok((tx, rx))
    }

    pub fn listen(&self, port: Option<u16>) -> Result<Listener> {
        let sockaddr = ("[::]", port.unwrap_or(0));
        let listener = each_addr(sockaddr, TcpListener::bind)?;
        let port = listener.local_addr()?.port();

        let (tx, rx) = mio::channel::channel();

        self.service
            .send(Register::Listener(listener, tx))
            .map_err(convert_send_error)?;

        Ok(Listener {
            external: self.external.clone(),
            port: port,
            rx: rx,
        })
    }
}

pub struct Receiver {
    rx: ReceiverPull,
}

impl Receiver {
    pub fn recv(&self) -> Option<Result<Message>> {
        use ::std::sync::mpsc::TryRecvError::*;

        match self.rx.try_recv() {
            Ok(res) => Some(res),
            Err(Empty) => None,
            Err(Disconnected) => {
                Some(Err(Error::new(ErrorKind::Other, "network service unreachable")))
            }
        }
    }
}

pub struct Sender {
    tx: SenderPush,
}

impl Sender {
    pub fn send<T: Into<Message>>(&self, msg: T) {}
}

pub struct Listener {
    external: String,
    port: u16,
    rx: MioReceiver<Result<(Sender, Receiver)>>,
}

impl Listener {
    pub fn local_addr(&self) -> (&str, u16) {
        (&*self.external, self.port)
    }

    pub fn accept(&self) -> Option<Result<(Sender, Receiver)>> {
        unimplemented!()
    }
}

fn _assert() {
    fn _is_send<T: Send>() {}
    _is_send::<Service>();
    _is_send::<Remote>();
}
