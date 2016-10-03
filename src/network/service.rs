use std::io::{Result, Error, ErrorKind};
use std::thread::{self, Builder};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, Condvar};
use std::net::{TcpListener, TcpStream};

use network::message::MessageBuf;

pub type EventSender<T> = ::event::Sender<Result<T>>;

pub struct Service {
    external: String,
}

impl Service {
    pub fn init<T: Into<Option<String>>>(external: T) -> Result<Self> {
        let external =  external.into().unwrap_or_else(|| String::from("localhost"));
        Ok(Service {
            external: external,
        })
    }
}

pub struct Sender {
    tx: mpsc::Sender<MessageBuf>,
}

impl Sender {
    pub fn send<T: Into<MessageBuf>>(&self, msg: T) {
        drop(self.tx.send(msg.into()));
    }
}

pub struct Receiver {
    sink: Arc<Slot<EventSender<MessageBuf>>>,
}

impl Receiver {
    pub fn on_recv(&self, tx: EventSender<MessageBuf>) {

    }
}

pub struct Listener {
    external: String,
    port: u16,

}

impl Listener {
    pub fn local_addr(&self) -> (&str, u16) {
        (&*self.external, self.port)
    }

    pub fn on_accept(&self, tx: EventSender<(Sender, Receiver)>) {

    }
}

fn pair(stream: TcpStream) -> Result<(Sender, Receiver)> {
    let instream = stream.try_clone()?;
    let mut outstream = stream;

    unimplemented!()
}

struct Slot<T> {
    data: Mutex<Option<T>>,
    some: Condvar,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Slot {
            data: Mutex::new(None),
            some: Condvar::new(),
        }
    }
}

fn _assert() {
    fn _is_send<T: Send>() {}
    _is_send::<Sender>();
    _is_send::<Receiver>();
    _is_send::<Service>();
}
