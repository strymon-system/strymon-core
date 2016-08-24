use std::io::Result;
use std::sync::mpsc;
use std::thread;

use self::frame::Frame;
pub use self::frame::{Decode, Encode};

mod frame;
mod tcp;

pub type Message = Frame;

fn from_tcp((tx, rx): (tcp::Sender, tcp::Receiver)) -> (Sender, Receiver) {
    (Sender { inner: tx }, Receiver { inner: rx })
}

pub fn connect(to: &str) -> Result<(Sender, Receiver)> {
    tcp::connect(to).map(from_tcp)
}

pub struct Sender {
    inner: tcp::Sender,
}

impl Sender {
    pub fn send<T: Encode>(&self, msg: T) {
        self.inner.send(Frame::encode(msg))
    }
}

pub struct Receiver {
    inner: tcp::Receiver,
}

impl Receiver {
    pub fn recv_any(&self) -> Result<Message> {
        self.inner.recv()
    }

    pub fn recv<T: Decode>(&self) -> Result<T> {
        self.inner.recv().and_then(|frame| frame.decode::<T>())
    }

    pub fn detach<T: Decode, F>(mut self, mut f: F)
        where F: FnMut(Result<T>),
              F: Send + 'static
    {
        thread::spawn(move || {
            let mut is_ok = true;
            while is_ok {
                let res = self.recv::<T>();
                is_ok = res.is_ok();
                f(res);
            }
        });
    }
}

pub fn listen(addr: Option<&str>) -> Result<Listener> {
    Ok(Listener { inner: try!(tcp::listen(addr)) })
}

pub struct Listener {
    inner: tcp::Listener,
}

impl Listener {
    pub fn accept(&mut self) -> Result<(Sender, Receiver)> {
        self.inner.accept().map(from_tcp)
    }
}
