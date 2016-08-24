use std::io::Result;
use std::sync::mpsc;
use std::thread;

pub use self::frame::{Frame, Decode, Encode};

pub mod decoder;
pub mod request;

mod frame;
mod tcp;

pub trait Transport: Encode + Decode /* + Boxed */ {}
impl<T: Encode + Decode> Transport for T { }

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
    pub fn recv(&self) -> Result<Frame> {
        self.inner.recv()
    }

    #[deprecated]
    pub fn recv_decode<T: Decode>(&self) -> Result<T> {
        self.inner.recv().and_then(|frame| frame.decode::<T>())
    }

    pub fn detach<F>(mut self, mut f: F)
        where F: FnMut(Result<Frame>),
              F: Send + 'static
    {
        thread::spawn(move || {
            let mut is_ok = true;
            while is_ok {
                let res = self.recv();
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
