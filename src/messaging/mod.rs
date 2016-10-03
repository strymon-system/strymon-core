use std::io::{Result};
use std::thread;

use self::bytes::{Decode, Encode};

pub mod decoder;
pub mod request;

mod bytes;
mod tcp;

pub trait Transfer: Encode + Decode + Send + 'static {}
impl<T: Encode + Decode + Send + 'static> Transfer for T {}

#[derive(Clone)]
pub enum Message {
    Bytes(Vec<u8>),
}

impl Message {
    pub fn downcast<T: Transfer>(self) -> ::std::result::Result<T, Self> {
        match self {
            Message::Bytes(mut bytes) => {
                if T::is(&bytes) {
                    Ok(T::decode(&mut bytes).expect("decode failed"))
                } else {
                    Err(Message::Bytes(bytes))
                }
            }
        }
    }
}

fn from_tcp((tx, rx): (tcp::Sender, tcp::Receiver)) -> (Sender, Receiver) {
    (Sender { inner: tx }, Receiver { inner: rx })
}

pub fn connect(to: &str) -> Result<(Sender, Receiver)> {
    tcp::connect(to).map(from_tcp)
}

#[derive(Clone)]
pub struct Sender {
    inner: tcp::Sender,
}

impl Sender {
    pub fn send<T: Transfer>(&self, msg: &T) {
        self.inner.send(T::encode(msg))
    }
}

pub struct Receiver {
    inner: tcp::Receiver,
}

impl Receiver {
    pub fn recv(&self) -> Result<Message> {
        self.inner.recv().map(Message::Bytes)
    }

    pub fn detach<F>(self, mut f: F)
        where F: FnMut(Result<Message>),
              F: Send + 'static
    {
        thread::spawn(move || {
            let mut is_ok = true;
            while is_ok {
                let result = self.recv();
                is_ok = result.is_ok();
                f(result);
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

    pub fn external_addr(&self, host: &str) -> Result<(String, u16)> {
        let sockaddr = self.inner.local_addr()?;
        Ok((host.to_string(), sockaddr.port()))
    }

    pub fn detach<F>(mut self, mut f: F)
        where F: FnMut(Result<(Sender, Receiver)>),
              F: Send + 'static
    {
        thread::spawn(move || {
            let mut is_ok = true;
            while is_ok {
                let result = self.accept();
                is_ok = result.is_ok();
                f(result);
            }
        });
    }
}

impl ::abomonation::Abomonation for Message {
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            Message::Bytes(ref mut b) => b.embalm(),
        }
    }

    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            Message::Bytes(ref b) => b.entomb(bytes),
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            Message::Bytes(ref mut b) => b.exhume(bytes),
        }
    }
}
