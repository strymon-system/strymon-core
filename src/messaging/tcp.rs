use std::sync::mpsc;
use std::cell::RefCell;
use std::thread;
use std::io::Result;
use std::net::{Shutdown, TcpListener, TcpStream};

use messaging::frame::{self, Frame};

fn from_native(tcp: TcpStream) -> Result<(Sender, Receiver)> {
    let instream = try!(tcp.try_clone());
    let mut outstream = tcp;

    let (out_tx, out_rx) = mpsc::channel::<Frame>();
    thread::spawn(move || {
        while let Ok(frame) = out_rx.recv() {
            if let Err(err) = frame::write(&mut outstream, frame) {
                info!("unexpected error while writing frame: {:?}", err);
                break;
            }
        }

        let _ = outstream.shutdown(Shutdown::Both);
    });

    let tx = Sender { inner: out_tx };

    let rx = Receiver { inner: RefCell::new(instream) };

    Ok((tx, rx))
}

pub fn connect(to: &str) -> Result<(Sender, Receiver)> {
    from_native(try!(TcpStream::connect(to)))
}

pub struct Sender {
    inner: mpsc::Sender<Frame>,
}

impl Sender {
    pub fn send(&self, frame: Frame) {
        let _ = self.inner.send(frame);
    }
}

pub struct Receiver {
    inner: RefCell<TcpStream>,
}

impl Receiver {
    pub fn recv(&self) -> Result<Frame> {
        frame::read(&mut *self.inner.borrow_mut())
    }
}

pub fn listen(addr: Option<&str>) -> Result<Listener> {
    let addr = addr.unwrap_or("[::]:0");
    Ok(Listener { inner: try!(TcpListener::bind(addr)) })
}

pub struct Listener {
    inner: TcpListener,
}

impl Listener {
    pub fn accept(&mut self) -> Result<(Sender, Receiver)> {
        match self.inner.accept() {
            Ok((tcp, _)) => from_native(tcp),
            Err(err) => Err(err),
        }
    }
}
