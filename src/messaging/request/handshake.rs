use std::io;
use std::any::Any;

use abomonation::Abomonation;

use messaging::{Sender, Receiver};
use messaging::request::Request;
use messaging::decoder::Decoder;

#[derive(Debug, Clone)]
pub struct Handshake<R: Request>(pub R);

impl<R: Request> Handshake<R>
    where R: Abomonation + Clone + Any,
          R::Success: Abomonation + Clone + Any,
          R::Error: Abomonation + Clone + Any
{   
    pub fn wait(self, tx: &Sender, rx: &Receiver) -> io::Result<Response<R>> {
        tx.send(&self);
        Decoder::from(rx.recv())
            .when::<Response<R>, _>(|res| res)
            .unwrap_result()
    }
}

#[derive(Debug, Clone)]
pub enum Response<R: Request> {
    Success(R::Success),
    Error(R::Error),
}

impl<R: Request> Response<R> {
    pub fn into_result(self) -> Result<R::Success, R::Error> {
        match self {
            Response::Success(t) => Ok(t),
            Response::Error(e) => Err(e),
        }
    }
}

impl<R: Request> From<Result<R::Success, R::Error>> for Response<R> {
    fn from(result: Result<R::Success, R::Error>) -> Response<R> {
        match result {
            Ok(t) => Response::Success(t),
            Err(e) => Response::Error(e),
        }
    }
}

impl<R: Request> Abomonation for Handshake<R>
    where R: Abomonation
{
    #[inline]
    unsafe fn embalm(&mut self) {
        self.0.embalm()
    }

    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        self.0.entomb(bytes)
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        self.0.exhume(bytes)
    }
}

impl<R: Request> Abomonation for Response<R>
    where R::Success: Abomonation,
          R::Error: Abomonation,
{
    #[inline]
    unsafe fn embalm(&mut self) {
        match *self {
            Response::Success(ref mut t) => t.embalm(),
            Response::Error(ref mut e) => e.embalm(),
        }
    }

    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        match *self {
            Response::Success(ref t) => t.entomb(bytes),
            Response::Error(ref e) => e.entomb(bytes),
        }
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        match *self {
            Response::Success(ref mut t) => t.exhume(bytes),
            Response::Error(ref mut e) => e.exhume(bytes),
        }
    }
}
