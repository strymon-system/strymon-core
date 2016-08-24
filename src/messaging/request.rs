use abomonation::Abomonation;

use messaging::{Transport, Sender};

pub trait Request: Transport {
    type Success: Transport;
    type Error: Transport;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(u64);

pub struct Async<R: Request> {
    pub token: Token,
    pub request: R,
}

pub struct Handoff<R: Request> {
    r: R,
}

pub struct Complete<R: Request> {
    r: R,
}

pub struct AsyncResult<R: Request> {
    r: R,
}

pub fn handoff<R: Request>(tx: Sender, token: Token) -> Handoff<R> {
    unimplemented!()
}

pub fn promise<R: Request>() -> (Complete<R>, AsyncResult<R>) {
    unimplemented!()
}

unsafe_abomonate!(Token);

impl<R: Request + Abomonation> Abomonation for Async<R> {
    #[inline]
    unsafe fn embalm(&mut self) {
        self.token.embalm();
        self.request.embalm();
    }
    #[inline]
    unsafe fn entomb(&self, bytes: &mut Vec<u8>) {
        self.token.entomb(bytes);
        self.request.entomb(bytes);
    }
    #[inline]
    unsafe fn exhume<'a, 'b>(&'a mut self, bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        Some(bytes)
            .and_then(|bytes| self.token.exhume(bytes))
            .and_then(|bytes| self.request.exhume(bytes))
    }
}
