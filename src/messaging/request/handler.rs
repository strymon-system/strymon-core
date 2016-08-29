use std::ops::Deref;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::marker::PhantomData;

use messaging::{Message, Receiver, Sender, Transfer};
use messaging::decoder::Decoder;
use messaging::bytes::{Decode, Encode};
use messaging::request::*;

use util::Generator;

use abomonation::Abomonation;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(u64);

#[derive(Clone, Debug)]
pub struct Req<R: Request> {
    token: Token,
    request: R,
}

impl<R: Request> Req<R> {
    pub fn respond(self, result: Result<R::Success, R::Error>) -> Resp {
        Resp::new::<R>(result, self.token)
    }
}

#[derive(Clone)]
pub struct Resp {
    token: Token,
    success: bool,
    bytes: Vec<u8>,
}

impl Resp {
    fn new<R: Request>(result: Result<R::Success, R::Error>, token: Token) -> Self {
        match result {
            Ok(s) => {
                Resp {
                    token: token,
                    success: true,
                    bytes: s.encode(),
                }
            }
            Err(e) => {
                Resp {
                    token: token,
                    success: false,
                    bytes: e.encode(),
                }
            }
        }
    }
}

pub struct AsyncHandler {
    generator: Generator<Token>,
    waiting: BTreeMap<Token, Box<Fn(Resp) -> Option<Resp> + Send>>,
}

impl AsyncHandler {
    pub fn new() -> Self {
        AsyncHandler {
            generator: Generator::new(),
            waiting: BTreeMap::new(),
        }
    }

    pub fn submit<R: Request>(&mut self, r: R) -> (Req<R>, AsyncResult<R::Success, R::Error>) {
        let token = self.generator.generate();
        let req = Req {
            token: token,
            request: r,
        };

        let (tx, rx) = promise::<R>();
        let fnbox = Box::new(move |mut resp: Resp| {
            assert_eq!(resp.token, token);

            // TODO: This is a bit of a hack, because we cannot deocode messages
            //       partially. Thus the manual decode step here.
            //       Additionally, we work around Box<FnOnce> by using `tx.tx`
            //       directly.
            if resp.success {
                if <R as Request>::Success::is(&resp.bytes) {
                    let s = <R as Request>::Success::decode(&mut resp.bytes)
                        .expect("failed to decode successful resp");
                    let _ = tx.tx.send(Ok(s));

                    None
                } else {
                    Some(resp)
                }
            } else {
                if <R as Request>::Error::is(&resp.bytes) {
                    let e = <R as Request>::Error::decode(&mut resp.bytes)
                        .expect("failed to decode error resp");
                    let _ = tx.tx.send(Err(e));

                    None
                } else {
                    Some(resp)
                }
            }
        });

        self.waiting.insert(token, fnbox);

        (req, rx)
    }

    pub fn resolve(&mut self, mut resp: Resp) -> Option<Resp> {
        let token = resp.token;
        match self.waiting.entry(token) {
            Entry::Occupied(e) => {
                if let Some(resp) = e.get()(resp) {
                    Some(resp)
                } else {
                    e.remove();
                    None
                }
            }
            _ => Some(resp),
        }
    }
}

pub struct Handoff<R: Request> {
    tx: Sender,
    token: Token,
    marker: PhantomData<R>,
}

impl<R: Request> Handoff<R> {
    fn new(tx: Sender, token: Token) -> Self {
        Handoff {
            tx: tx,
            token: token,
            marker: PhantomData,
        }
    }

    pub fn success(self, s: R::Success) {
        self.result(Ok(s))
    }

    pub fn failed(self, e: R::Error) {
        self.result(Err(e))
    }

    pub fn result(self, result: Result<R::Success, R::Error>) {
        let resp = Resp::new::<R>(result, self.token);
        self.tx.send::<Resp>(&resp);
    }
}

pub fn handoff<R: Request>(req: Req<R>, tx: Sender) -> (R, Handoff<R>) {
    (req.request, Handoff::new(tx, req.token))
}

unsafe_abomonate!(Token);
unsafe_abomonate!(Resp: token, success, bytes);

impl<R: Request + Abomonation> Abomonation for Req<R> {
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


impl<R: Request> Deref for Req<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

impl From<u64> for Token {
    fn from(id: u64) -> Token {
        Token(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use messaging::request::Request;

    #[test]
    fn async() {
        let mut async = AsyncHandler::new();

        let (req1, rx1) = async.submit(());
        let (req2, rx2) = async.submit(());

        let resp2 = Resp::new::<()>(Ok(1337), req2.token);
        assert!(async.resolve(resp2.clone()).is_none());
        assert!(async.resolve(resp2).is_some());
        assert_eq!(rx2.await(), Ok(1337));

        let resp1 = Resp::new::<()>(Err(false), req1.token);
        assert!(async.resolve(resp1).is_none());
        assert_eq!(rx1.await(), Err(false));
    }
}
