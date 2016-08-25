use std::ops::Deref;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::marker::PhantomData;

use messaging::{Message, Sender, Transfer};
use messaging::bytes::{Decode, Encode};
use messaging::request::*;

use util::Generator;

use abomonation::Abomonation;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(u64);

pub struct Req<R: Request> {
    token: Token,
    request: R,
}

#[derive(Clone)]
pub struct Response {
    token: Token,
    success: bool,
    bytes: Vec<u8>,
}

impl Response {
    fn new<R: Request>(result: Result<R::Success, R::Error>, token: Token) -> Self {
        match result {
            Ok(s) => Response {
                token: token,
                success: true,
                bytes: s.encode(),
            },
            Err(e) => Response {
                token: token,
                success: false,
                bytes: e.encode(),
            }
        }
    }
}

pub struct Waiter {
    generator: Generator<Token>,
    waiting: BTreeMap<Token, Box<Fn(Response) -> Option<Response> + Send>>,
}

impl Waiter {
    pub fn new() -> Self {
        Waiter {
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
        let fnbox = Box::new(move |mut response: Response| {
            assert_eq!(response.token, token);

            // TODO: This is a bit of a hack, because we cannot deocode messages
            //       partially. Thus the manual decode step here.
            //       Additionally, we work around Box<FnOnce> by using `tx.tx`
            //       directly.
            if response.success {
                if <R as Request>::Success::is(&response.bytes) {
                    let s = <R as Request>::Success::decode(&mut response.bytes)
                        .expect("failed to decode successful response");
                    let _ = tx.tx.send(Ok(s));

                    None
                } else {
                    Some(response)
                }
            } else {
                if <R as Request>::Error::is(&response.bytes) {
                    let e = <R as Request>::Error::decode(&mut response.bytes)
                        .expect("failed to decode error response");
                    let _ = tx.tx.send(Err(e));

                    None
                } else {
                    Some(response)
                }
            }
        });

        self.waiting.insert(token, fnbox);

        (req, rx)
    }

    pub fn resolve(&mut self, mut response: Response) -> Option<Response> {
        let token = response.token;
        match self.waiting.entry(token) {
            Entry::Occupied(e) => {
                if let Some(response) = e.get()(response) {
                    Some(response)
                } else {
                    e.remove();
                    None
                }
            }
            _ => Some(response),
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
        let response = Response::new::<R>(result, self.token);
        self.tx.send::<Response>(&response);
    }
}

pub fn handoff<R: Request>(req: Req<R>, tx: Sender) -> (R, Handoff<R>) {
    (req.request, Handoff::new(tx, req.token))
}


unsafe_abomonate!(Token);
unsafe_abomonate!(Response: token, success, bytes);

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
    fn waiter() {
        let mut waiter = Waiter::new();

        let (req1, rx1) = waiter.submit(());
        let (req2, rx2) = waiter.submit(());

        let resp2 = Response::new::<()>(Ok(1337), req2.token);
        assert!(waiter.resolve(resp2.clone()).is_none());
        assert!(waiter.resolve(resp2).is_some());
        assert_eq!(rx2.await(), Ok(1337));

        let resp1 = Response::new::<()>(Err(false), req1.token);
        assert!(waiter.resolve(resp1).is_none());
        assert_eq!(rx1.await(), Err(false));
    }
}
