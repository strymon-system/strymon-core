use std::ops::Deref;
use std::collections::BTreeMap;
use std::marker::PhantomData;

use messaging::Sender;
use messaging::bytes::{Decode, Encode};
use messaging::request::*;

use util::Generator;

use abomonation::Abomonation;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(u64);

#[derive(Clone, Debug)]
pub struct AsyncReq<R: Request> {
    token: Token,
    request: R,
}

impl<R: Request> AsyncReq<R> {
    pub fn reply(self, result: Result<R::Success, R::Error>) -> AsyncReply {
        AsyncReply::new::<R>(result, self.token)
    }
}

#[derive(Clone)]
pub struct AsyncReply {
    token: Token,
    success: bool,
    bytes: Vec<u8>,
}

impl AsyncReply {
    fn new<R: Request>(result: Result<R::Success, R::Error>, token: Token) -> Self {
        let success = result.is_ok();
        let bytes = match result {
            Ok(s) => s.encode(),
            Err(e) => e.encode(),
        };

        AsyncReply {
            token: token,
            success: success,
            bytes: bytes,
        }
    }
}

pub struct AsyncHandler {
    generator: Generator<Token>,
    waiting: BTreeMap<Token, Box<FnMut(AsyncReply) + Send>>,
}

impl AsyncHandler {
    pub fn new() -> Self {
        AsyncHandler {
            generator: Generator::new(),
            waiting: BTreeMap::new(),
        }
    }

    pub fn submit<R: Request>(&mut self, r: R) -> (AsyncReq<R>, AsyncResult<R::Success, R::Error>) {
        let token = self.generator.generate();
        let req = AsyncReq {
            token: token,
            request: r,
        };

        let (tx, rx) = promise::<R>();

        let mut tx = Some(tx);
        let fnbox = Box::new(move |mut reply: AsyncReply| {
            assert_eq!(reply.token, token);

            // FIXME: a bit of a hack since partial decoding is not supported
            let result = if reply.success {
                let success = <R as Request>::Success::decode(&mut reply.bytes)
                                .expect("failed to decode successful reply");
                Ok(success)
            } else {
                let error = <R as Request>::Error::decode(&mut reply.bytes)
                                .expect("failed to decode error reply");
                Err(error)
            };

            // FIXME: work around the fact that FnBox is still not a thing
            tx.take().expect("reply handler called twice?!").result(result);
        });

        self.waiting.insert(token, fnbox);

        (req, rx)
    }

    pub fn resolve(&mut self, reply: AsyncReply) {
        self.waiting
            .remove(&reply.token)
            .map(move |mut notify| notify(reply))
            .expect("unexpected reply!");
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
        let reply = AsyncReply::new::<R>(result, self.token);
        self.tx.send::<AsyncReply>(&reply);
    }
}

pub fn handoff<R: Request>(req: AsyncReq<R>, tx: Sender) -> (R, Handoff<R>) {
    (req.request, Handoff::new(tx, req.token))
}

unsafe_abomonate!(Token);
unsafe_abomonate!(AsyncReply: token, success, bytes);

impl<R: Request + Abomonation> Abomonation for AsyncReq<R> {
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


impl<R: Request> Deref for AsyncReq<R> {
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

        let reply2 = AsyncReply::new::<()>(Ok(1337), req2.token);
        async.resolve(reply2.clone());
        assert_eq!(rx2.await(), Ok(1337));

        let reply1 = AsyncReply::new::<()>(Err(false), req1.token);
        async.resolve(reply1);
        assert_eq!(rx1.await(), Err(false));
    }
}
