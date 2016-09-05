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
    pub fn response(self, result: Result<R::Success, R::Error>) -> AsyncResponse {
        AsyncResponse::new::<R>(result, self.token)
    }
}

#[derive(Clone)]
pub struct AsyncResponse {
    token: Token,
    success: bool,
    bytes: Vec<u8>,
}

impl AsyncResponse {
    fn new<R: Request>(result: Result<R::Success, R::Error>, token: Token) -> Self {
        let success = result.is_ok();
        let bytes = match result {
            Ok(s) => s.encode(),
            Err(e) => e.encode(),
        };

        AsyncResponse {
            token: token,
            success: success,
            bytes: bytes,
        }
    }
}

pub struct AsyncHandler {
    generator: Generator<Token>,
    waiting: BTreeMap<Token, Box<FnMut(AsyncResponse) + Send>>,
}

impl AsyncHandler {
    pub fn new() -> Self {
        AsyncHandler {
            generator: Generator::new(),
            waiting: BTreeMap::new(),
        }
    }

    pub fn submit<R: Request>(&mut self, r: R, tx: Complete<R>) -> AsyncReq<R> {
        let token = self.generator.generate();
        let req = AsyncReq {
            token: token,
            request: r,
        };

        let mut tx = Some(tx);
        let fnbox = Box::new(move |mut response: AsyncResponse| {
            assert_eq!(response.token, token);

            // FIXME: a bit of a hack since partial decoding is not supported
            let result = if response.success {
                let success = <R as Request>::Success::decode(&mut response.bytes)
                    .expect("failed to decode successful response");
                Ok(success)
            } else {
                let error = <R as Request>::Error::decode(&mut response.bytes)
                    .expect("failed to decode error response");
                Err(error)
            };

            // FIXME: work around the fact that FnBox is still not a thing
            tx.take().expect("response handler called twice?!").result(result);
        });

        self.waiting.insert(token, fnbox);

        req
    }

    pub fn resolve(&mut self, response: AsyncResponse) {
        self.waiting
            .remove(&response.token)
            .map(move |mut notify| notify(response))
            .expect("unexpected response!");
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
        let response = AsyncResponse::new::<R>(result, self.token);
        self.tx.send::<AsyncResponse>(&response);
    }
}

pub fn handoff<R: Request>(req: AsyncReq<R>, tx: Sender) -> (R, Handoff<R>) {
    (req.request, Handoff::new(tx, req.token))
}

unsafe_abomonate!(Token);
unsafe_abomonate!(AsyncResponse: token, success, bytes);

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
    use messaging::request;

    #[test]
    fn async() {
        let mut async = AsyncHandler::new();

        let (tx1, rx1) = request::promise::<()>();
        let req1 = async.submit((), tx1);
        let (tx2, rx2) = request::promise::<()>();
        let req2 = async.submit((), tx2);

        let response2 = AsyncResponse::new::<()>(Ok(1337), req2.token);
        async.resolve(response2.clone());
        assert_eq!(rx2.await(), Ok(1337));

        let response1 = AsyncResponse::new::<()>(Err(false), req1.token);
        async.resolve(response1);
        assert_eq!(rx1.await(), Err(false));
    }
}
