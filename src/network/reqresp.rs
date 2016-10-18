use abomonation::Abomonation;
use async::queue;
use byteorder::{NetworkEndian, ByteOrder};

use futures::{self, Future, Async, Complete, Poll};
use futures::stream::{Stream, Fuse};

use network::message::{Encode, Decode};
use network::message::abomonate::{Abomonate, NonStatic};
use network::message::buf::MessageBuf;

use network::{Sender, Receiver};
use std::any::Any;
use std::collections::HashMap;
use std::io::{Error as IoError, Result as IoResult, ErrorKind};
use std::marker::PhantomData;
use std::str::from_utf8;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use void::Void;

pub trait Request: Abomonation + Any + Clone + NonStatic {
    type Success: Abomonation + Any + Clone + NonStatic;
    type Error: Abomonation + Any + Clone + NonStatic;

    fn name() -> &'static str;
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
struct Token(u32);

fn encode_u32(i: u32, bytes: &mut Vec<u8>) {
    let mut buf = [0; 4];
    NetworkEndian::write_u32(&mut buf, i);
    bytes.extend_from_slice(&buf);
}

fn decode_u32(bytes: &[u8]) -> Result<u32, IoError> {
    if bytes.len() == 4 {
        Ok(NetworkEndian::read_u32(bytes))
    } else {
        Err(IoError::new(ErrorKind::UnexpectedEof, "not enough bytes for u32"))
    }
}

impl Encode<Token> for Token {
    type EncodeError = Void;

    fn encode(token: &Token, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Ok(encode_u32(token.0, bytes))
    }
}

impl Decode<Token> for Token {
    type DecodeError = IoError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        Ok(Token(decode_u32(bytes)?))
    }
}

#[derive(Copy, Clone)]
#[repr(u32)]
enum Type {
    Request = 0,
    Response = 1,
}

impl Encode<Type> for Type {
    type EncodeError = Void;

    fn encode(ty: &Type, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Ok(encode_u32(*ty as u32, bytes))
    }
}

impl Decode<Type> for Type {
    type DecodeError = IoError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        let ty = decode_u32(bytes)?;
        match ty {
            0 => Ok(Type::Request),
            1 => Ok(Type::Response),
            _ => Err(IoError::new(ErrorKind::InvalidData, "invalid req/resp type")),
        }
    }
}

struct Name;

impl Encode<&'static str> for Name {
    type EncodeError = Void;

    fn encode(s: &&'static str, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Ok(bytes.extend_from_slice(s.as_bytes()))
    }
}

impl Decode<String> for Name {
    type DecodeError = IoError;

    fn decode(bytes: &mut [u8]) -> Result<String, Self::DecodeError> {
        match from_utf8(bytes) {
            Ok(s) => Ok(String::from(s)),
            Err(e) => Err(IoError::new(ErrorKind::InvalidData, e)),
        }
    }
}

pub struct RequestBuf {
    token: Token,
    name: String,
    origin: Sender,
    msg: MessageBuf,
}

impl RequestBuf {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn decode<R: Request>(mut self) -> Result<(R, Responder<R>), IoError> {
        let payload = self.msg
            .pop::<Abomonate, R>()
            .map_err(|err| {
                IoError::new(ErrorKind::InvalidData,
                             format!("unable to decode request: {:?}", err))
            })?;
        let responder = Responder {
            token: self.token,
            origin: self.origin,
            marker: PhantomData,
        };

        Ok((payload, responder))
    }
}

pub struct Responder<R: Request> {
    token: Token,
    origin: Sender,
    marker: PhantomData<R>,
}

impl<R: Request> Responder<R> {
    pub fn respond(self, res: Result<R::Success, R::Error>) {
        let mut msg = MessageBuf::empty();
        msg.push::<Type, _>(&Type::Response).unwrap();
        msg.push::<Token, _>(&self.token).unwrap();
        msg.push::<Abomonate, _>(&res).unwrap();
        self.origin.send(msg)
    }
}

pub fn multiplex((tx, rx): (Sender, Receiver)) -> (Outgoing, Incoming) {
    let (outgoing_tx, outgoing_rx) = queue::channel();

    let outgoing = Outgoing {
        token: Default::default(),
        tx: outgoing_tx,
    };

    let incoming = Incoming {
        pending: HashMap::new(),
        outgoing: outgoing_rx.fuse(),
        sender: tx,
        receiver: rx,
    };

    (outgoing, incoming)
}

type Pending = Complete<MessageBuf>;

#[must_use = "streams do nothing unless polled"]
pub struct Incoming {
    pending: HashMap<Token, Pending>,
    outgoing: Fuse<queue::Receiver<(MessageBuf, Token, Pending), Void>>,
    sender: Sender,
    receiver: Receiver,
}

impl Incoming {
    fn do_outgoing(&mut self) {
        loop {
            match self.outgoing.poll().unwrap() {
                Async::Ready(Some((msg, token, pending))) => {
                    // send out outgoing request, register pending response
                    if let Some(_) = self.pending.insert(token, pending) {
                        panic!("invalid token reuse: {:?}", token);
                    }
                    self.sender.send(msg);
                }
                Async::Ready(None) => {
                    // all sender dropped, no more outgoing
                    break;
                }
                Async::NotReady => {
                    // currently nothing to do, try again later
                    break;
                }
            }
        }
    }

    fn do_incoming(&mut self, mut msg: MessageBuf) -> IoResult<Option<RequestBuf>> {
        let ty = msg.pop::<Type, Type>()?;
        let token = msg.pop::<Token, Token>()?;
        match ty {
            Type::Request => {
                // yield current request
                let name = msg.pop::<Name, String>()?;
                let buf = RequestBuf {
                    token: token,
                    name: name,
                    origin: self.sender.clone(),
                    msg: msg,
                };

                Ok(Some(buf))
            }
            Type::Response => {
                // resolve one response, continue loop
                if let Some(pending) = self.pending.remove(&token) {
                    pending.complete(msg);
                } else {
                    info!("dropping canceled response for {:?}", token)
                }

                Ok(None)
            }
        }
    }

    fn garbage_collect_canceled(&mut self) {
        // TODO(swicki) poll_cancel
    }
}

impl Stream for Incoming {
    type Item = RequestBuf;
    type Error = IoError;

    fn poll(&mut self) -> Poll<Option<RequestBuf>, IoError> {
        loop {
            if !self.outgoing.is_done() {
                self.do_outgoing();
            }

            match self.receiver.poll()? {
                Async::Ready(Some(msg)) => {
                    if let Some(req) = self.do_incoming(msg)? {
                        return Ok(Async::Ready(Some(req)));
                    }
                }
                Async::Ready(None) => {
                    // network closed, nothing to be resolved anymore
                    return Ok(Async::Ready(None));
                }
                Async::NotReady => {
                    // do a garbage collection cycle on canceled pending requests
                    self.garbage_collect_canceled();
                    return Ok(Async::NotReady);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct Outgoing {
    token: Arc<AtomicUsize>,
    tx: queue::Sender<(MessageBuf, Token, Pending), Void>,
}

impl Outgoing {
    fn next_token(&self) -> Token {
        Token(self.token.fetch_add(1, Ordering::SeqCst) as u32)
    }

    pub fn request<R: Request>(&self, r: &R) -> Response<R> {
        let token = self.next_token();
        let (tx, rx) = futures::oneshot();
        let mut msg = MessageBuf::empty();
        msg.push::<Type, _>(&Type::Request).unwrap();
        msg.push::<Token, _>(&token).unwrap();
        msg.push::<Name, &'static str>(&R::name()).unwrap();
        msg.push::<Abomonate, R>(r).unwrap();

        // if send fails, the response will signal this
        drop(self.tx.send(Ok((msg, token, tx))));

        let rx = rx.map_err(|_| Err(IoError::new(ErrorKind::Other, "request canceled")));
        let rx = rx.and_then(|mut msg| {
            let res = msg.pop::<Abomonate, Result<R::Success, R::Error>>()
                .map_err(|_| IoError::new(ErrorKind::Other, "unable to decode response"));
            match res {
                Ok(Ok(o)) => Ok(o),
                Ok(Err(e)) => Err(Ok(e)),
                Err(e) => Err(Err(e)),
            }
        });

        Response { rx: Box::new(rx) }
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Response<R: Request> {
    rx: Box<Future<Item = R::Success, Error = Result<<R as Request>::Error, IoError>>>,
}

impl<R: Request> Future for Response<R> {
    type Item = R::Success;
    type Error = Result<<R as Request>::Error, IoError>;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        self.rx.poll()
    }
}

#[cfg(test)]
mod tests {
    use abomonation::Abomonation;
    use async;
    use async::do_while::*;
    use futures::{self, Future};
    use futures::stream::Stream;
    use network::reqresp::{multiplex, Request};
    use network::Network;

    fn assert_io<F: FnOnce() -> ::std::io::Result<()>>(f: F) {
        f().expect("I/O test failed")
    }

    #[derive(Clone)]
    struct Ping(i32);
    #[derive(Clone)]
    struct Pong(i32);
    unsafe_abomonate!(Ping);
    unsafe_abomonate!(Pong);
    impl Request for Ping {
        type Success = Pong;
        type Error = ();

        fn name() -> &'static str {
            "Ping"
        }
    }

    #[test]
    fn simple_ping() {

        assert_io(|| {
            let network = Network::init(None)?;
            let listener = network.listen(None)?;

            let conn = network.connect(listener.external_addr())?;
            let server = listener.map(multiplex)
                .do_while(|(_, rx)| {
                    let handler = rx.do_while(move |req| {
                            assert_eq!(req.name(), "Ping");
                            let (req, resp) = req.decode::<Ping>().unwrap();
                            resp.respond(Ok(Pong(req.0 + 1)));

                            Err(Stop::Terminate)
                        })
                        .map_err(|e| Err(e).unwrap());

                    async::spawn(handler);

                    Err(Stop::Terminate)
                })
                .map_err(|e| Err(e).unwrap());

            let (tx, rx) = multiplex(conn);
            let client = rx.for_each(|_| panic!("request on client"))
                .map_err(|err| println!("client finished with error: {:?}", err));

            let done = futures::lazy(move || {
                async::spawn(server);
                async::spawn(client);

                tx.request(&Ping(5))
                    .and_then(move |pong| {
                        assert_eq!(pong.0, 6);
                        Ok(())
                    })
                    .map_err(|_| panic!("got ping error"))
            });

            async::finish(done)
        });
    }
}
