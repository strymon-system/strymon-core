use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::{Error as IoError, Result as IoResult, ErrorKind};
use std::str::from_utf8;
use std::collections::HashMap;
use std::marker::PhantomData;

use futures::{self, Future, Async, Complete, Poll};
use futures::stream::{Stream, Fuse};
use void::Void;
use byteorder::{NetworkEndian, ByteOrder};

use network::service::{Sender, Receiver};
use async::queue;

use network::message::{Encode, Decode};
use network::message::buf::MessageBuf;

pub trait Request: Encode + Decode + 'static {
    type Success: Encode + Decode + 'static;
    type Error: Encode + Decode + 'static;

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

impl Encode for Token {
    type EncodeError = Void;

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Ok(encode_u32(self.0, bytes))
    }
}

impl Decode for Token {
    type DecodeError = IoError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        Ok(Token(decode_u32(bytes)?))
    }
}

#[derive(Copy, Clone)]
enum Type {
    Request,
    Response(bool)
}

impl Encode for Type {
    type EncodeError = Void;

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        let num = match *self {
            Type::Request => 0,
            Type::Response(true) => 1,
            Type::Response(false) => 2,
        };
        Ok(encode_u32(num, bytes))
    }
}

impl Decode for Type {
    type DecodeError = IoError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        let ty = decode_u32(bytes)?;
        match ty {
            0 => Ok(Type::Request),
            1 => Ok(Type::Response(true)),
            2 => Ok(Type::Response(false)),
            _ => Err(IoError::new(ErrorKind::InvalidData, "invalid req/resp type"))
        }
    }
}

struct Name(String);

impl Encode for Name {
    type EncodeError = Void;

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Ok(bytes.extend_from_slice(self.0.as_bytes()))
    }
}

impl Decode for Name {
    type DecodeError = IoError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        match from_utf8(bytes) {
            Ok(s) => Ok(Name(String::from(s))),
            Err(e) => Err(IoError::new(ErrorKind::InvalidData, e))
        }
    }
}

pub struct RequestBuf {
    token: Token,
    name: Name,
    origin: Sender,
    msg: MessageBuf,
}

impl RequestBuf {
    pub fn name(&self) -> &str {
        &self.name.0
    }
    
    pub fn decode<R: Request>(mut self) -> Result<(R, Responder<R>), R::DecodeError> {
        let payload = self.msg.pop::<R>()?;
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
    pub fn success(self, s: R::Success) -> Result<(), <R::Success as Encode>::EncodeError> {
        let mut msg = MessageBuf::empty();
        msg.push(&Type::Response(true)).unwrap();
        msg.push(&self.token).unwrap();
        msg.push(&s)?;
        Ok(self.origin.send(msg))
    }

    pub fn error(self, e: R::Error) -> Result<(), <R::Error as Encode>::EncodeError> {
        let mut msg = MessageBuf::empty();
        msg.push(&Type::Response(false)).unwrap();
        msg.push(&self.token).unwrap();
        msg.push(&e)?;
        Ok(self.origin.send(msg))
    }
    
    pub fn respond<E>(self, res: Result<R::Success, R::Error>) -> Result<(), E>
        where R::Success: Encode<EncodeError=E>,
              R::Error: Encode<EncodeError=E>
    {
        match res {
            Ok(s) => self.success(s),
            Err(e) => self.error(e),
        }
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

type Pending = Complete<(MessageBuf, bool)>;

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
                },
                Async::Ready(None) => {
                    // all sender dropped, no more outgoing
                    break;
                },
                Async::NotReady => {
                    // currently nothing to do, try again later
                    break;
                },
            }
        }
    }

    fn do_incoming(&mut self, mut msg: MessageBuf) -> IoResult<Option<RequestBuf>> {
        let ty = msg.pop::<Type>()?;
        let token = msg.pop::<Token>()?;
        match ty {
            Type::Request => {
                // yield current request
                let name = msg.pop::<Name>()?;
                let buf = RequestBuf {
                    token: token,
                    name: name,
                    origin: self.sender.clone(),
                    msg: msg,
                };

                Ok(Some(buf))
            },
            Type::Response(success) => {
                // resolve one response, continue loop
                if let Some(pending) = self.pending.remove(&token) {
                    pending.complete((msg, success));
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
                        return Ok(Async::Ready(Some(req)))
                    }
                },
                Async::Ready(None) => {
                    // network closed, nothing to be resolved anymore
                    return Ok(Async::Ready(None));
                },
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

    pub fn send<R: Request>(&self, r: R) -> Result<Response<R>, R::EncodeError> {
        let token = self.next_token();
        let (tx, rx) = futures::oneshot();
        let mut msg = MessageBuf::empty();
        msg.push(&Type::Request).unwrap();
        msg.push(&token).unwrap();
        msg.push(&Name(String::from(R::name()))).unwrap();
        msg.push(&r)?;

        // if send fails, the response will signal this
        drop(self.tx.send(Ok((msg, token, tx))));
        
        fn other(msg: &'static str) -> IoError {
            IoError::new(ErrorKind::Other, msg)
        }

        let rx = rx.map_err(|_| Err(other("request canceled")));
        let rx = rx.and_then(|(mut msg, success)| {
            if success {
                msg.pop::<R::Success>()
                   .map_err(|_| Err(other("unable to decode response")))
            } else {
                let err = msg.pop::<R::Error>()
                            .map_err(|_| other("unable to decode response"));
                Err(err)
            }
        });

        Ok(Response {
            rx: Box::new(rx),
        })
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Response<R: Request> {
    rx: Box<Future<Item=R::Success, Error=Result<<R as Request>::Error, IoError>>>,
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
    use async;
    use async::dowhile::*;
    use futures::{self, Future};
    use futures::stream::Stream;
    use abomonation::Abomonation;
    use network::reqresp::{multiplex, Request, Incoming, Outgoing};
    use network::message::abomonate::{Crypt, CryptSerialize};
    use network::service::Service;

    fn assert_io<F: FnOnce() -> ::std::io::Result<()>>(f: F) {
        f().expect("I/O test failed")
    }

    #[derive(Clone)] struct Ping(i32);
    #[derive(Clone)] struct Pong(i32);
    unsafe_abomonate!(Ping);
    unsafe_abomonate!(Pong);
    impl CryptSerialize for Ping {}
    impl CryptSerialize for Pong {}
    impl Request for Ping {
        type Success = Pong;
        type Error = Pong;

        fn name() -> &'static str { "Ping" }
    }

    #[test]
    fn simple_ping() {

        assert_io(|| {
            let service = Service::init(None)?;
            let listener = service.listen(None)?;

            let conn = service.connect(listener.external_addr())?;
            let server = listener.map(multiplex).do_while(|(tx, rx)| {
                let handler = rx.do_while(move |req| {
                    assert_eq!(req.name(), "Ping");
                    let (req, resp) = req.decode::<Ping>().unwrap();
                    resp.respond(Ok(Pong(req.0))).unwrap();

                    Err(Stop::Terminate)
                }).map_err(|e| Err(e).unwrap() );

                async::spawn(handler);

                Err(Stop::Terminate)
            }).map_err(|e| Err(e).unwrap() );

            let (tx, rx) = multiplex(conn);
            let client = rx
                .for_each(|req| panic!("request on client"))
                .map_err(|err| println!("client finished with error: {:?}", err));

            let done = futures::lazy(move || {
                async::spawn(server);
                async::spawn(client);

                tx.send(Ping(5)).unwrap().and_then(move |pong| {
                    assert_eq!(pong.0, 5);
                    Ok(())
                }).map_err(|_| panic!("got ping error"))
            });

            async::finish(done)
        });
    }
}
