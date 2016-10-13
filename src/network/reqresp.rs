use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::{Error as IoError, Result as IoResult, ErrorKind};
use std::str::from_utf8;
use std::collections::HashMap;

use std::mem;

use futures::{self, Future, Async, Oneshot, Complete, Poll};
use futures::stream::{self, Stream};
use void::{self, Void};
use byteorder::{NetworkEndian, ByteOrder};

use network::service::{Sender, Receiver};
use async::queue;

use network::message::{Encode, Decode};
use network::message::buf::MessageBuf;
use network::message::abomonate::VaultMessage;

pub trait Request: Encode + Decode {
    type Success: Encode + Decode;
    type Error: Encode + Decode;

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

#[repr(u32)]
#[derive(Copy, Clone)]
enum Type {
    Request = 0,
    Response = 1,
}

impl Encode for Type {
    type EncodeError = Void;

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Ok(encode_u32(*self as u32, bytes))
    }
}

impl Decode for Type {
    type DecodeError = IoError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        let ty = decode_u32(bytes)?;
        match ty {
            0 => Ok(Type::Request),
            1 => Ok(Type::Response),
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
    
    pub fn decode<R: Request>(mut self) -> (R, Responder<R>) {
        unimplemented!()
    }
}

pub struct Responder<R: Request> {
    r: R,
}

type Pending = Complete<Result<MessageBuf, IoError>>;

struct MuxState {
    pending: HashMap<Token, Pending>,
    requests: queue::Sender<RequestBuf, IoError>,
    network: Sender,    
}

impl MuxState {   
    fn resolve_pending(&mut self, token: Token, msg: MessageBuf) -> IoResult<()> {
        if let Some(pending) = self.pending.remove(&token) {
            Ok(pending.complete(Ok(msg)))
        } else {
            Err(IoError::new(ErrorKind::Other, "got unexpected response token"))
        }
    }

    fn handle_network(&mut self, mut msg: MessageBuf) -> IoResult<()> {
        let ty = msg.pop::<Type>()?;
        let token = msg.pop::<Token>()?;
        match ty {
            Type::Response => {
                self.resolve_pending(token, msg);
                Ok(())
            },
            Type::Request => {
                let name = msg.pop::<Name>()?;
                let origin = self.network.clone();
                let req = RequestBuf {
                    token: token,
                    name: name,
                    origin: origin,
                    msg: msg,
                };
                Ok(())
            }
        }
    }
    
    fn handle_pending(&mut self, p: Result<(MessageBuf, Token, Pending), Token>) -> IoResult<()> {
        match p {
            Ok((msg, token, pending)) => {
                let old = self.pending.insert(token, pending);
                self.network.send(msg);
                assert!(old.is_none(), "invalid token reuse");
            },
            Err(token) => {
                self.pending.remove(&token);
            }
        };
        Ok(())
    }
}

/*

    errors:
        - unable to parse message
        - invalid incoming request token
        - invalid incoming response token (!)
        

*/

pub struct Mux {
    inner: Box<Stream<Item=(), Error=IoError>>,
}

impl Mux {
    pub fn from((tx, mut rx): (Sender, Receiver)) -> (Self, (Outgoing, Incoming)) {
        let (incoming_tx, incoming_rx) = queue::channel();
        let (outgoing_tx, outgoing_rx) = queue::channel();

        let mut state = MuxState {
            pending: HashMap::new(),
            requests: incoming_tx,
            network: tx,
        };

        // pending is a stream of Ok(new_pending) or Err(canceled);
        let pending = outgoing_rx.map(Ok).or_else(|e| Ok(Err(e)));
        let inner = rx.merge(pending).and_then(move |event| {
            use ::futures::stream::MergedItem::*;
            match event {
                First(m) => state.handle_network(m),
                Second(p) => state.handle_pending(p),
                Both(m, p) => {
                    let m = state.handle_network(m);
                    let p = state.handle_pending(p);
                    // TODO(swicki): if m and p are an error, we ignore p here
                    m.and(p)
                }
            }
        });

        let outgoing = Outgoing { tx: outgoing_tx, token: Default::default() };
        let incoming = Incoming { rx: incoming_rx };
        let mux = Mux { inner: Box::new(inner) };
        (mux, (outgoing, incoming))         
    }
}

impl Stream for Mux {
    type Item = ();
    type Error = IoError;

    fn poll(&mut self) -> Poll<Option<()>, Self::Error> {
        self.inner.poll()
    }
}


pub struct Incoming {
    rx: queue::Receiver<RequestBuf, IoError>,
}

impl Stream for Incoming {
    type Item = RequestBuf;
    type Error = IoError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.rx.poll()
    }
}

/*pub fn from(){
    let mut pending: HashMap<Token, Complete<MessageBuf>> = HashMap::new();
    let rx: MsgReceiver = unimplemented!();
    rx.and_then(|mut msg| {
        let ty = msg.pop::<Type>()?;
        let token = msg.pop::<Token>()?;
        match ty {
            Type::Request => {
                let name = msg.pop::<Name>()?;
                // send to request handler
                Ok(())
            },
            Type::Response => {
                match pending.remove(&token) {
                    Some(c) => Ok(c.complete(msg)),
                    None => {
                        Err(Error::new(ErrorKind::Other, "invalid response token"))
                    }
                }
            }
        }
    });
}*/

#[derive(Clone)]
pub struct Outgoing {
    token: Arc<AtomicUsize>,
    tx: queue::Sender<(MessageBuf, Token, Pending), Token>,
}

impl Outgoing {
    pub fn send<R: Request>(r: R) -> Response<R> {
        unimplemented!()
    }
}


pub struct Response<R: Request> {
    rx: Oneshot<Result<R::Success, Result<<R as Request>::Error, IoError>>>,
}

// TODO on drop remove from hashmap

impl<R: Request> Future for Response<R> {
    type Item = R::Success;
    type Error = Result<<R as Request>::Error, IoError>;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(Ok(i))) => Ok(Async::Ready(i)),
            Ok(Async::Ready(Err(e))) => Err(e),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_) => Err(Err(IoError::new(ErrorKind::Other, "request canceled")))
        }
    }
}



/*

let (mux, rx) = ReqResp::from((tx, rx));
let tx = mux.sender();
let tx = mux.sender();
async::spawn(mux)



and_then(|msg| {
    rr.feed(msg)
})


(tx, rx) 

let (client, server) = 

let (rx, tx, driver) = ReqResp::from((tx, rx));

let foo = tx.send(Foo());


rx.and_then(move |req| {
    match req.name {
        Foo::name() => {
            coord.add_worker(
        }
    }
})

rr.request<R>(method, args) -> Response<Succ, Result<Err, Io>>
rr.incoming() -> Stream<Request, Io>

rr.request(Subscribe(foo, bar, c));



request.respond(result)
request.*/
