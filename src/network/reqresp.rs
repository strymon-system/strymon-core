use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::io::{Error, ErrorKind};
use std::str::from_utf8;

use std::mem;

use futures::{self, Future, Async, Oneshot};
use futures::stream::{self, Stream};
use void::Void;
use byteorder::{NetworkEndian, ByteOrder};

use network::service::{Sender as MsgSender, Receiver as MsgReceiver};

use network::message::{Encode, Decode};
use network::message::buf::MessageBuf;
use network::message::abomonate::VaultMessage;

pub trait Request: Encode + Decode {
    type Success: Encode + Decode;
    type Error: Encode + Decode;

    fn name() -> &'static str;
}

struct Token(u32);

fn encode_u32(i: u32, bytes: &mut Vec<u8>) {
    let mut buf = [0; 4];
    NetworkEndian::write_u32(&mut buf, i);
    bytes.extend_from_slice(&buf);
}

fn decode_u32(bytes: &[u8]) -> Result<u32, Error> {
    if bytes.len() == 4 {
        Ok(NetworkEndian::read_u32(bytes))
    } else {
        Err(Error::new(ErrorKind::UnexpectedEof, "not enough bytes for u32"))
    }
}

impl Encode for Token {
    type EncodeError = Void;

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Ok(encode_u32(self.0, bytes))
    }
}

impl Decode for Token {
    type DecodeError = Error;

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
    type DecodeError = Error;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        let ty = decode_u32(bytes)?;
        match ty {
            0 => Ok(Type::Request),
            1 => Ok(Type::Response),
            _ => Err(Error::new(ErrorKind::InvalidData, "invalid req/resp type"))
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
    type DecodeError = Error;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        match from_utf8(bytes) {
            Ok(s) => Ok(Name(String::from(s))),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e))
        }
    }
}

pub struct RequestBuf {
    token: Token,
    name: Name,
    origin: MsgSender,
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

fn test(){
    let rx: MsgReceiver = unimplemented!();
    rx.and_then(|mut msg| {
        let ty = msg.pop::<Type>()?;
        let token = msg.pop::<Token>()?;
        let name = msg.pop::<Name>()?;
        match ty {
            Type::Request => {
                
                Ok(())
            },
            Type::Response => {
                Ok(())
            }
        }
    });
}

pub struct Sender {
    
}

impl Sender {
    pub fn send<R: Request>(r: R) -> Response<R> {
        unimplemented!()
    }
}

pub struct Receiver {

}


pub struct Response<R: Request> {
    rx: Oneshot<Result<R::Success, Result<<R as Request>::Error, Error>>>,
}

impl<R: Request> Future for Response<R> {
    type Item = R::Success;
    type Error = Result<<R as Request>::Error, Error>;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(Ok(i))) => Ok(Async::Ready(i)),
            Ok(Async::Ready(Err(e))) => Err(e),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_) => Err(Err(Error::new(ErrorKind::Other, "request canceled")))
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
