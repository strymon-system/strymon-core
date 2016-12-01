use std::any::Any;
use std::collections::HashMap;
use std::io::{Error as IoError, Result as IoResult, ErrorKind};
use std::net::{TcpListener, TcpStream, Shutdown, ToSocketAddrs};
use std::marker::PhantomData;
use std::str::from_utf8;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use abomonation::Abomonation;

use byteorder::{NetworkEndian, ByteOrder};

use futures::{self, Future, Async, Complete, Poll};
use futures::stream::{self, Stream};

use async::queue;
use network::message::{Encode, Decode};
use network::message::abomonate::{Abomonate, NonStatic};
use network::message::buf::{MessageBuf, read};
use network::{Sender, Network, accept};

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
        let payload = self.msg.pop::<Abomonate, R>().map_err(Into::<IoError>::into)?;
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

type Pending = Complete<MessageBuf>;

#[must_use = "futures do nothing unless polled"]
pub struct Response<R: Request> {
    rx: Box<Future<Item = R::Success, Error = Result<<R as Request>::Error, IoError>>>,
    pending: Arc<Mutex<HashMap<Token, Pending>>>,
    token: Token,
}

impl<R: Request> Response<R> {
    pub fn wait_unwrap(self) -> Result<R::Success, R::Error> {
        self.map_err(|e| e.expect("request failed with I/O error")).wait()
    }
}

impl<R: Request> Future for Response<R> {
    type Item = R::Success;
    type Error = Result<<R as Request>::Error, IoError>;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        self.rx.poll()
    }
}

impl<R: Request> Drop for Response<R> {
    fn drop(&mut self) {
        // cancel pending response (if not yet completed)
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&self.token);
        }
    }
}

#[derive(Clone)]
pub struct Outgoing {
    token: Arc<AtomicUsize>,
    pending: Arc<Mutex<HashMap<Token, Pending>>>,
    sender: Sender,
}

impl Outgoing {
    fn next_token(&self) -> Token {
        Token(self.token.fetch_add(1, Ordering::SeqCst) as u32)
    }

    pub fn request<R: Request>(&self, r: &R) -> Response<R> {
        let token = self.next_token();
        let (tx, rx) = futures::oneshot();
        
        // step 1: create request packet
        let mut msg = MessageBuf::empty();
        msg.push::<Type, _>(&Type::Request).unwrap();
        msg.push::<Token, _>(&token).unwrap();
        msg.push::<Name, &'static str>(&R::name()).unwrap();
        msg.push::<Abomonate, R>(r).unwrap();
        
        // step 2: add completion handle for pending responses
        {
            let mut pending = self.pending.lock().expect("request thread panicked");
            pending.insert(token, tx);
        }

        // step 3: send packet to network
        self.sender.send(msg);
        
        // step 4: prepare response decoder
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

        Response {
            rx: Box::new(rx),
            pending: self.pending.clone(),
            token: token,
        }
    }
}

pub struct Incoming {
    receiver: queue::Receiver<RequestBuf, IoError>,
}

impl Stream for Incoming {
    type Item = RequestBuf;
    type Error = IoError;

    fn poll(&mut self) -> Poll<Option<RequestBuf>, IoError> {
        self.receiver.poll()
    }
}

struct Resolver {
    incoming: queue::Sender<RequestBuf, IoError>,
    pending: Arc<Mutex<HashMap<Token, Pending>>>,
    sender: Sender,
    stream: TcpStream,
}

impl Resolver {
    fn decode(&mut self, mut msg: MessageBuf) -> IoResult<()> {
        let ty = msg.pop::<Type, Type>()?;
        let token = msg.pop::<Token, Token>()?;
        match ty {
            Type::Request => {
                let name = msg.pop::<Name, String>()?;
                let buf = RequestBuf {
                    token: token,
                    name: name,
                    origin: self.sender.clone(),
                    msg: msg,
                };

                if let Err(_) = self.incoming.send(Ok(buf)) {
                    error!("incoming request queue dropped, ignoring request");
                }
            }
            Type::Response => {
                let mut pending = self.pending.lock().unwrap();
                if let Some(handle) = pending.remove(&token) {
                    handle.complete(msg);
                } else {
                    info!("dropping canceled response for {:?}", token);
                }
            }
        }
        
        Ok(())
    }

    fn dispatch(mut self) {
        thread::spawn(move || {
            loop {
                let res = match read(&mut self.stream) {
                    Ok(Some(message)) => self.decode(message),
                    Ok(None) => break,
                    Err(err) => Err(err),
                };
                
                // make sure to announce any decoding errors to client
                if let Err(err) = res {
                    let _ = self.incoming.send(Err(err));
                    break;
                }
            }

            drop(self.stream.shutdown(Shutdown::Both));
        });
    }
}

pub struct Server {
    external: Arc<String>,
    port: u16,
    rx: stream::Receiver<(Outgoing, Incoming), IoError>,
}

impl Server {
    // TODO(swicki) could this be merged with network::Listener?
    fn new(network: Network, port: u16) -> IoResult<Self> {
        let sockaddr = ("0.0.0.0", port);
        let listener = TcpListener::bind(&sockaddr)?;
        let external = network.external.clone();
        let port = listener.local_addr()?.port();
        let rx = accept(listener, multiplex);

        Ok(Server {
            external: external,
            port: port,
            rx: rx,
        })
    }

    pub fn external_addr(&self) -> (&str, u16) {
        (&*self.external, self.port)
    }
}

impl Stream for Server {
    type Item = (Outgoing, Incoming);
    type Error = IoError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.rx.poll()
    }
}

fn multiplex(stream: TcpStream) -> IoResult<(Outgoing, Incoming)> {
    let instream = stream.try_clone()?;
    let outstream = stream;

    let (incoming_tx, incoming_rx) = queue::channel(); 
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let sender = Sender::new(outstream);

    let resolver = Resolver {
        pending: pending.clone(),
        sender: sender.clone(),
        incoming: incoming_tx,
        stream: instream,
    };
    
    let outgoing = Outgoing {
        token: Default::default(),
        pending: pending,
        sender: sender,
    };

    let incoming = Incoming {
        receiver: incoming_rx,
    };

    resolver.dispatch();

    Ok((outgoing, incoming))
}

impl Network {
    pub fn client<E: ToSocketAddrs>(&self, endpoint: E) -> IoResult<(Outgoing, Incoming)> {
        multiplex(TcpStream::connect(endpoint)?)
    }
    
    pub fn server<P: Into<Option<u16>>>(&self, port: P) -> IoResult<Server> {
        Server::new(self.clone(), port.into().unwrap_or(0))
    }
}

fn _assert() {
    fn _is_send<T: Send>() {}
    _is_send::<Incoming>();
    _is_send::<Outgoing>();
    _is_send::<Server>();
}

#[cfg(test)]
mod tests {
    use abomonation::Abomonation;
    use async;
    use async::do_while::*;
    use futures::{self, Future};
    use network::reqrep::Request;
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
            let server = network.server(None)?;

            let (tx, _) = network.client(server.external_addr())?;
            let server = 
                server.do_while(|(_, rx)| {
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

            let done = futures::lazy(move || {
                async::spawn(server);

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
