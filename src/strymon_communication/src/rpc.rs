// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Asynchronous remote procedure calls.
//!
//! This implements a small framework for receiving and sending requests and
//! responses between two connected peers.
//! During set-up, one of the peers acts as a *server*, listening on a server
//! socket to accept new incoming *clients*. Clients connect to a server
//! using the socket address.
//!
//! Once a connection between two peers is established, their initial role as
//! server and client becomes irrelevant, as they take on the role of either
//! being a *sender* or *receiver* (or both) of requests.
//!
//! # 1. Defining a custom protocol
//!
//! In order to use this framework, client code must first define the types
//! to be used for a request-response protocol. It is not using a custom
//! interface description language (IDL) but rather is based on implementing
//! certain Rust traits. Different types of requests must be distinguished
//! from each other through a value called the *name* (i.e. the name of the
//! remote procedure). Since the name is typically used for dispatching on the
//! receiver-side, it is recommended to use an `enum` for it.
//!
//! The name, argument and return types of a method are defined by implementing
//! the [`Request`](trait.Request.html) type. Each invocation can either return
//! successfully with the type specified in
//! [`Request::Success`](trait.Request.html#associatedtype.Success) or fail with
//! an application-specific error defined in
//! [`Request::Error`](trait.Request.html#associatedtype.Error).
//!
//! # 2. Connection set-up
//!
//! During connection set-up, one peer needs to act as a server. To instantiate
//! a server, invoke [`Network::server()`](struct.Network.html#method.server)
//! to obtain a handle for new incoming clients.
//!
//! The client is expected to call
//! [`Network::client()`](struct.Network.html#method.client) with the
//! corresponding socket address to connect to the server. Both peers will
//! obtain a pair of
//! ([`Incoming`](struct.Incoming.html), [`Outgoing`](struct.Outgoing.html))
//! queue handles. These are used to either *receive* incoming requests, or
//! *send* out outgoing requests.
//!
//! # 3. Handling requests \& responses
//!
//! Once connected, a peer might decide to send requests by invoking
//! [`Outgoing::request()`](struct.Outgoing.html#method.request) with an
//! argument implementing the [`Request`](trait.Request.html) trait. The
//! remote peer will receive this request on its
//! [`Incoming`](struct.Incoming.html) queue in the form of an encoded
//! [`RequestBuf`](struct.RequestBuf.html) object. To decode this object, it
//! is common to match on the method name returned by
//! [`RequestBuf::name()`](struct.RequestBuf.html#method.name) and then decode
//! the request payload using
//! [`RequestBuf::decode()`](struct.RequestBuf.html#method.decode). Decoding
//! a request successfully returns the decoded payload, as well as a
//! [`Responder`](struct.Responder.html) object which is used to send back the
//! response to the origin.
//!
//! Once the response arrives back at the original sender, the
//! [`Response`](struct.Response.html) future will resolve to the decoded
//! response.
//!
//! ## Examples:
//!
//! Please refer to `tests/calc.rs` for a complete example of how to use
//! this module.

use std::collections::HashMap;
use std::io::{self, ErrorKind};
use std::net::{TcpListener, TcpStream, Shutdown, ToSocketAddrs};
use std::marker::PhantomData;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::{Async, Poll};
use futures::future::Future;
use futures::stream::Stream;
use futures::sync::mpsc;
use futures::sync::oneshot;

use Network;
use transport;
use message::MessageBuf;

use serde::ser::Serialize;
use serde::de::DeserializeOwned;

/// A trait to distinguish remote procedure calls.
///
/// #Examples
/// ```
/// use strymon_communication::rpc::Name;
/// #[derive(Clone, Copy)]
/// #[repr(u8)]
/// pub enum MyRPC {
///    UselessCall = 1,
/// }
///
/// impl Name for MyRPC {
///     type Discriminant = u8;
///     fn discriminant(&self) -> Self::Discriminant {
///         *self as Self::Discriminant
///     }
///
///     fn from_discriminant(value: &Self::Discriminant) -> Option<Self> {
///         match *value {
///             1 => Some(MyRPC::UselessCall),
///             _ => None,
///         }
///     }
/// }
/// ```
pub trait Name: Send + Sized + 'static {

    /// The discriminant type representing `Self`.
    type Discriminant: Serialize + DeserializeOwned + 'static;

    /// Convert `Self` into a discriminant.
    fn discriminant(&self) -> Self::Discriminant;

    /// Restore `Self` from a discriminant. Returns `None` if `Self` cannot be restored.
    fn from_discriminant(&Self::Discriminant) -> Option<Self>;
}

/// A trait for defining the signature of a remote procedure.
///
/// Within this framework, a new request type can be defined by implementing
/// this trait. The type implementing `Request` stores the arguments sent to
/// the remote receiver and then has to respond with either `Ok(Request::Success)`
/// or `Err(Request::Error)`. In order for the receiver to be able to distinguish
/// the different incoming requests without fully decoding it, each request
/// type must define an associated constant `Request::NAME` for this purpose.
///
/// # Examples
/// ```rust,ignore
/// #[derive(Serialize, Deserialize, Clone, Debug)]
/// pub struct GetNamesForId {
///     pub arg: u32,
/// }
///
/// #[derive(Serialize, Deserialize, Clone, Debug)]
/// struct InvalidId;
///
/// // CustomRPC is an enum implementing the `Name` trait
/// impl Request<CustomRPC> for GetNamesForId {
///     // A unique identifier for this method
///     const NAME = CustomRPC::GetNamesForId;
///
///     // Return type of a successful response
///     type Success = Vec<String>;
///     // Return type of a failed invocation
///     type Error = InvalidId;
/// }
/// ```
pub trait Request<N: Name>: Serialize + DeserializeOwned {

    /// The type of a successful response.
    type Success: Serialize + DeserializeOwned;

    /// The type of a failed response.
    type Error: Serialize + DeserializeOwned;

    /// A unique value identifying this type of request.
    const NAME: N;
}

type RequestId = u32;

#[derive(Copy, Clone)]
#[repr(u8)]
enum Type {
    Request = 0,
    Response = 1,
}

impl Type {
    fn from_u8(num: u8) -> io::Result<Self> {
        match num {
            0 => Ok(Type::Request),
            1 => Ok(Type::Response),
            _ => Err(io::Error::new(ErrorKind::InvalidData, "invalid req/resp type")),
        }
    }
}

/// Receiver-side buffer containing an incoming request.
///
/// This type represents a request to be processed by a receiver. In order to
/// serve a request, the receiver code needs to identify the method by calling
/// [`name()`](#method.name), decode it using [`decode()`](#method.decode),
/// process it and respond using the obtained [`Responder`](struct.Responder.html).
///
/// # Examples:
/// Requests are received on the [`Incoming`](struct.Incoming.html) queue and
/// are then typically decoded using a `match` statement as follows:
///
/// ```rust,ignore
/// fn dispatch(&mut self, request: RequestBuf) -> io::Result<()> {
///     match request.name() {
///         &CustomRPC::Foo => {
///             let (args, responder) = request.decode::<Foo>()?;
///             let result = self.foo(args);
///             responder.respond(result);
///         },
///         &CustomRPC::Bar => {
///             // handle requests of type `Bar`…
///         },
///     };
///     Ok(())
/// }
/// ```
pub struct RequestBuf<N: Name> {
    id: RequestId,
    name: N,
    origin: transport::Sender,
    msg: MessageBuf,
    _n: PhantomData<N>,
}

impl<N: Name> RequestBuf<N> {
    /// Extracting the `Request::NAME` to identfy the request type.
    pub fn name(&self) -> &N {
        &self.name
    }
    /// Decodes the request into the request data and a responder handle.
    ///
    /// The returned [`Responder`](struct.Responder.html) handle is to be used
    /// to serve the decoded request `R`.
    pub fn decode<R: Request<N>>(mut self) -> io::Result<(R, Responder<N, R>)> {
        let payload = self.msg.pop::<R>()?;
        let responder = Responder {
            id: self.id,
            origin: self.origin,
            marker: PhantomData,
        };

        Ok((payload, responder))
    }
}

/// Receiver-side handle for responding to a given request.
///
/// Since the responder is bound to a given typed request, can only be
/// obtained through [`RequestBuf::decode()`](struct.RequestBuf.html#method.decode).
pub struct Responder<N: Name, R: Request<N>> {
    id: RequestId,
    origin: transport::Sender,
    marker: PhantomData<(N, R)>,
}

impl<N: Name, R: Request<N>> Responder<N, R> {
    /// Sends back the response to the client which submitted the request.
    pub fn respond(self, res: Result<R::Success, R::Error>) {
        let mut msg = MessageBuf::empty();
        msg.push(Type::Response as u8).unwrap();
        msg.push(self.id).unwrap();
        msg.push(res).unwrap();
        self.origin.send(msg)
    }
}

type Pending = oneshot::Sender<MessageBuf>;

/// Sender-side future eventually yielding the response for a request.
///
/// Upon successful completion, the future will yield `Ok(Request::Success)`.
/// Application-level errors occurring at the receiver-side yield a
/// `Err(Ok(Request::Error))` while networking errors will be returned as
/// `Err(Err(std::io::Error))`.
///
/// In order to obtain this type, one must first send out a request with
/// [`Outgoing::request()`](struct.Outgoing.html#method.request).
#[must_use = "futures do nothing unless polled"]
pub struct Response<N: Name, R: Request<N>> {
    rx: oneshot::Receiver<MessageBuf>,
    pending: Arc<Mutex<HashMap<RequestId, Pending>>>,
    id: RequestId,
    _request: PhantomData<(N, R)>,
}

impl<N: Name, R: Request<N>> Response<N, R> {
    /// Convenience-wrapper which performs a blocking wait on the response.
    ///
    /// # Panics
    /// This panics if a networking error occurs while waiting for the response.
    pub fn wait_unwrap(self) -> Result<R::Success, R::Error> {
        self.map_err(|e| e.expect("request failed with I/O error")).wait()
    }
}

impl<N: Name, R: Request<N>> Future for Response<N, R> {
    type Item = R::Success;
    type Error = Result<<R as Request<N>>::Error, io::Error>;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        match self.rx.poll() {
            Ok(Async::Ready(mut msg)) => {
                // decode the message
                match msg.pop::<Result<R::Success, R::Error>>() {
                    Ok(Ok(success)) => Ok(Async::Ready(success)),
                    Ok(Err(error)) => Err(Ok(error)),
                    Err(err) => Err(Err(io::Error::new(ErrorKind::Other, err))),
                }
            },
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(_) => Err(Err(io::Error::new(ErrorKind::Other, "request canceled"))),
        }
    }
}

impl<N: Name, R: Request<N>> Drop for Response<N, R> {
    fn drop(&mut self) {
        // cancel pending response (if not yet completed)
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&self.id);
        }
    }
}

/// Sender-side queue for sending out requests.
///
/// Requests can be sent to the remote peer using the
/// [`Outgoing::request()`](#method.request) method. As this method returns a
/// future for the response, a sender can send out many concurrent outgoing
/// requests without having to wait for responses first. This means that some
/// requests might return before others.
#[derive(Clone)]
pub struct Outgoing {
    next_id: Arc<AtomicUsize>,
    pending: Arc<Mutex<HashMap<RequestId, Pending>>>,
    sender: transport::Sender,
}

impl Outgoing {
    fn next_id(&self) -> RequestId {
        self.next_id.fetch_add(1, Ordering::SeqCst) as u32
    }

    /// Asynchronously sends out a request to the remote peer.
    ///
    /// Returns a future for the pending response. The next request can be
    /// submitted without having to wait for the previous response to arrive.
    pub fn request<N: Name, R: Request<N>>(&self, r: &R) -> Response<N, R> {
        let id = self.next_id();
        let (tx, rx) = oneshot::channel();

        // step 1: create request packet
        let mut msg = MessageBuf::empty();
        msg.push(Type::Request as u8).unwrap();
        msg.push(id).unwrap();
        msg.push(R::NAME.discriminant()).unwrap();
        msg.push::<&R>(r).unwrap();

        // step 2: add completion handle for pending responses
        {
            let mut pending = self.pending.lock().expect("request thread panicked");
            pending.insert(id, tx);
        }

        // step 3: send packet to network
        self.sender.send(msg);

        // step 4: prepare response decoder
        Response {
            rx: rx,
            pending: self.pending.clone(),
            id: id,
            _request: PhantomData,
        }
    }
}

/// Receiver-side queue of incoming requests.
///
/// This implements the `futures::stream::Stream` to yield encoded
/// [`RequestBuf`](struct.RequestBuf.html)s.
#[must_use = "futures do nothing unless polled"]
pub struct Incoming<N: Name> {
    rx: mpsc::UnboundedReceiver<Result<RequestBuf<N>, io::Error>>,
}

impl<N: Name> Stream for Incoming<N> {
    type Item = RequestBuf<N>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<RequestBuf<N>>, io::Error> {
        transport::poll_receiver(&mut self.rx)
    }
}

struct Resolver<N: Name> {
    incoming: mpsc::UnboundedSender<Result<RequestBuf<N>, io::Error>>,
    pending: Arc<Mutex<HashMap<RequestId, Pending>>>,
    sender: transport::Sender,
    stream: TcpStream,
}

impl<N: Name> Resolver<N> {
    /// decodes a message received on the incoming socket queue.
    fn decode(&mut self, mut msg: MessageBuf) -> io::Result<()> {
        let ty = msg.pop().and_then(Type::from_u8)?;
        let id = msg.pop::<RequestId>()?;
        match ty {
            // if we got a new request, forward it on the queue for incoming
            // requests and create an opaque requestbuf so the receiver can
            // try to decode it
            Type::Request => {
                let name = msg.pop::<N::Discriminant>()?;
                let name = N::from_discriminant(&name)
                    .and_then(|n| Some(Ok(n)))
                    .unwrap_or_else(|| Err(io::Error::new(ErrorKind::Other, "decoding discriminant failed")))?;
                let buf = RequestBuf {
                    id: id,
                    name: name,
                    origin: self.sender.clone(),
                    msg: msg,
                    _n: PhantomData,
                };

                // try to send to receiver
                if self.incoming.unbounded_send(Ok(buf)).is_err() {
                    error!("incoming request queue dropped, ignoring request");
                }
            }
            // if it was a response, we should have a pending response
            // handler waiting - find it and complete the pending request
            Type::Response => {
                let mut pending = self.pending.lock().unwrap();
                let completed = pending
                    .remove(&id)
                    .and_then(move |tx| tx.send(msg).ok())
                    .is_some();

                if !completed {
                    info!("dropping canceled response for {:?}", id);
                }
            }
        }

        Ok(())
    }

    // starts a dispatcher for incoming message and decide if they are
    // incoming requests or responses
    // TODO(swicki): add a timeout which removes old pending responses
    fn dispatch(mut self) {
        thread::spawn(move || {
            loop {
                let res = match MessageBuf::read(&mut self.stream) {
                    // got a full message, try to decode it
                    Ok(Some(message)) => self.decode(message),
                    // remote end closed connection, shut down this thread
                    Ok(None) => break,
                    // error while receiving, signal this to "incoming" queue
                    Err(err) => Err(err),
                };

                // make sure to announce any network errors to client
                if let Err(err) = res {
                    let _ = self.incoming.unbounded_send(Err(err));
                    break;
                }
            }

            drop(self.stream.shutdown(Shutdown::Both));
        });
    }
}

/// Handle for queue of newly connected peers.
#[must_use = "futures do nothing unless polled"]
pub struct Server<N: Name> {
    external: Arc<String>,
    port: u16,
    rx: mpsc::Receiver<io::Result<(Outgoing, Incoming<N>)>>,
}

impl<N: Name> Server<N> {
    // TODO(swicki) could this be merged with network::Listener?
    fn new(network: Network, port: u16) -> io::Result<Self> {
        let sockaddr = ("0.0.0.0", port);
        let listener = TcpListener::bind(&sockaddr)?;
        let external = network.hostname.clone();
        let port = listener.local_addr()?.port();
        let rx = transport::accept(listener, multiplex);

        Ok(Server {
            external: external,
            port: port,
            rx: rx,
        })
    }

    /// Returns the address of the socket in the form of `(hostname, port)`.
    pub fn external_addr(&self) -> (&str, u16) {
        (&*self.external, self.port)
    }
}

impl<N: Name> Stream for Server<N> {
    type Item = (Outgoing, Incoming<N>);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        transport::poll_receiver(&mut self.rx)
    }
}

/// creates a new request dispatcher/multiplexer for each accepted tcp socket
fn multiplex<N: Name>(stream: TcpStream) -> io::Result<(Outgoing, Incoming<N>)> {
    let local = stream.local_addr()?;
    let remote = stream.peer_addr()?;

    let instream = stream.try_clone()?;
    let outstream = stream;

    let (incoming_tx, incoming_rx) = mpsc::unbounded();
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let sender = transport::Sender::new(outstream, local, remote);

    let resolver = Resolver {
        pending: pending.clone(),
        sender: sender.clone(),
        incoming: incoming_tx,
        stream: instream,
    };

    let outgoing = Outgoing {
        next_id: Arc::new(AtomicUsize::new(0)),
        pending: pending,
        sender: sender,
    };

    let incoming = Incoming { rx: incoming_rx };

    resolver.dispatch();

    Ok((outgoing, incoming))
}

impl Network {
    /// Connects to an remote procedure call server.
    ///
    /// Please refer to the [`rpc`](rpc/index.html) module level documentation for more details.
    pub fn client<N: Name, E: ToSocketAddrs>(&self,
                                    endpoint: E)
                                    -> io::Result<(Outgoing, Incoming<N>)> {
        multiplex(TcpStream::connect(endpoint)?)
    }

    /// Creates a new remote procedure call server.
    ///
    /// If the `port` is not specified, a random ephemerial port is chosen.
    /// Please refer to the [`rpc`](rpc/index.html) module level documentation for more details.
    pub fn server<N: Name, P: Into<Option<u16>>>(&self, port: P) -> io::Result<Server<N>> {
        Server::new(self.clone(), port.into().unwrap_or(0))
    }
}

fn _assert() {
    enum E {A}
    impl Name for E {
        type Discriminant = u8;
        fn discriminant(&self) -> u8 {0}
        fn from_discriminant(_: &u8) -> Option<E> {None}
    }
    fn _is_send<T: Send>() {}
    _is_send::<Incoming<E>>();
    _is_send::<Outgoing>();
    _is_send::<Server<E>>();
}
