// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Types used to announce and find services announced in the catalog.

use std::io;
use std::marker::PhantomData;

use typename::TypeName;
use futures::{Async, Poll};
use futures::stream::{Stream, Fuse};

use strymon_model::{Topic, TopicType, TopicSchema};
use strymon_communication::rpc::*;

use Coordinator;
use super::publish::PublicationError;
use super::subscribe::SubscriptionError;
use util::StreamsUnordered;

/// A source of incoming requests.
///
/// Will deregister the announced service from the catalog when dropped.
pub struct Service<N: Name> {
    server: Fuse<Server<N>>,
    clients: StreamsUnordered<Incoming<N>>,
    topic: Topic,
    coord: Coordinator,
}

impl<N: Name> Drop for Service<N> {
    fn drop(&mut self) {
        if let Err(err) = self.coord.unpublish(self.topic.id) {
            warn!("failed to unpublish service: {:?}", err)
        }
    }
}

impl<N: Name> Stream for Service<N> {
    type Item = RequestBuf<N>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // make sure to accept any new incoming clients first
        while let Async::Ready(Some((_, client))) = self.server.poll()? {
            self.clients.push(client);
        }

        match self.clients.poll() {
            Ok(Async::Ready(None)) if !self.server.is_done() => {
                // we don't have any clients, but the server is still alive
                Ok(Async::NotReady)
            }
            other => other,
        }
    }
}

/// Client handle for sending requests to a announced service.
///
/// Will deregister the announced service from the catalog when dropped.
pub struct Client<N: Name> {
    queue: Outgoing,
    topic: Topic,
    coord: Coordinator,
    name: PhantomData<N>,
}

impl<N: Name> Client<N> {
    /// Asynchronously sends out a request to the remote peer.
    ///
    /// Returns a future for the pending response. The next request can be
    /// submitted without having to wait for the previous response to arrive.
    pub fn request<R: Request<N>>(&self, r: &R) -> Response<N, R> {
        self.queue.request(r)
    }
}

impl<N: Name> Drop for Client<N> {
    fn drop(&mut self) {
        if let Err(err) = self.coord.unsubscribe(self.topic.id) {
            warn!("failed to unpublish service: {:?}", err)
        }
    }
}

impl Coordinator {
    /// Creates a new request service and announces it in the catalog.
    ///
    /// Given a service interface definition and a name, creates a new server
    /// for receiving incoming requests. A topic is created under the specified
    /// name which is used by clients to bind to the service.
    pub fn announce_service<N: Name + TypeName>(
        &self,
        name: &str,
    ) -> Result<Service<N>, PublicationError> {
        // create a new service and obtain its address
        let server = self.network.server(None)?.fuse();
        let addr = {
            let (host, port) = server.get_ref().external_addr();
            (host.to_string(), port)
        };

        // announce the service to the coordinator
        let schema = TopicSchema::Service(TopicType::of::<N>());
        let topic = self.publish_request(name.to_string(), schema, addr)?;
        let clients = StreamsUnordered::new();
        let coord = self.clone();
        Ok(Service {
            server,
            clients,
            topic,
            coord,
        })
    }

    /// Creates a binding to a service topic for sending requests.
    ///
    /// Returns a handle for submitting requests to the given server. The
    /// binding created by this invocation is also stored in the catalog as
    /// a `Subscription`. The subscription is revoked once the `Client` handle
    /// is dropped.
    ///
    /// When `blocking` is true, this call blocks until a remote job registers
    /// a service with the matching name. If `blocking` is false, the call
    /// returns with an error if the topic does not exist.
    pub fn bind_service<N: Name + TypeName>(
        &self,
        name: &str,
        blocking: bool,
    ) -> Result<Client<N>, SubscriptionError> {
        let topic = self.subscribe_request(name, blocking)?;
        let schema = TopicSchema::Service(TopicType::of::<N>());
        if topic.schema != schema {
            return Err(SubscriptionError::TypeMismatch);
        }

        let queue = {
            let addr = (&*topic.addr.0, topic.addr.1);
            let (tx, _) = self.network.client::<N, _>(addr)?;
            tx
        };
        let coord = self.clone();
        let name = PhantomData;
        Ok(Client {
            queue,
            topic,
            coord,
            name,
        })
    }
}
