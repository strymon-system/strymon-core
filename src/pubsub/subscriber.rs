use std::io::{Result, Error};
use std::any::Any;
use std::marker::PhantomData;
use std::net::ToSocketAddrs;

use futures::{Poll, Async};
use futures::stream::Stream;
use abomonation::Abomonation;

use network::{Network, Receiver, Sender};
use network::message::abomonate::{Abomonate, NonStatic};

pub struct SubscriberClient<D> {
    rx: Receiver,
    _tx: Sender,
    marker: PhantomData<D>,
}

impl<D> SubscriberClient<D> {
    pub fn connect<A: ToSocketAddrs>(addr: A, network: &Network) -> Result<Self> {
        let (tx, rx) = network.connect(addr)?;

        Ok(SubscriberClient {
            rx: rx,
            _tx: tx,
            marker: PhantomData,
        })
    }
}

impl<D: Abomonation + Any + Clone + NonStatic> Stream for SubscriberClient<D> {
    type Item = Vec<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Vec<D>>, Error> {   
        let data = if let Some(mut buf) = try_ready!(self.rx.poll()) {
            let vec = buf.pop::<Abomonate, Vec<D>>().map_err(Into::<Error>::into)?;
            Some(vec)
        } else {
            None
        };
        
        Ok(Async::Ready(data))
    }
}
