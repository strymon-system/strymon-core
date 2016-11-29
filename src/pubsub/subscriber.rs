use std::io::{Result, Error};
use std::any::Any;
use std::marker::PhantomData;
use std::env;

use futures::{Poll, Async};
use futures::stream::Stream;
use abomonation::Abomonation;

use network::{Network, Receiver, Sender};
use network::message::abomonate::{Abomonate, NonStatic};
use model::Topic;

pub struct ItemSubscriber<D> {
    rx: Receiver,
    _tx: Sender,
    marker: PhantomData<D>,
}

impl<D> ItemSubscriber<D> {
    pub fn connect(topic: &Topic, network: &Network) -> Result<Self> {
        // TODO(swicki) check topic type
        let (tx, rx) = network.connect((&*topic.addr.0, topic.addr.1))?;

        Ok(ItemSubscriber {
            rx: rx,
            _tx: tx,
            marker: PhantomData,
        })
    }
}

impl<D: Abomonation + Any + Clone + NonStatic> Stream for ItemSubscriber<D> {
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

pub struct TimelySubscriber<T, D> {
    rx: Receiver,
    _tx: Sender,
    marker: PhantomData<(T, D)>,
}

impl<T, D> TimelySubscriber<T, D> {
    pub fn connect(topic: &Topic, network: &Network) -> Result<Self> {
        // TODO(swicki) check topic type
        let (tx, rx) = network.connect((&*topic.addr.0, topic.addr.1))?;

        Ok(TimelySubscriber {
            rx: rx,
            _tx: tx,
            marker: PhantomData,
        })
    }
}

impl<T, D> Stream for TimelySubscriber<T, D>
    where T: Abomonation + Any + Clone + NonStatic,
          D: Abomonation + Any + Clone + NonStatic
{
    type Item = (Vec<T>, T, Vec<D>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {   
        let data = if let Some(mut buf) = try_ready!(self.rx.poll()) {
            let frontier = buf.pop::<Abomonate, Vec<T>>().map_err(Into::<Error>::into)?;
            let time = buf.pop::<Abomonate, T>().map_err(Into::<Error>::into)?;
            let data = buf.pop::<Abomonate, Vec<D>>().map_err(Into::<Error>::into)?;

            Some((frontier, time, data))
        } else {
            None
        };
        
        Ok(Async::Ready(data))
    }
}
