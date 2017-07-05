use std::io::{Result, Error};
use std::marker::PhantomData;

use futures::{Poll, Async};
use futures::stream::Stream;

use serde::de::DeserializeOwned;

use strymon_communication::Network;
use strymon_communication::transport::{Sender, Receiver};

use model::Topic;

pub type CollectionSubscriber<D> = Subscriber<(D, i32)>;

pub struct Subscriber<D> {
    rx: Receiver,
    _tx: Sender,
    marker: PhantomData<D>,
}

impl<D> Subscriber<D> {
    pub fn connect(topic: &Topic, network: &Network) -> Result<Self> {
        let (tx, rx) = network.connect((&*topic.addr.0, topic.addr.1))?;

        Ok(Subscriber {
            rx: rx,
            _tx: tx,
            marker: PhantomData,
        })
    }
}

impl<D: DeserializeOwned> Stream for Subscriber<D> {
    type Item = Vec<D>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Vec<D>>, Error> {
        let data = if let Some(mut buf) = try_ready!(self.rx.poll()) {
            let vec = buf.pop::<Vec<D>>()?;
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
        let (tx, rx) = network.connect((&*topic.addr.0, topic.addr.1))?;

        Ok(TimelySubscriber {
            rx: rx,
            _tx: tx,
            marker: PhantomData,
        })
    }
}

impl<T, D> Stream for TimelySubscriber<T, D>
    where T: DeserializeOwned,
          D: DeserializeOwned
{
    type Item = (Vec<T>, T, Vec<D>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Error> {
        let data = if let Some(mut buf) = try_ready!(self.rx.poll()) {
            let data = buf.pop::<Vec<D>>()?;
            let time = buf.pop::<T>()?;
            let frontier = buf.pop::<Vec<T>>()?;

            Some((frontier, time, data))
        } else {
            None
        };

        Ok(Async::Ready(data))
    }
}
