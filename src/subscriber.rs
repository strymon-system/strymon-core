use std::io::{Error as IoError, Result as IoResult};
use std::env;
use std::marker::PhantomData;
use std::collections::BTreeMap;
use std::sync::mpsc;

use timely::Data;
use timely::progress::{Timestamp, PathSummary};
use timely::dataflow::channels::Content;
use timely::dataflow::scopes::Scope;
use timely::dataflow::stream::Stream;
use timely::dataflow::operators::Unary;
use timely::dataflow::operators::Capability;
use timely::dataflow::channels::pact::{Exchange, Pipeline, ParallelizationContract};

use timely_communication::{Allocate, Push, Pull};

use coordinator::catalog::request::PublishError as CatalogError;
use messaging::{self, Sender, Receiver, Message};

use executor::executable;
use worker::coordinator::Catalog;
use topic::{Topic, TypeId};

use util::Generator;

pub trait Subscribe<S: Scope, D: Data> {
    fn subscribe(&self, name: &str, advance: <S::Timestamp as Timestamp>::Summary) -> Result<Stream<S, D>, SubscribeError>;
}


#[derive(Debug)]
pub enum SubscribeError {
    Catalog(CatalogError),
    Io(IoError),
}

impl From<IoError> for SubscribeError {
    fn from(io: IoError) -> Self {
        SubscribeError::Io(io)
    }
}

impl From<CatalogError> for SubscribeError {
    fn from(c: CatalogError) -> Self {
        SubscribeError::Catalog(c)
    }
}

impl<T: Timestamp, D: Data, S> Subscribe<S, D> for S
    where S: Scope<Timestamp = T>
{
    fn subscribe(&self, name: &str, advance: <S::Timestamp as Timestamp>::Summary) -> Result<Stream<S, D>, SubscribeError> {
        unimplemented!()
    }
}
