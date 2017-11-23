// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io::{Error as IoError, ErrorKind};
use std::sync::Mutex;

use timely_communication::{Allocator, WorkerGuards};
use timely::progress::Timestamp;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::{self, Configuration};
use timely::dataflow::scopes::Root;

use futures::Future;

use serde::ser::Serialize;
use serde::de::DeserializeOwned;

use strymon_communication::Network;
use strymon_communication::rpc::Outgoing;

use executor::executable::NativeExecutable;
use strymon_model::QueryId;
use strymon_rpc::coordinator::{AddWorkerGroup, QueryToken};

pub mod subscribe;
pub mod publish;
pub mod keepers;

#[derive(Clone)]
pub struct Coordinator {
    token: QueryToken,
    network: Network,
    tx: Outgoing,
}

fn initialize(id: QueryId,
              process: usize,
              coord: String)
              -> Result<Coordinator, IoError> {
    let network = Network::init()?;
    let (tx, _) = network.client(&*coord)?;

    let announce = tx.request(&AddWorkerGroup {
        query: id,
        group: process,
    });

    let token = announce.wait()
        .map_err(|err| {
            err.and_then::<(), _>(|err| {
                let err = format!("failed to register: {:?}", err);
                Err(IoError::new(ErrorKind::Other, err))
            })
        })
        .map_err(Result::unwrap_err)?;

    Ok(Coordinator {
        tx: tx,
        network: network,
        token: token,
    })
}

pub fn execute<T, F>(func: F) -> Result<WorkerGuards<T>, String>
    where T: Send + 'static,
          F: Fn(&mut Root<Allocator>, Coordinator) -> T,
          F: Send + Sync + 'static
{
    let config = NativeExecutable::from_env().map_err(|err| {
            format!(concat!("Failed to parse data from executor. ",
                            "Has this binary been launched by an executor? ",
                            "Error: {:?}"),
                    err)
        })?;

    // create timely configuration
    let timely_conf = if config.hostlist.len() > 1 {
        info!("Configuration:Cluster({}, {}/{})",
              config.threads,
              config.process,
              config.hostlist.len());
        Configuration::Cluster(config.threads, config.process, config.hostlist, true)
    } else if config.threads > 1 {
        info!("Configuration:Process({})", config.threads);
        Configuration::Process(config.threads)
    } else {
        info!("Configuration:Thread");
        Configuration::Thread
    };

    let coord = initialize(config.query_id, config.process, config.coord)
        .map_err(|err| format!("failed to connect to coordinator: {:?}", err))?;

    // wrap in mutex because timely requires `Sync` for some reason
    let coord = Mutex::new(coord);
    timely::execute(timely_conf, move |root| {
        let coord = coord.lock().unwrap().clone();
        func(root, coord)
    })
}

/// This is a helper trait to workaround the fact that Rust does not allow
/// us to implement Serde's traits for Timely's custom timestamp types.
pub trait PubSubTimestamp: Timestamp {
    type Converted: Serialize + DeserializeOwned;

    fn to_pubsub(&self) -> Self::Converted;
    fn from_pubsub(converted: Self::Converted) -> Self;
}

impl<TOuter, TInner> PubSubTimestamp for Product<TOuter, TInner>
    where TOuter: PubSubTimestamp, TInner: PubSubTimestamp
{
    type Converted = (TOuter::Converted, TInner::Converted);

    fn to_pubsub(&self) -> Self::Converted {
        (self.outer.to_pubsub(), self.inner.to_pubsub())
    }

    fn from_pubsub((outer, inner): Self::Converted) -> Self {
        Product::new(TOuter::from_pubsub(outer), TInner::from_pubsub(inner))
    }
}

impl PubSubTimestamp for RootTimestamp {
    type Converted = ();

    fn to_pubsub(&self) -> Self::Converted {
        ()
    }

    fn from_pubsub(_: ()) -> Self {
        RootTimestamp
    }
}

macro_rules! impl_pubsub_timestamp {
    ($ty:ty) => {
        impl PubSubTimestamp for $ty {
            type Converted = Self;

            fn to_pubsub(&self) -> Self::Converted {
                *self
            }

            fn from_pubsub(conv: Self::Converted) -> Self {
                conv
            }
        }
    }
}

impl_pubsub_timestamp!(());
impl_pubsub_timestamp!(usize);
impl_pubsub_timestamp!(u32);
impl_pubsub_timestamp!(u64);
impl_pubsub_timestamp!(i32);
