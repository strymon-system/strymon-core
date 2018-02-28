// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
#![deny(missing_docs)]

//! Shared data types for Strymon Core.
//!
//! This crate defines a number of common data types used by the various components of Strymon Core.
//! Most types in this crate are used to store and the metadata of various enities in the system,
//! such as running jobs, available executors, published topics and so forth.
//!
//! All types in the root module can be serialized both with [Serde](https://serde.rs) and
//! [Abomonation](http://www.frankmcsherry.org/timely-dataflow/abomonation/). The data types used
//! in the root module of this crate are all part of the catalog and inform users about the current
//! state of the system.
//!
//! The types found in the [`config`](config/index.html) module on the other hand are purely
//! implementation details and define the format in which configuration data is passed between the
//! different components.

#[macro_use]
extern crate typename;

extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;

use std::fmt;

use typename::TypeName;

pub mod config;

/// A unique numerical identifier for a topic.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
         Abomonation, TypeName)]
pub struct TopicId(pub u64);

impl From<u64> for TopicId {
    fn from(id: u64) -> TopicId {
        TopicId(id)
    }
}

/// A representation of the type of data found in a topic.
///
/// The topic type is currently only defined by the name of a type, but it might be extended in
/// the future to contain a machine-readable schema definition of the contained data type.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation, TypeName)]
pub struct TopicType {
    /// The [`type_name`](https://docs.rs/typename/) of the type found in this topic.
    pub name: String,
}

impl TopicType {
    /// Creates a new instance for a given type.
    ///
    /// # Examples
    /// ```
    /// use strymon_model::TopicType;
    /// assert_eq!(TopicType::of::<Vec<i32>>().name, "std::vec::Vec<i32>");
    /// ```
    pub fn of<T: TypeName>() -> Self {
        TopicType { name: T::type_name() }
    }
}

/// The kind of protocol used in a topic.
///
/// A topic can either be a `Stream` topic, which provides access to a published Timely Dataflow
/// edge, or it be of the `Service`, which it describes the interface of an request-response kind of
/// service.
///
/// See also the [`strymon_job`](../strymon_job/index.html) crate for more information on topics.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation, TypeName)]
pub enum TopicSchema {
    /// A published Timely stream of type ([`Timestamp`][ts] [`Data`][data]).
    /// [ts]: http://www.frankmcsherry.org/timely-dataflow/timely/progress/timestamp/trait.Timestamp.html
    /// [data]: http://www.frankmcsherry.org/timely-dataflow/timely/trait.Data.html
    Stream(TopicType, TopicType),
    /// A service exporting the given [`Name`](../strymon_communication/rpc/trait.Name.html)
    /// interface type.
    Service(TopicType),
}

impl TopicSchema {
    /// Returns `true` for any `Stream` topic.
    pub fn is_stream(&self) -> bool {
        match *self {
            TopicSchema::Stream(_, _) => true,
            _ => false,
        }
    }

    /// Returns `true` for any `Service` topic.
    pub fn is_service(&self) -> bool {
        match *self {
            TopicSchema::Service(_) => true,
            _ => false,
        }
    }
}

impl fmt::Display for TopicSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TopicSchema::Service(ref d) => write!(f, "Service(item={:?})", d.name),
            TopicSchema::Stream(ref d, ref t) => {
                write!(f, "Stream(timestamp={:?}, data={:?})", t.name, d.name)
            }
        }
    }
}

/// The metadata of a published topic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, TypeName)]
pub struct Topic {
    /// A unique, non-reusable numerical identifier for this topic instance.
    pub id: TopicId,
    /// A unique (but re-usable) name for this topic.
    pub name: String,
    /// A [`strymon_communication`](../strymon_communication/index.html) endpoint address for
    /// this topic.
    pub addr: (String, u16),
    /// The kind of protocol used for this topic.
    pub schema: TopicSchema,
}

/// A unique numerical identifier for a job.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
         Abomonation, TypeName)]
pub struct QueryId(pub u64);

impl From<u64> for QueryId {
    fn from(id: u64) -> QueryId {
        QueryId(id)
    }
}

/// The meta-data of a submitted and running job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, TypeName)]
pub struct Query {
    /// A unique identifier for this job.
    pub id: QueryId,
    /// A human-readable description of the job.
    pub name: Option<String>,
    /// Information about the job executable.
    pub program: QueryProgram,
    /// The *total* amount of workers.
    pub workers: usize,
    /// A list of executors currently executing this job.
    pub executors: Vec<ExecutorId>,
    /// The Unix timestamp (at the coordinator) of the submission time.
    pub start_time: u64,
}

/// The meta-data about the executable code of a job.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, TypeName)]
pub struct QueryProgram {
    /// The name of the binary submitted to Strymon.
    pub binary_name: String,
    /// The kind of the submitted executable.
    pub format: ExecutionFormat,
    /// The URI from which the job executable can be fetched.
    pub source: String, // TODO(swicki) use Url crate for this?
    /// Command-line arguments to be passed to the executable.
    pub args: Vec<String>,
}

/// The format of a job executable.
///
/// Currently, only native binary executables (e.g. ELF binaries on Linux) are supported.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, TypeName)]
pub enum ExecutionFormat {
    /// The binary uses the native executable format.
    NativeExecutable,
    #[doc(hidden)]
    __NonExhaustive,
}

/// A unique numerical identifier of an executor instance.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
         Abomonation, TypeName)]
pub struct ExecutorId(pub u64);

impl From<u64> for ExecutorId {
    fn from(id: u64) -> ExecutorId {
        ExecutorId(id)
    }
}

/// The meta-data of an available executor in the system.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, TypeName)]
pub struct Executor {
    /// A unique identifier for this job.
    pub id: ExecutorId,
    /// The hostname of the machine this executor is running on.
    pub host: String,
    /// The kind of executable format this executor suppports.
    pub format: ExecutionFormat,
}

/// Associates the publication of a *topic* by its publishing *job*.
///
/// There can only be a single publication per topic. A job might publish multiple topics.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
         Abomonation, TypeName)]
pub struct Publication(pub QueryId, pub TopicId);

/// Associates the subscription to a *topic* by a subscribing *job*.
///
/// There can be many subscriptions on a topic. A job might subscribe to multiple topics.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
         Abomonation, TypeName)]
pub struct Subscription(pub QueryId, pub TopicId);
