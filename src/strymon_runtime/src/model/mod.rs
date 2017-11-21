// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fmt;
use named_type::NamedType;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation)]
pub struct TopicId(pub u64);

impl From<u64> for TopicId {
    fn from(id: u64) -> TopicId {
        TopicId(id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct TopicType {
    pub name: String,
}

impl TopicType {
    pub fn of<T: NamedType>() -> Self {
        TopicType {
            name: T::type_name().to_string()
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub enum TopicSchema {
    Collection(TopicType),
    Stream(TopicType, TopicType),
}

impl TopicSchema {
    pub fn is_collection(&self) -> bool {
        match *self {
            TopicSchema::Collection(_) => true,
            _ => false,
        }
    }

    pub fn is_stream(&self) -> bool {
        match *self {
            TopicSchema::Stream(_, _) => true,
            _ => false,
        }
    }
}

impl fmt::Display for TopicSchema {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TopicSchema::Collection(ref d) => write!(f, "Collection(item={:?})", d.name),
            TopicSchema::Stream(ref d, ref t) => {
                write!(f, "Stream(timestamp={:?}, data={:?})", t.name, d.name)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, NamedType)]
pub struct Topic {
    pub id: TopicId,
    pub name: String,
    pub addr: (String, u16),
    pub schema: TopicSchema,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation, NamedType)]
pub struct QueryId(pub u64);

impl From<u64> for QueryId {
    fn from(id: u64) -> QueryId {
        QueryId(id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, NamedType)]
pub struct Query {
    pub id: QueryId,
    pub name: Option<String>,
    pub program: QueryProgram,
    pub workers: usize, // in total
    pub executors: Vec<ExecutorId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, NamedType)]
pub struct QueryProgram {
    pub format: ExecutionFormat,
    pub source: String, // TODO(swicki) use Url crate for this?
    pub args: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, NamedType)]
pub enum ExecutionFormat {
    NativeExecutable,
    Other,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation, NamedType)]
pub struct ExecutorId(pub u64);

impl From<u64> for ExecutorId {
    fn from(id: u64) -> ExecutorId {
        ExecutorId(id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, NamedType)]
pub struct Executor {
    pub id: ExecutorId,
    pub host: String,
    pub format: ExecutionFormat,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation, NamedType)]
pub struct Publication(pub QueryId, pub TopicId);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation, NamedType)]
pub struct Subscription(pub QueryId, pub TopicId);

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Abomonation, NamedType)]
pub struct KeeperId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Abomonation, NamedType)]
pub struct Keeper {
    pub id: KeeperId,
    pub name: String,
    /// Worker id -> worker address
    pub workers: Vec<(usize, (String, u16))>,
}

impl From<u64> for KeeperId {
    fn from(id: u64) -> KeeperId {
        KeeperId(id)
    }
}
