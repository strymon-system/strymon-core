// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Data types used in the protocol between the publisher and subscribers.
//!
//! The protocol is based on the multi-part `MessageBuf` type. This allows
//! for partial decoding of data messages: We can read the timestamp of a
//! `Message::DataMessage` without having to decode the data part.

use std::fmt;
use std::io;
use std::borrow::Cow;
use std::marker::PhantomData;

use serde::{ser, de, Serialize, Deserialize};

use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely::progress::nested::product::Product;

use strymon_communication::message::MessageBuf;

/// Tag used for indicate whether the remainder of the `MessageBuf` contains
/// `InitialSnapshot`, `Message::LowerFrontierUpdate`, or `Message::DataMessage`.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum MessageTag {
    InitialSnapshot,
    LowerFrontierUpdate,
    DataMessage,
}

/// The first message sent on to a new subscriber on the topic.
#[derive(Debug, Clone)]
pub struct InitialSnapshot<T> {
    pub lower: Vec<T>,
    pub upper: Vec<T>,
}

impl<T: RemoteTimestamp> InitialSnapshot<T> {
    /// Encodes an `InitialSnapshot` message.
    pub fn encode(lower: &[T], upper: &[T]) -> io::Result<MessageBuf> {
        let mut buf = MessageBuf::empty();
        buf.push(MessageTag::InitialSnapshot)?;
        buf.push(RemoteFrontier::from(lower))?;
        buf.push(RemoteFrontier::from(upper))?;
        Ok(buf)
    }

    /// Decodes the first message of a topic into the `InitialSnapshot`.
    pub fn decode(mut msg: MessageBuf) -> io::Result<Self> {
        let tag = msg.pop::<MessageTag>()?;
        if tag != MessageTag::InitialSnapshot {
            let desc = format!("expected `InitialSnapshot`, got `{:?}`", tag);
            return Err(io::Error::new(io::ErrorKind::InvalidData, desc));
        }

        let lower = msg.pop::<RemoteFrontier<T>>()?.into();
        let upper = msg.pop::<RemoteFrontier<T>>()?.into();

        Ok(InitialSnapshot { lower, upper })
    }
}

/// A serialized Timely data batch, decodes into `Vec<T>`.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Data<T> {
    payload: MessageBuf,
    _marker: PhantomData<T>,
}

impl<T: de::DeserializeOwned> Data<T> {
    /// Attach a data type to the `MessageBuf`
    ///
    /// Assumes the payload contains of a single data object.
    fn new(payload: MessageBuf) -> Self {
        Data {
            payload: payload,
            _marker: PhantomData,
        }
    }

    /// Decodes a `Data<T>` into the underlying `Vec<T>`.
    ///
    /// The underlying message buffer is dropped even if decoding fails.
    pub fn decode(mut self) -> io::Result<Vec<T>> {
        self.payload.pop()
    }
}

/// The regular messages published in a topic.
///
/// This message can be put into a mulit-part `MessageBuf`. This means that all
/// data tuples to be published must implement the `Serialize` trait, and
/// timestamps must implement the `RemoteTimestamp` trait, which converts them
/// into a serializable custom type.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Message<T, D> {
    /// Updates the contents the lower frontier.
    ///
    /// Informs the subscriber about which timestamps are completed and thus
    /// are safe to emit notifications for.
    LowerFrontierUpdate { update: Vec<(T, i64)> },
    /// Creates a message indicting a Timely data batch.
    ///
    /// All data records belong to the same logical timestamp `time`. The `data`
    /// is only deserialized if needed.
    DataMessage { time: T, data: Data<D> },
}

impl<T, D> Message<T, D>
where
    T: RemoteTimestamp,
    D: Serialize,
{
    // Encodes a new `LowerFrontierUpdate` message.
    pub fn frontier_update(update: Vec<(T, i64)>) -> io::Result<MessageBuf> {
        let mut buf = MessageBuf::empty();
        buf.push(MessageTag::LowerFrontierUpdate)?;
        buf.push(RemoteUpdate::from(update))?;
        Ok(buf)
    }

    // Encodes a new `DataMessage` message.
    pub fn data_message(time: T, data: Vec<D>) -> io::Result<MessageBuf> {
        let mut buf = MessageBuf::empty();
        buf.push(MessageTag::DataMessage)?;
        buf.push(RemoteTime::from(time))?;
        buf.push(data)?;
        Ok(buf)
    }
}

impl<T, D> Message<T, D>
where
    T: RemoteTimestamp,
    D: de::DeserializeOwned,
{
    /// Partially decodes a `MessageBuf` into a `Message`
    ///
    /// This assumes that the `InitialSnapshot` has already been processed.
    pub fn decode(mut msg: MessageBuf) -> io::Result<Self> {
        let msg = match msg.pop::<MessageTag>()? {
            MessageTag::LowerFrontierUpdate => {
                Message::LowerFrontierUpdate { update: msg.pop::<RemoteUpdate<T>>()?.update }
            }
            MessageTag::DataMessage => {
                Message::DataMessage {
                    time: msg.pop::<RemoteTime<T>>()?.time,
                    data: Data::<D>::new(msg),
                }
            }
            tag => {
                let desc = format!(
                    "expected `LowerFrontierUpdate` or `DataMessage`, got `{:?}`",
                    tag
                );
                return Err(io::Error::new(io::ErrorKind::InvalidData, desc));
            }
        };

        Ok(msg)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RemoteFrontier<'a, T: RemoteTimestamp + 'a> {
    #[serde(with = "remote_frontier")]
    frontier: Cow<'a, [T]>,
}

impl<'a, T: RemoteTimestamp + 'a> From<&'a [T]> for RemoteFrontier<'a, T> {
    fn from(borrowed: &'a [T]) -> Self {
        RemoteFrontier { frontier: Cow::Borrowed(borrowed) }
    }
}

impl<'a, T: RemoteTimestamp + 'a> Into<Vec<T>> for RemoteFrontier<'a, T> {
    fn into(self) -> Vec<T> {
        self.frontier.into_owned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RemoteUpdate<T: RemoteTimestamp> {
    #[serde(with = "remote_update")]
    update: Vec<(T, i64)>,
}

impl<T: RemoteTimestamp> From<Vec<(T, i64)>> for RemoteUpdate<T> {
    fn from(update: Vec<(T, i64)>) -> Self {
        RemoteUpdate { update }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RemoteTime<T: RemoteTimestamp> {
    #[serde(with = "remote_timestamp")]
    time: T,
}

impl<T: RemoteTimestamp> From<T> for RemoteTime<T> {
    fn from(time: T) -> Self {
        RemoteTime { time }
    }
}

mod remote_frontier {
    use super::*;

    pub fn serialize<'a, S, T>(field: &Cow<'a, [T]>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
        T: RemoteTimestamp + 'a,
    {
        ser.collect_seq(field.iter().map(|t| t.to_remote()))
    }

    pub fn deserialize<'de, 'a, D, T>(de: D) -> Result<Cow<'a, [T]>, D::Error>
    where
        D: de::Deserializer<'de>,
        T: RemoteTimestamp + 'a,
    {
        struct RemoteFrontierVisitor<T>(PhantomData<T>);

        impl<'de, T> de::Visitor<'de> for RemoteFrontierVisitor<T>
        where
            T: RemoteTimestamp,
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a sequence of timestamps")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let cap = seq.size_hint().unwrap_or(1);
                let mut frontier = Vec::with_capacity(cap);
                while let Some(remote) = seq.next_element()? {
                    frontier.push(T::from_remote(remote));
                }
                Ok(frontier)
            }
        }

        let visitor = RemoteFrontierVisitor(PhantomData);
        de.deserialize_seq(visitor).map(Cow::Owned)
    }
}

mod remote_update {
    use super::*;

    pub fn serialize<S, T>(field: &Vec<(T, i64)>, ser: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
        T: RemoteTimestamp,
    {
        ser.collect_seq(field.iter().map(|&(ref t, i)| (t.to_remote(), i)))
    }

    pub fn deserialize<'de, D, T>(de: D) -> Result<Vec<(T, i64)>, D::Error>
    where
        D: de::Deserializer<'de>,
        T: RemoteTimestamp,
    {
        struct RemoteUpdateVisitor<T>(PhantomData<T>);

        impl<'de, T> de::Visitor<'de> for RemoteUpdateVisitor<T>
        where
            T: RemoteTimestamp,
        {
            type Value = Vec<(T, i64)>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a sequence of (timestamp, delta) tuples")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let cap = seq.size_hint().unwrap_or(1);
                let mut update = Vec::with_capacity(cap);
                while let Some((remote, i)) = seq.next_element()? {
                    update.push((T::from_remote(remote), i));
                }
                Ok(update)
            }
        }

        de.deserialize_seq(RemoteUpdateVisitor(PhantomData))
    }
}

mod remote_timestamp {
    use super::*;

    pub fn serialize<'a, S, T>(field: &T, ser: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
        T: RemoteTimestamp + 'a,
    {
        Serialize::serialize(&field.to_remote(), ser)
    }

    pub fn deserialize<'de, 'a, D, T>(de: D) -> Result<T, D::Error>
    where
        D: de::Deserializer<'de>,
        T: RemoteTimestamp + 'a,
    {
        <T::Remote as Deserialize>::deserialize(de).map(T::from_remote)
    }
}


/// A trait for Timely timestamps which can be serialized using Serde.
///
/// This is a workaround to Rust's orphan rules, allowing the conversion from
/// and to a `Remote` type used in Strymon topics. An implementation is provided
/// for all timestamp types owned by Timely. It must be implemented
/// separately by users which have their own custom Timely timestamps.
pub trait RemoteTimestamp: Timestamp {
    /// The `Remote` type must implement the Serde traits, it can be `Self`.
    type Remote: ser::Serialize + de::DeserializeOwned;

    /// Convert the local timestamp into a serializeable remote type.
    fn to_remote(&self) -> Self::Remote;
    /// Reconstruct the Timely timestamp from a deserialized remote type.
    fn from_remote(remote: Self::Remote) -> Self;
}

impl<TOuter, TInner> RemoteTimestamp for Product<TOuter, TInner>
where
    TOuter: RemoteTimestamp,
    TInner: RemoteTimestamp,
{
    type Remote = (TOuter::Remote, TInner::Remote);

    fn to_remote(&self) -> Self::Remote {
        (self.outer.to_remote(), self.inner.to_remote())
    }

    fn from_remote((outer, inner): Self::Remote) -> Self {
        Product::new(TOuter::from_remote(outer), TInner::from_remote(inner))
    }
}

impl RemoteTimestamp for RootTimestamp {
    type Remote = ();

    fn to_remote(&self) -> Self::Remote {
        ()
    }

    fn from_remote(_: ()) -> Self {
        RootTimestamp
    }
}

macro_rules! impl_remote_timestamp {
    ($ty:ty) => {
        impl RemoteTimestamp for $ty {
            type Remote = Self;

            fn to_remote(&self) -> Self::Remote {
                *self
            }

            fn from_remote(remote: Self::Remote) -> Self {
                remote
            }
        }
    }
}

impl_remote_timestamp!(());
impl_remote_timestamp!(usize);
impl_remote_timestamp!(u32);
impl_remote_timestamp!(u64);
impl_remote_timestamp!(i32);

#[cfg(test)]
mod tests {
    use super::{Message, InitialSnapshot};

    #[test]
    fn initial_snapshot_encode_decode() {
        let msg = InitialSnapshot::encode(&[1, 2, 3], &[4, 5, 6]).unwrap();
        let msg = InitialSnapshot::<i32>::decode(msg).unwrap();
        assert_eq!(msg.lower, &[1, 2, 3]);
        assert_eq!(msg.upper, &[4, 5, 6]);

        let msg = InitialSnapshot::encode(&[], &[()]).unwrap();
        let msg = InitialSnapshot::<()>::decode(msg).unwrap();
        assert_eq!(msg.lower, &[]);
        assert_eq!(msg.upper, &[()]);
    }

    #[test]
    fn frontier_update_encode_decode() {
        let msg = Message::<i32, ()>::frontier_update(vec![(0, -1), (1, 1)]).unwrap();
        let msg = Message::<i32, ()>::decode(msg).unwrap();
        assert_eq!(
            msg,
            Message::LowerFrontierUpdate { update: vec![(0, -1), (1, 1)] }
        );
    }

    #[test]
    fn data_message_encode_decode() {
        let msg = Message::<i32, String>::data_message(
            42,
            vec!["hello".to_string(), "world".to_string()],
        ).unwrap();
        let msg = Message::<i32, String>::decode(msg).unwrap();
        match msg {
            Message::DataMessage { time: 42, data } => {
                assert_eq!(
                    data.decode().unwrap(),
                    vec!["hello".to_string(), "world".to_string()]
                );
            }
            msg => panic!("Invalid message: {:?}", msg),
        }
    }
}
