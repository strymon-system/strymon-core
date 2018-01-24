// Copyright 2018 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Data types used in the protocol between the publisher and subscribers.

use std::fmt;
use std::borrow::Cow;
use std::marker::PhantomData;

use serde::{ser, de, Serialize, Deserialize};

use timely::progress::timestamp::{Timestamp, RootTimestamp};
use timely::progress::nested::product::Product;

/// The messages published in a topic.
///
/// This message is supposed to be serialized with Serde. This means that all
/// data tuples to be published must implement the `Serialize` trait, and
/// timestamps must implement the `RemoteTimestamp` trait, which converts them
/// into a serializable custom type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Message<'a, T, D> where T: Clone + 'a {
    /// Current contents the lower frontier.
    ///
    /// Informs the subscriber about which timestamps are completed and thus
    /// are safe to emit notifications for.
    LowerFrontier {
        #[serde(with = "remote_frontier")]
        #[serde(bound = "T: RemoteTimestamp")]
        lower: Cow<'a, [T]>
    },
    /// Contents of the upper frontier.
    ///
    /// Informs the subscriber about currently active epochs, allowing it to
    /// filter out messages which are smaller or equal to any element in the
    /// `upper` frontier.
    UpperFrontier {
        #[serde(with = "remote_frontier")]
        #[serde(bound = "T: RemoteTimestamp")]
        upper: Cow<'a, [T]>,
    },
    /// A timely data batch.
    ///
    /// All data records belong to the same logical timestamp `time`.
    DataMessage {
        #[serde(with = "remote_timestamp")]
        #[serde(bound = "T: RemoteTimestamp")]
        time: T,
        data: Vec<D>,
    }
}

impl<'a, T, D> Message<'a, T, D> where T: Clone + 'a {
    /// Constructs a new `LowerFrontier` message.
    pub fn lower(frontier: &'a [T]) -> Self {
        Message::LowerFrontier {
            lower: Cow::Borrowed(frontier),
        }
    }

    /// Constructs a new `UpperFrontier` message.
    pub fn upper(frontier: &'a [T]) -> Self {
        Message::UpperFrontier {
            upper: Cow::Borrowed(frontier),
        }
    }

    /// Constructs a new `DataMessage` message.
    pub fn data(time: T, data: Vec<D>) -> Self {
        Message::DataMessage { time, data }
    }
}

mod remote_frontier {
    use super::*;

    pub fn serialize<'a, S, T>(field: &Cow<'a, [T]>, ser: S) -> Result<S::Ok, S::Error>
        where S: ser::Serializer, T: RemoteTimestamp + 'a
    {
        ser.collect_seq(field.iter().map(|t| t.to_remote()))
    }

    pub fn deserialize<'de, 'a, D, T>(de: D) -> Result<Cow<'a, [T]>, D::Error>
        where D: de::Deserializer<'de>, T: RemoteTimestamp + 'a
    {
        struct RemoteFrontierVisitor<T>(PhantomData<T>);

        impl<'de, T> de::Visitor<'de> for RemoteFrontierVisitor<T>
            where T: RemoteTimestamp
        {
            type Value = Vec<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                write!(formatter, "a sequence of timestamps")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where A: de::SeqAccess<'de>,
            {
                let mut frontier = Vec::with_capacity(seq.size_hint().unwrap_or(1));
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

mod remote_timestamp {
    use super::*;

    pub fn serialize<'a, S, T>(field: &T, ser: S) -> Result<S::Ok, S::Error>
        where S: ser::Serializer, T: RemoteTimestamp + 'a
    {
        Serialize::serialize(&field.to_remote(), ser)
    }

    pub fn deserialize<'de, 'a, D, T>(de: D) -> Result<T, D::Error>
        where D: de::Deserializer<'de>, T: RemoteTimestamp + 'a
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
    where TOuter: RemoteTimestamp, TInner: RemoteTimestamp
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
    use std::borrow::Cow;
    use serde_test::{Token, assert_tokens};
    use timely::progress::nested::product::Product;
    use super::Message;

    #[test]
    fn serde_lower_frontier() {
        let msg: Message<i32, ()> = Message::LowerFrontier {
            lower: Cow::Borrowed(&[0]),
        };
        assert_tokens(&msg, &[
            Token::StructVariant { name: "Message", variant: "LowerFrontier", len: 1, },
            Token::Str("lower"),
            Token::Seq { len: Some(1), },
            Token::I32(0),
            Token::SeqEnd,
            Token::StructVariantEnd,
        ]);

        let msg: Message<(), ()> = Message::LowerFrontier {
            lower: Cow::Borrowed(&[()]),
        };
        assert_tokens(&msg, &[
            Token::StructVariant { name: "Message", variant: "LowerFrontier", len: 1, },
            Token::Str("lower"),
            Token::Seq { len: Some(1), },
            Token::Unit,
            Token::SeqEnd,
            Token::StructVariantEnd,
        ]);
    }

    #[test]
    fn serde_upper_frontier() {
        let frontier = [Product::new(0, 1), Product::new(1, 0)];
        let msg: Message<_, ()> = Message::UpperFrontier {
            upper: Cow::Borrowed(&frontier),
        };
        assert_tokens(&msg, &[
            Token::StructVariant { name: "Message", variant: "UpperFrontier", len: 1, },
            Token::Str("upper"),
            Token::Seq { len: Some(2), },
            Token::Tuple { len: 2, },
            Token::I32(0),
            Token::I32(1),
            Token::TupleEnd,
            Token::Tuple { len: 2, },
            Token::I32(1),
            Token::I32(0),
            Token::TupleEnd,
            Token::SeqEnd,
            Token::StructVariantEnd,
        ]);
    }

    #[test]
    fn serde_data_message() {
        let msg: Message<u64, String> = Message::DataMessage {
            time: 555555555555555,
            data: vec!["foo".into(), "bar".into()],
        };

        assert_tokens(&msg, &[
            Token::StructVariant { name: "Message", variant: "DataMessage", len: 2, },
            Token::Str("time"),
            Token::U64(555555555555555),
            Token::Str("data"),
            Token::Seq { len: Some(2), },
            Token::Str("foo"),
            Token::Str("bar"),
            Token::SeqEnd,
            Token::StructVariantEnd,
        ]);
    }
}
