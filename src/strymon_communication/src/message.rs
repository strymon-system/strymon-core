// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Reference-counted buffer for multi-part messages.

use std::io::{self, Cursor, Read, Write, ErrorKind};

use serde::ser::Serialize;
use serde::de::{Deserialize, DeserializeOwned};

use rmp_serde::{encode, decode, from_slice};

use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use bytes::BytesMut;

/// A `MessageBuf` represents a contiguous buffer of [MessagePack](https://msgpack.org/)
/// encoded objects. It can be used as a multi-part message to allow partial
/// deserialization. Partial messages can be inserted using the `push` method
/// and are read in first-in, first-out order by the `pop` method.
///
/// MessageBuf is a convenience wrapper around
/// [`bytes::BytesMut`](https://docs.rs/bytes/0.4/bytes/struct.BytesMut.html)
/// and thus inherets the same properties: No allocation is required for small
/// objects; clones are reference-counted and implement copy-on-write semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageBuf {
    buf: BytesMut,
}

/// Custom writer which extends the buffer on each call to write
struct Writer<'a> {
    buf: &'a mut BytesMut,
}

impl<'a> Write for Writer<'a> {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(src);
        Ok(src.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> Writer<'a> {
    fn new(buf: &'a mut BytesMut) -> Writer<'a> {
        Writer { buf }
    }
}

impl MessageBuf {
    /// Create a new message buffer containing the serialized object.
    pub fn new<S: Serialize>(item: S) -> io::Result<Self> {
        // we start with an empty buffer, because if the serialized element
        // is smaller than 24 bytes on x86_64, it will not allocate.
        let mut msg = MessageBuf::empty();
        msg.push(item).map_err(|err| io::Error::new(ErrorKind::Other, err))?;

        Ok(msg)
    }

    /// Create a new, empty message.
    ///
    /// Use one of the `From` impls to construct a message from an already
    /// existing buffer or object.
    pub fn empty() -> Self {
        MessageBuf {
            buf: BytesMut::new()
        }
    }

    /// Returns true if the contained bytes have a length of zero.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Serialize and append an item to the message buffer.
    pub fn push<S: Serialize>(&mut self, item: S) -> io::Result<()> {
        let mut writer = Writer::new(&mut self.buf);
        encode::write(&mut writer, &item)
            .map_err(|err| io::Error::new(ErrorKind::Other, err))
    }

    /// Remove and deserialize the top item in the message buffer.
    ///
    /// The object is not modified and kept in the buffer if deserialization fails.
    /// The `push` and `pop` operations implement FIFO semantics.
    pub fn pop<D: DeserializeOwned>(&mut self) -> io::Result<D> {
        // TODO(swicki): it would be nice if we could split `self.buf`
        // and return an `owning_ref` for zero-copy deserialization.
        // Unfortunately, `rmp_serde` does not support this directly, but we
        // could implement `rmp_serde::decode::Read` manually.

        let (item, bytes_read) = {
            let mut reader = Cursor::new(&self.buf);
            let item = decode::from_read(&mut reader)
                .map_err(|err| io::Error::new(ErrorKind::Other, err))?;
            let bytes_read = reader.position() as usize;
            (item, bytes_read)
        };

        // now that we successfully deserialized, we can drop parts of the buffer
        let _ = self.buf.split_to(bytes_read);

        Ok(item)
    }

    /// Peek at the top item in the message buffer. This borrows the buffer
    /// for zero-copy deserialization.
    pub fn peek<'de, D: Deserialize<'de>>(&'de self) -> io::Result<D> {
        from_slice(&self.buf).map_err(|err| io::Error::new(ErrorKind::Other, err))
    }

    /// Copies the contained bytes into the provided writer.
    ///
    /// Prepends the size of the message in bytes as a big-endian 32 bit
    /// unsigned integer.
    pub fn write<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // TODO: use writev/vecio or some other scatter/gather method to
        // avoid two system calls
        writer.write_u32::<NetworkEndian>(self.buf.len() as u32)?;
        writer.write_all(&self.buf)
    }

    /// Reads a message from a reader, expecting a frame length prefix.
    ///
    /// If first read of the message length field fails with an
    /// `ErrorKind::UnexpectedEof` then `Ok(None)` is returned to indicate
    /// that the reader closed gracefully. All other read errors, most notably
    /// `ErrorKind::WouldBlock`, are propagated verbatim.
    pub fn read<R: Read>(reader: &mut R) -> io::Result<Option<MessageBuf>> {
        let length = match reader.read_u32::<NetworkEndian>() {
            Ok(length) => length as usize,
            // special case: remote host disconnected without sending any new message
            Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(err),
        };

        let mut bytes: Vec<u8> = Vec::with_capacity(length);
        unsafe {
            assert!(bytes.capacity() <= length);
            bytes.set_len(length);
            reader.read_exact(&mut bytes)?;
        }

        Ok(Some(MessageBuf {
            buf: BytesMut::from(bytes),
        }))
    }

    #[cfg(feature = "tracing")]
    /// Returns a decoded printable representation of the message
    pub fn debug<'a>(&'a self) -> tracing::Debug<'a> {
        tracing::Debug::new(&*self.buf)
    }
}

impl From<BytesMut> for MessageBuf {
    fn from(buf: BytesMut) -> Self {
        MessageBuf { buf }
    }
}

impl Into<BytesMut> for MessageBuf {
    fn into(self) -> BytesMut {
        self.buf
    }
}

#[cfg(feature = "tracing")]
mod tracing {
    use std::fmt;
    use rmpv::decode::read_value_ref;
    use super::*;

    /// A proxy type implementing a more detailed version of `fmt::Debug`
    pub struct Debug<'a> {
        buf: &'a [u8],
    }

    impl<'a> Debug<'a> {
        pub fn new(buf: &'a [u8]) -> Debug<'a> {
            Debug { buf }
        }
    }


    impl<'a> fmt::Debug for Debug<'a> {
        fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            let mut remaining = self.buf.len() as u64;
            let mut reader = Cursor::new(self.buf);
            let mut fmt = fmt.debug_tuple("MessageBuf");
            while remaining > 0 {
                let item = read_value_ref(&mut reader).map_err(|_| fmt::Error)?;
                remaining = self.buf.len() as u64 - reader.position();
                fmt.field(&DisplayDebug(item));
            }
            fmt.finish()
        }
    }

    /// Adapter which implements `Debug` by calling into `Display`.
    struct DisplayDebug<T>(T);

    impl<T: fmt::Display> fmt::Debug for DisplayDebug<T> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            self.0.fmt(f)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MessageBuf;

    #[test]
    fn push_and_pop_many_msg() {
        let string = String::from("hi");
        let vector = vec![1u8, 2, 3];
        let integer = 42i32;

        let mut buf = MessageBuf::empty();
        buf.push(&string).unwrap();

        assert_eq!(string, buf.peek::<&str>().unwrap());

        buf.push(&vector).unwrap();
        buf.push(&integer).unwrap();
        assert_eq!(string, buf.pop::<String>().unwrap());
        assert_eq!(vector, buf.pop::<Vec<u8>>().unwrap());
        assert_eq!(integer, buf.pop::<i32>().unwrap());
    }

    #[test]
    #[should_panic]
    fn pop_empty() {
        let mut buf = MessageBuf::empty();
        buf.pop::<i32>().unwrap();
    }

    #[test]
    fn type_mismatch() {
        let mut buf = MessageBuf::new(6).unwrap();
        buf.pop::<String>().unwrap_err();
    }
}
