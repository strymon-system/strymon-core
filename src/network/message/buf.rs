use byteorder::{NetworkEndian, WriteBytesExt, ReadBytesExt};

use network::message::{Encode, Decode};
use std::collections::VecDeque;
use std::io::{self, Read, Write, Cursor, Error, ErrorKind};
use std::mem;
use std::sync::Arc;

#[derive(Clone, Default)]
pub struct MessageBuf {
    inner: Arc<Buf>,
}

impl MessageBuf {
    pub fn empty() -> Self {
        Default::default()
    }

    pub fn push<E, T>(&mut self, payload: &T) -> Result<(), E::EncodeError>
        where E: Encode<T>
    {
        Arc::make_mut(&mut self.inner).push::<E, T>(payload)
    }

    pub fn pop<D, T>(&mut self) -> Result<T, D::DecodeError>
        where D: Decode<T>
    {
        Arc::make_mut(&mut self.inner).pop::<D, T>()
    }
}

pub fn read<R: Read>(reader: &mut R) -> io::Result<Option<MessageBuf>> {
    let length = match reader.read_u32::<NetworkEndian>() {
        Ok(length) => length,
        // special case: remote host disconneced without sending any new message
        Err(ref err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    };

    let buflen = reader.read_u32::<NetworkEndian>()?;

    if buflen >= length || (length - buflen) % 8 != 0 {
        return Err(Error::new(ErrorKind::InvalidData, "invalid header"));
    }

    let mut buf = vec![0u8; length as usize]; // TODO(swicki) optimize this
    reader.read_exact(&mut buf)?;

    let sections = (length - buflen) / 8;
    let mut pos = VecDeque::<(u32, u32)>::with_capacity(sections as usize);
    {
        let mut reader = Cursor::new(&buf[buflen as usize..]);
        for _ in 0..sections {
            let start = reader.read_u32::<NetworkEndian>()?;
            let end = reader.read_u32::<NetworkEndian>()?;
            pos.push_back((start, end));
        }
    }

    buf.truncate(buflen as usize);

    Ok(Some(MessageBuf {
        inner: Arc::new(Buf {
            buf: buf,
            pos: pos,
        }),
    }))
}

pub fn write<W: Write>(writer: &mut W, msg: &MessageBuf) -> io::Result<()> {
    let msg = &*msg.inner;
    let buflen = msg.buf.len() as u32;
    let sections = msg.pos.len() as u32;

    // header: (u32, u32) | buflen: [u8] | footer: [(u32, u32)]
    let length = buflen + (sections * 8);

    // header
    writer.write_u32::<NetworkEndian>(length)?;
    writer.write_u32::<NetworkEndian>(buflen)?;

    // buflen
    writer.write_all(&msg.buf)?;

    // footer
    for &(start, end) in &msg.pos {
        writer.write_u32::<NetworkEndian>(start)?;
        writer.write_u32::<NetworkEndian>(end)?;
    }

    Ok(())
}


#[derive(Clone, Default)]
struct Buf {
    buf: Vec<u8>,
    pos: VecDeque<(u32, u32)>, // (start, end)
}

impl Buf {
    pub fn push<E, T>(&mut self, payload: &T) -> Result<(), E::EncodeError>
        where E: Encode<T>
    {
        let align = mem::align_of::<T>();
        let start = self.buf.len();
        // we assume the alignment is always a power of two
        let aligned = (start + align - 1) & !(align - 1);
        self.buf.resize(aligned, 0);

        match E::encode(payload, &mut self.buf) {
            Ok(()) => {
                self.pos.push_back((aligned as u32, self.buf.len() as u32));
                Ok(())
            }
            Err(err) => {
                self.buf.truncate(start);
                Err(err)
            }
        }
    }

    pub fn pop<D, T>(&mut self) -> Result<T, D::DecodeError>
        where D: Decode<T>
    {
        let (start, end) = self.pos.pop_front().expect("cannot pop from empty buffer");
        match D::decode(&mut self.buf[start as usize..end as usize]) {
            Ok(t) => Ok(t),
            Err(err) => {
                // put offset back
                self.pos.push_front((start, end));
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use network::message::abomonate::Abomonate;
    use super::*;

    #[test]
    fn pop_msg() {
        let mut buf = MessageBuf::empty();
        let orig = vec![String::from("foo"), String::from("bar")];
        buf.push::<Abomonate, _>(&orig).unwrap();
        let vec = buf.pop::<Abomonate, Vec<String>>().unwrap();

        assert_eq!(vec, orig);
    }

    #[test]
    fn push_many_msg() {
        let string = String::from("hi");
        let vector = vec![1u8, 2, 3];
        let integer = 42i32;

        let mut buf = MessageBuf::empty();
        buf.push::<Abomonate, _>(&string).unwrap();

        assert_eq!(string, buf.pop::<Abomonate, String>().unwrap());

        buf.push::<Abomonate, _>(&vector).unwrap();
        buf.push::<Abomonate, _>(&integer).unwrap();
        assert_eq!(vector, buf.pop::<Abomonate, Vec<u8>>().unwrap());
        assert_eq!(integer, buf.pop::<Abomonate, i32>().unwrap());
    }

    #[test]
    #[should_panic]
    fn pop_empty() {
        let mut buf = MessageBuf::empty();
        buf.pop::<Abomonate, i32>().unwrap();
    }

    #[test]
    fn type_mismatch() {
        use network::message::abomonate::DecodeError::*;

        let mut buf = MessageBuf::empty();
        buf.push::<Abomonate, i32>(&5).unwrap();
        assert_eq!(TypeMismatch, buf.pop::<Abomonate, String>().unwrap_err());
    }

    #[test]
    fn read_write() {
        let mut buf = MessageBuf::empty();
        buf.push::<Abomonate, _>(&String::from("Some Content")).unwrap();
        buf.push::<Abomonate, f32>(&3.14f32).unwrap();

        let mut stream = Vec::<u8>::new();
        write(&mut stream, &buf).expect("failed to write");
        let mut out = read(&mut &*stream).expect("failed to read").unwrap();

        assert_eq!("Some Content", out.pop::<Abomonate, String>().unwrap());
        assert_eq!(3.14, out.pop::<Abomonate, f32>().unwrap());
    }

}
