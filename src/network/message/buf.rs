use std::sync::Arc;
use std::collections::VecDeque;
use std::mem;
use std::io::{self, Read, Write, Cursor, Error, ErrorKind};

use network::message::{Encode, Decode};
use byteorder::{NetworkEndian, WriteBytesExt, ReadBytesExt};

#[derive(Clone, Default)]
pub struct MessageBuf {
    inner: Arc<Buf>,
}

impl MessageBuf {
    pub fn empty() -> Self {
        Default::default()
    }
        
    pub fn push<T: Encode>(&mut self, payload: &T) -> Result<(), T::EncodeError> {
        Arc::make_mut(&mut self.inner).push(payload)
    }
    
    pub fn pop<T: Decode>(&mut self) -> Result<T, T::DecodeError> {
        Arc::make_mut(&mut self.inner).pop()
    }
}

pub fn read<R: Read>(reader: &mut R) -> io::Result<MessageBuf> {
    let length = reader.read_u32::<NetworkEndian>()?;
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

    Ok(MessageBuf {
        inner: Arc::new(Buf {
            buf: buf,
            pos: pos,
        })
    })
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
    pub fn push<T: Encode>(&mut self, payload: &T) -> Result<(), T::EncodeError> {
        let align = mem::align_of::<T>();
        let start = self.buf.len();
        // we assume the alignment is always a power of two
        let aligned = (start + align - 1) & !(align - 1);
        self.buf.resize(aligned, 0);

        match payload.encode(&mut self.buf) {
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
    
    pub fn pop<T: Decode>(&mut self) -> Result<T, T::DecodeError> {
        let (start, end) = self.pos.pop_front().expect("cannot pop from empty buffer");
        match T::decode(&mut self.buf[start as usize..end as usize]) {
            Ok(t) => Ok(t),
            Err(err) => {
                // put offset back            
                self.pos.push_front((start, end));
                Err(err)
            }
        }
     
    }
}

impl<T: Encode<EncodeError=::void::Void>> From<T> for MessageBuf {
    fn from(t: T) -> Self {
        let mut buf = MessageBuf::empty();
        buf.push(&t).unwrap();
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use network::message::abomonate::Crypt;

    #[test]
    fn pop_msg() {
        let mut buf = MessageBuf::from(Crypt(&vec!["foo", "bar"]));
        let Crypt(vec) = buf.pop::<Crypt<Vec<&'static str>>>().unwrap();
        assert_eq!(vec, vec!["foo", "bar"]);
    }

    #[test]
    fn push_many_msg() {
        let string = String::from("hi");
        let vector = vec![1u8,2,3];
        let integer = 42i32;

        let mut buf = MessageBuf::empty();
        buf.push(&Crypt(&string)).unwrap();

        assert_eq!(string, buf.pop::<Crypt<String>>().unwrap().0);

        buf.push(&Crypt(&vector)).unwrap();
        buf.push(&Crypt(&integer)).unwrap();
        assert_eq!(vector, buf.pop::<Crypt<Vec<u8>>>().unwrap().0);
        assert_eq!(integer, buf.pop::<Crypt<i32>>().unwrap().0);
    }

    #[test]
    #[should_panic]
    fn pop_empty() {
        let mut buf = MessageBuf::empty();
        buf.pop::<Crypt<i32>>().unwrap();
    }
    
    #[test]
    fn type_mismatch() {
        use network::message::abomonate::DecodeError::*;

        let mut buf = MessageBuf::from(Crypt(&0i32));
        assert_eq!(TypeMismatch, buf.pop::<Crypt<String>>().unwrap_err());
    }

    #[test]
    fn read_write() {
        let mut buf = MessageBuf::from(Crypt(&"Some Content"));
        buf.push(&Crypt(&3.14f32)).unwrap();

        let mut stream = Vec::<u8>::new();
        write(&mut stream, &buf).expect("failed to write");
        let mut out = read(&mut &*stream).expect("failed to read");

        assert_eq!("Some Content", out.pop::<Crypt<&'static str>>().unwrap().0);
        assert_eq!(3.14, out.pop::<Crypt<f32>>().unwrap().0);
    }
}
