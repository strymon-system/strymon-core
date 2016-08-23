use std::any::{Any, TypeId};
use std::io::{Result, Error, ErrorKind, Read, Write};
use std::mem;
use std::sync::Arc;

use abomonation::{self, Abomonation};
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt, ByteOrder};

pub fn read<R: Read>(reader: &mut R) -> Result<Frame> {
    let length = try!(reader.read_u32::<NetworkEndian>());
    let mut bytes = vec![0u8; length as usize]; // TODO optimize this
    try!(reader.read_exact(&mut bytes));

    Ok((Frame::new(bytes)))
}

pub fn write<W: Write>(writer: &mut W, frame: Frame) -> Result<()> {
    assert!(frame.len() <= ::std::u32::MAX as usize, "frame is too big!");
    try!(writer.write_u32::<NetworkEndian>(frame.len() as u32));
    try!(writer.write_all(frame.as_ref()));
    
    Ok(())
}

#[derive(Clone)]
pub struct Frame {
    payload: Arc<Vec<u8>>,
}

impl Frame {
    pub fn new(bytes: Vec<u8>) -> Self {
        Frame { payload: Arc::new(bytes) }
    }

    pub fn encode<T: Encode>(t: T) -> Self {
        Frame::new(t.bytes())
    }

    pub fn is<T: Decode>(&self) -> bool {
        T::is_bytes(&self.payload)
    }

    pub fn decode<T: Decode>(mut self) -> Result<T> {
        T::bytes(&mut *Arc::make_mut(&mut self.payload))
          .ok_or(Error::new(ErrorKind::Other, "unexpected message"))
    }
    
    fn len(&self) -> usize {
        self.payload.len()
    }
}

impl AsRef<[u8]> for Frame {
    fn as_ref(&self) -> &[u8] {
        &*self.payload
    }
}

pub trait Encode: Sized {
    fn bytes(self) -> Vec<u8>;
}

pub trait Decode: Sized {
    fn is_bytes(&[u8]) -> bool;
    fn bytes(&mut [u8]) -> Option<Self>;
}

const TYPEID_BYTES: usize = 8;

impl<T: Abomonation + Any + Clone + Send> Encode for T {
    fn bytes(self) -> Vec<u8> {
        let mut bytes = Vec::new();

        unsafe {
            let typeslice: [u8; TYPEID_BYTES];
            typeslice = mem::transmute(TypeId::of::<T>());
            bytes.extend_from_slice(&typeslice);
            abomonation::encode::<T>(&self, &mut bytes);
        }

        bytes
    }
}

impl<T: Abomonation + Any + Clone + Send> Decode for T {
    fn is_bytes(bytes: &[u8]) -> bool {
        if bytes.len() > TYPEID_BYTES {
            let mut typeslice = [0u8; TYPEID_BYTES];
            typeslice.copy_from_slice(&bytes[..TYPEID_BYTES]);
            let typeid: TypeId = unsafe { mem::transmute(typeslice) };

            typeid == TypeId::of::<T>()
        } else {
            false
        }
    }

    fn bytes(bytes: &mut [u8]) -> Option<Self> {
        if Self::is_bytes(&bytes) {
            let mut bytes = &mut bytes[TYPEID_BYTES..];
            match unsafe { abomonation::decode::<T>(bytes) } {
                Some((typed, ref remaining)) if remaining.is_empty() => Some(typed.clone()),
                _ => None,
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_message() {
        let s = "Hello, World".to_string();
        let msg = Frame::encode(s.clone());
        assert!(msg.is::<String>());
        assert_eq!(msg.decode::<String>().unwrap(), s);
    }

    #[test]
    fn test_read_write() {
        let s1 = "Some Content".to_string();
        let msg1 = Frame::encode(s1.clone());
        
        let mut stream = Vec::<u8>::new();
        write(&mut stream, msg1).expect("failed to write");

        let msg2 = read(&mut &*stream).expect("failed to read");
        let s2 = msg2.decode::<String>().expect("failed to decode");
        assert_eq!(s1, s2);
    }
}
