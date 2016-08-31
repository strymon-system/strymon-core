use std::any::{Any, TypeId};
use std::io::{Read, Result, Write};
use std::mem;

use abomonation::{self, Abomonation};
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};

pub fn read<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let length = try!(reader.read_u32::<NetworkEndian>());
    let mut bytes = vec![0u8; length as usize]; // TODO optimize this
    try!(reader.read_exact(&mut bytes));

    Ok(bytes)
}

pub fn write<W: Write>(writer: &mut W, frame: Vec<u8>) -> Result<()> {
    assert!(frame.len() <= ::std::u32::MAX as usize, "frame is too big!");
    try!(writer.write_u32::<NetworkEndian>(frame.len() as u32));
    try!(writer.write_all(&frame));

    Ok(())
}

pub trait Encode: Sized {
    fn encode(&self) -> Vec<u8>;
}

pub trait Decode: Sized {
    fn is(&[u8]) -> bool;
    fn decode(&mut [u8]) -> Option<Self>;
}

const TYPEID_BYTES: usize = 8;

impl<T: Abomonation + Any> Encode for T {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        unsafe {
            let typeslice: [u8; TYPEID_BYTES];
            typeslice = mem::transmute(TypeId::of::<T>());
            bytes.extend_from_slice(&typeslice);
            abomonation::encode::<T>(self, &mut bytes);
        }

        bytes
    }
}

impl<T: Abomonation + Any + Clone> Decode for T {
    fn is(bytes: &[u8]) -> bool {
        if bytes.len() > TYPEID_BYTES {
            let mut typeslice = [0u8; TYPEID_BYTES];
            typeslice.copy_from_slice(&bytes[..TYPEID_BYTES]);
            let typeid: TypeId = unsafe { mem::transmute(typeslice) };

            typeid == TypeId::of::<T>()
        } else {
            false
        }
    }

    fn decode(bytes: &mut [u8]) -> Option<Self> {
        if Self::is(&bytes) {
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
    fn test_encode_decode() {
        let s = "Hello, World".to_string();
        let mut msg = s.clone().encode();
        assert!(String::is(&msg));
        assert_eq!(String::decode(&mut msg).unwrap(), s);
    }

    #[test]
    fn test_read_write() {
        let s1 = "Some Content".to_string();
        let msg1 = s1.clone().encode();

        let mut stream = Vec::<u8>::new();
        write(&mut stream, msg1).expect("failed to write");

        let mut msg2 = read(&mut &*stream).expect("failed to read");
        let s2 = String::decode(&mut msg2).expect("failed to decode");
        assert_eq!(s1, s2);
    }
}
