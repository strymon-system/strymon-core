use std::any::{Any, TypeId};
use std::mem;
use std::sync::Arc;

use abomonation::{self, Abomonation};

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

    pub fn decode<T: Decode>(mut self) -> Option<T> {
        T::bytes(&mut *Arc::make_mut(&mut self.payload))
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
}
