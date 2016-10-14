use std::any::{Any, TypeId};
use std::ops::Deref;
use std::mem;

use abomonation::{Abomonation, encode, decode};
use void::Void;

use network::message::{Encode, Decode};

#[derive(Debug)]
pub struct Crypt<T>(pub T);

impl<T> Deref for Crypt<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}
    

const TYPEID_BYTES: usize = 8;

impl<'a, T: Abomonation + Any + Clone> Encode for Crypt<&'a T> {
    type EncodeError = Void;

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        unsafe {
            let typeslice: [u8; TYPEID_BYTES];
            typeslice = mem::transmute(TypeId::of::<T>());
            bytes.extend_from_slice(&typeslice);
            Ok(encode::<T>(self.0, bytes))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    TypeMismatch,
    ExhumationFailure,
    UnexpectedRemains,
}

fn is<T: Any>(bytes: &[u8]) -> bool {
    if bytes.len() > TYPEID_BYTES {
        let mut typeslice = [0u8; TYPEID_BYTES];
        typeslice.copy_from_slice(&bytes[..TYPEID_BYTES]);
        let typeid: TypeId = unsafe { mem::transmute(typeslice) };

        typeid == TypeId::of::<T>()
    } else {
        false
    }
}

impl<T: Abomonation + Any + Clone> Decode for Crypt<T> {
    type DecodeError = DecodeError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        if is::<T>(&bytes) {
            let mut bytes = &mut bytes[TYPEID_BYTES..];
            if let Some((t, remaining)) = unsafe { decode::<T>(bytes) } {
                if remaining.is_empty() {
                    Ok(Crypt(t.clone()))
                } else {
                    Err(DecodeError::UnexpectedRemains)
                }
            } else {
                Err(DecodeError::ExhumationFailure)
            }
        } else {
            Err(DecodeError::TypeMismatch)
        }
    }
}

pub trait CryptSerialize: Abomonation + Any + Clone { }

impl<T: CryptSerialize> Encode for T {
    type EncodeError = Void;

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Crypt(self).encode(bytes)
    }
}

impl<T: CryptSerialize> Decode for T {
    type DecodeError = DecodeError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        Crypt::<Self>::decode(bytes).map(|crypt| crypt.0)
    }
}
