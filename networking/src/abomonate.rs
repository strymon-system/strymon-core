use std::any::{Any, TypeId};
use std::mem;

use abomonation::{Abomonation, encode, decode};
use void::Void;

use message::{Encode, Decode};

pub enum Never { }

#[derive(Debug)]
pub struct Vault<T>(pub T);

const TYPEID_BYTES: usize = 8;

impl<'a, T: Abomonation + Any + Clone> Encode for Vault<&'a T> {
    type Error = Void;

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::Error> {
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

impl<T: Abomonation + Any + Clone> Decode for Vault<T> {
    type Error = DecodeError;

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::Error> {
        if is::<T>(&bytes) {
            let mut bytes = &mut bytes[TYPEID_BYTES..];
            if let Some((t, remaining)) = unsafe { decode::<T>(bytes) } {
                if remaining.is_empty() {
                    Ok(Vault(t.clone()))
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