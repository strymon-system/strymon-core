

use abomonation::{Abomonation, encode, decode};

use network::message::{Encode, Decode};
use std::any::{Any, TypeId};
use std::io::{Error, ErrorKind};
use std::mem;
use void::Void;

// this is needed, because &'static refs cannot be safely abomonated
pub trait NonStatic {}
impl NonStatic for .. {}
impl<T> !NonStatic for &'static T {}

pub struct Abomonate;

const TYPEID_BYTES: usize = 8;

impl<T: Abomonation + Any + Clone + NonStatic> Encode<T> for Abomonate {
    type EncodeError = Void;

    fn encode(input: &T, bytes: &mut Vec<u8>) -> Result<(), Void> {
        unsafe {
            let typeslice: [u8; TYPEID_BYTES];
            typeslice = mem::transmute(TypeId::of::<T>());
            bytes.extend_from_slice(&typeslice);
            Ok(encode::<T>(input, bytes))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    TypeMismatch,
    ExhumationFailure,
    UnexpectedRemains,
}

impl Into<Error> for DecodeError {
    fn into(self) -> Error {
        Error::new(ErrorKind::Other,
                   format!("abomonate decode error: {:?}", self))
    }
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

impl<T: Abomonation + Any + Clone + NonStatic> Decode<T> for Abomonate {
    type DecodeError = DecodeError;

    fn decode(bytes: &mut [u8]) -> Result<T, Self::DecodeError> {
        if is::<T>(&bytes) {
            let mut bytes = &mut bytes[TYPEID_BYTES..];
            if let Some((t, remaining)) = unsafe { decode::<T>(bytes) } {
                if remaining.is_empty() {
                    Ok(t.clone())
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
