use std::any::Any;

use abomonation::{Abomonation, encode, decode};

pub enum Never { }

pub struct Abomolope<A: Abomonation + Any>(pub A);

impl<A: Abomonation + Any> Envelope for Abomolope<A> {
    type DecodeError = ();
    type EncodeError = Never;

    fn header() -> u64 {
        0
    }

    fn decode(bytes: &mut [u8]) -> Result<Self, Self::DecodeError> {
        Err(())
    }

    fn encode(&self, bytes: &mut Vec<u8>) -> Result<(), Self::EncodeError> {
        Ok(())
    }
}

pub struct UnsafeAbomolope<A: Abomonation>(A);

impl<A: Abomonation> UnsafeAbomolope(A) {
    unsafe pub fn new(payload: A) -> Self {
        UnsafeAbomolope(payload)
    }
}
