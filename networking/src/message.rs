use std::sync::Arc;


trait Envelope: Sized {
    type DecodeError;
    type EncodeError;

    fn header() -> u64;
    fn decode(&mut [u8]) -> Result<Self, Self::DecodeError>;
    fn encode(&self, &mut Vec<u8>) -> Result<(), Self::EncodeError>;
}
