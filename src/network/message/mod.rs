pub use self::buf::MessageBuf;

pub mod abomonate;
pub mod buf;

pub trait Encode {
    type EncodeError;

    fn encode(&self, &mut Vec<u8>) -> Result<(), Self::EncodeError>;
}

pub trait Decode: Sized {
    type DecodeError;

    fn decode(&mut [u8]) -> Result<Self, Self::DecodeError>;
}
