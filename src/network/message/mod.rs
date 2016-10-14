pub use self::buf::MessageBuf;

pub mod abomonate;
pub mod buf;

pub trait Encode<T> {
    type EncodeError;

    fn encode(&T, &mut Vec<u8>) -> Result<(), Self::EncodeError>;
}

pub trait Decode<T> {
    type DecodeError;

    fn decode(&mut [u8]) -> Result<T, Self::DecodeError>;
}
