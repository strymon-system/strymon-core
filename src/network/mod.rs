//pub use self::service::{Service, Sender, Receiver, Listener};
pub use self::message::MessageBuf;

pub mod abomonate;
pub mod message;

//pub mod service;

pub trait Encode {
    type Error;

    fn encode(&self, &mut Vec<u8>) -> Result<(), Self::Error>;
}

pub trait Decode: Sized {
    type Error;

    fn decode(&mut [u8]) -> Result<Self, Self::Error>;
}
