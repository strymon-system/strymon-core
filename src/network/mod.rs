//pub use self::service::{Service, Sender, Receiver, Listener};
pub use self::message::MessageBuf;

pub mod abomonate;
pub mod message;

mod service;
pub use self::service::*;

pub trait Encode {
    type Error;

    fn encode(&self, &mut Vec<u8>) -> Result<(), Self::Error>;
}

pub trait Decode: Sized {
    type Error;

    fn decode(&mut [u8]) -> Result<Self, Self::Error>;
}

#[cfg(test)]
mod tests {
    use std::io::Result;
    use network::*;
    use network::abomonate::VaultMessage;

    use futures::stream::Stream;

    fn assert_io<F: FnOnce() -> Result<()>>(f: F) {
        f().expect("I/O test failed")
    }

    #[test]
    fn service_integration() {
        assert_io(|| {
            let service = Service::init(None)?;
            let listener = service.listen(None)?;
            let (tx, rx) = service.connect(listener.external_addr())?;

            let ping = VaultMessage::from(&"Ping");           
            tx.send(ping);
            
            // process one single client
            listener.and_then(|(tx, rx)| {
                let mut ping = VaultMessage::from(rx.wait().next().unwrap()?);
                assert_eq!("Ping", ping.pop::<&str>().unwrap());
                
                let pong = VaultMessage::from(&"Pong");
                tx.send(pong);
                Ok(())
            }).wait().next().unwrap()?;

            let mut pong = VaultMessage::from(rx.wait().next().unwrap()?);
            assert_eq!("Pong", pong.pop::<&str>().unwrap());

            Ok(())
        });
    }
}
