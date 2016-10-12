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

#[cfg(test)]
mod tests {
    use std::io::Result;
    use network::*;
    use network::message::abomonate::VaultMessage;

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
