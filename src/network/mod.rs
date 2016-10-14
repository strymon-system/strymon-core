pub mod message;
pub mod service;
pub mod reqresp;

#[cfg(test)]
mod tests {
    use std::io::Result;
    use network::service::*;
    use network::message::MessageBuf;
    use network::message::abomonate::Crypt;

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

            let ping = MessageBuf::from(Crypt(&"Ping"));           
            tx.send(ping);

            // process one single client
            listener.and_then(|(tx, rx)| {
                let mut ping = rx.wait().next().unwrap()?;
                assert_eq!("Ping", ping.pop::<Crypt<&str>>().unwrap().0);

                let pong = MessageBuf::from(Crypt(&"Pong"));
                tx.send(pong);
                Ok(())
            }).wait().next().unwrap()?;

            let mut pong = rx.wait().next().unwrap()?;
            assert_eq!("Pong", pong.pop::<Crypt<&str>>().unwrap().0);

            Ok(())
        });
    }
}
