pub mod message;
pub mod service;
pub mod reqresp;

#[cfg(test)]
mod tests {

    use futures::stream::Stream;
    use network::message::MessageBuf;
    use network::message::abomonate::Abomonate;
    use network::service::*;
    use std::io::Result;

    fn assert_io<F: FnOnce() -> Result<()>>(f: F) {
        f().expect("I/O test failed")
    }

    #[test]
    fn service_integration() {
        assert_io(|| {
            let service = Service::init(None)?;
            let listener = service.listen(None)?;
            let (tx, rx) = service.connect(listener.external_addr())?;


            let mut ping = MessageBuf::empty();
            ping.push::<Abomonate, _>(&String::from("Ping")).unwrap();
            tx.send(ping);

            // process one single client
            listener.and_then(|(tx, rx)| {
                    let mut ping = rx.wait().next().unwrap()?;
                    assert_eq!("Ping", ping.pop::<Abomonate, String>().unwrap());

                    let mut pong = MessageBuf::empty();
                    pong.push::<Abomonate, _>(&String::from("Pong")).unwrap();
                    tx.send(pong);
                    Ok(())
                })
                .wait()
                .next()
                .unwrap()?;

            let mut pong = rx.wait().next().unwrap()?;
            assert_eq!("Pong", pong.pop::<Abomonate, String>().unwrap());

            Ok(())
        });
    }
}
